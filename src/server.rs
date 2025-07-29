use crate::config::{Config, ServerConfig, ServerServiceConfig, ServiceType, TransportType};
use crate::config_watcher::{ConfigChange, ServerServiceChange};
use crate::constants::{listen_backoff, UDP_BUFFER_SIZE};
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::multi_map::MultiMap;
use crate::protocol::Hello::{ControlChannelHello, DataChannelHello};
use crate::protocol::{
    self, read_auth, read_hello, Ack, ControlChannelCmd, DataChannelCmd, UdpTraffic,
    HASH_WIDTH_IN_BYTES, PROTO_V2,
};
#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;
use crate::transport::{SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use rand::RngCore;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self, copy_bidirectional, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{self, timeout, Instant};
use tracing::{debug, error, info, info_span, instrument, warn, Instrument, Span};

type ServiceDigest = protocol::Digest; // SHA256 of a service name
type Nonce = protocol::Digest; // Also called `session_key`

const TCP_POOL_SIZE: usize = 8; // The number of cached connections for TCP services
const UDP_POOL_SIZE: usize = 2; // The number of cached connections for UDP services
const CHAN_SIZE: usize = 2048; // The capacity of various chans
const DATA_CHANNEL_REQUEST_BUFFER: usize = CHAN_SIZE / 2; // Buffer for pending data channel requests
const HANDSHAKE_TIMEOUT: u64 = 5; // Timeout for transport handshake

// The entrypoint of running a server
pub async fn run_server(
    config: Config,
    shutdown_rx: broadcast::Receiver<bool>,
    update_rx: mpsc::Receiver<ConfigChange>,
) -> Result<()> {
    let config = match config.server {
            Some(config) => config,
            None => {
                return Err(anyhow!("Try to run as a server, but the configuration is missing. Please add the `[server]` block"))
            }
        };

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut server = Server::<TcpTransport>::from(config).await?;
            server.run(shutdown_rx, update_rx).await?;
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut server = Server::<TlsTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut server = Server::<NoiseTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut server = Server::<WebsocketTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }

    Ok(())
}

// A hash map of ControlChannelHandles, indexed by ServiceDigest or Nonce
// See also MultiMap
type ControlChannelMap<T> = MultiMap<ServiceDigest, Nonce, ControlChannelHandle<T>>;

// Server holds all states of running a server
struct Server<T: Transport> {
    // `[server]` config
    config: Arc<ServerConfig>,

    // `[server.services]` config, indexed by ServiceDigest
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    // Collection of control channels
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    // Wrapper around the transport layer
    transport: Arc<T>,
}

// Generate a hash map of services which is indexed by ServiceDigest
fn generate_service_hashmap(
    server_config: &ServerConfig,
) -> HashMap<ServiceDigest, ServerServiceConfig> {
    let mut ret = HashMap::new();
    for u in &server_config.services {
        ret.insert(protocol::digest(u.0.as_bytes()), (*u.1).clone());
    }
    ret
}

impl<T: 'static + Transport> Server<T> {
    // Create a server from `[server]`
    pub async fn from(config: ServerConfig) -> Result<Server<T>> {
        let config = Arc::new(config);
        let services = Arc::new(RwLock::new(generate_service_hashmap(&config)));
        let control_channels = Arc::new(RwLock::new(ControlChannelMap::new()));
        let transport = Arc::new(T::new(&config.transport)?);
        Ok(Server {
            config,
            services,
            control_channels,
            transport,
        })
    }

    // The entry point of Server
    pub async fn run(
        &mut self,
        mut shutdown_rx: broadcast::Receiver<bool>,
        mut update_rx: mpsc::Receiver<ConfigChange>,
    ) -> Result<()> {
        // Listen at `server.bind_addr`
        let l = self
            .transport
            .bind(&self.config.bind_addr)
            .await
            .with_context(|| "Failed to listen at `server.bind_addr`")?;
        info!("Listening at {}", self.config.bind_addr);

        // Retry at least every 100 ms
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_millis(100),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for connections and shutdown signals
        loop {
            tokio::select! {
                // Wait for incoming control and data channels
                ret = self.transport.accept(&l) => {
                    match ret {
                        Err(err) => {
                            // Detects whether it's an IO error
                            if let Some(err) = err.downcast_ref::<io::Error>() {
                                // If it is an IO error, then it's possibly an
                                // EMFILE. So sleep for a while and retry
                                // TODO: Only sleep for EMFILE, ENFILE, ENOMEM, ENOBUFS
                                if let Some(d) = backoff.next_backoff() {
                                    error!("Failed to accept: {:#}. Retry in {:?}...", err, d);
                                    time::sleep(d).await;
                                } else {
                                    // This branch will never be executed according to the current retry policy
                                    error!("Too many retries. Aborting...");
                                    break;
                                }
                            }
                            // If it's not an IO error, then it comes from
                            // the transport layer, so ignore it
                        }
                        Ok((conn, addr)) => {
                            backoff.reset();

                            // Do transport handshake with a timeout
                            match time::timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), self.transport.handshake(conn)).await {
                                Ok(conn) => {
                                    match conn.with_context(|| "Failed to do transport handshake") {
                                        Ok(conn) => {
                                            let services = self.services.clone();
                                            let control_channels = self.control_channels.clone();
                                            let server_config = self.config.clone();
                                            tokio::spawn(async move {
                                                if let Err(err) = handle_connection(conn, services, control_channels, server_config, addr).await {
                                                    error!("{:#}", err);
                                                }
                                            }.instrument(info_span!("connection", %addr)));
                                        }, Err(e) => {
                                            error!("{:#}", e);
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("Transport handshake timeout: {}", e);
                                }
                            }
                        }
                    }
                },
                // Wait for the shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shutting down gracefully...");
                    break;
                },
                e = update_rx.recv() => {
                    if let Some(e) = e {
                        self.handle_hot_reload(e).await;
                    }
                }
            }
        }

        info!("Shutdown");

        Ok(())
    }

    async fn handle_hot_reload(&mut self, e: ConfigChange) {
        match e {
            ConfigChange::ServerChange(server_change) => match server_change {
                ServerServiceChange::Add(cfg) => {
                    let hash = protocol::digest(cfg.name.as_bytes());
                    let mut wg = self.services.write().await;
                    let _ = wg.insert(hash, cfg);

                    let mut wg = self.control_channels.write().await;
                    let _ = wg.remove1(&hash);
                }
                ServerServiceChange::Delete(s) => {
                    let hash = protocol::digest(s.as_bytes());
                    let _ = self.services.write().await.remove(&hash);

                    let mut wg = self.control_channels.write().await;
                    let _ = wg.remove1(&hash);
                }
            },
            ignored => warn!("Ignored {:?} since running as a server", ignored),
        }
    }
}

// Handle connections to `server.bind_addr`
async fn handle_connection<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    server_config: Arc<ServerConfig>,
    addr: SocketAddr,
) -> Result<()> {
    // Read hello with timeout
    let hello = match timeout(
        Duration::from_secs(HANDSHAKE_TIMEOUT),
        read_hello(&mut conn),
    )
    .await
    {
        Ok(Ok(hello)) => hello,
        Ok(Err(e)) => {
            error!("Failed to read hello: {}", e);
            let _ = conn.shutdown().await; // 显式关闭连接以防文件描述符泄露
            return Err(e);
        }
        Err(_) => {
            error!("Read hello timeout");
            let _ = conn.shutdown().await; // 显式关闭连接以防文件描述符泄露
            bail!("Operation timed out");
        }
    };

    match hello {
        ControlChannelHello(protocol_version, service_digest) => {
            let mut timestamp = 0;

            // if a version is v2, read the timestamp from conning
            if protocol_version == PROTO_V2 {
                // read an u64 timestamp from conning, if not, default set to 0
                timestamp = match conn.read_u64_le().await {
                    Ok(ts) => {
                        debug!("read ts from conn success: {}", ts);
                        ts
                    }
                    Err(e) => {
                        // if EOF or connection closed, it means no timestamp
                        debug!("failed to read timestamp, maybe old version client: {}", e);
                        0
                    }
                };
            }
            info!("client timestamp: {}", timestamp);

            do_control_channel_handshake(
                conn,
                services,
                control_channels,
                service_digest,
                server_config,
                timestamp,
                addr,
            )
            .await?;
        }
        DataChannelHello(_, nonce) => {
            do_data_channel_handshake(conn, control_channels, nonce).await?;
        }
    }
    Ok(())
}

async fn do_control_channel_handshake<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    service_digest: ServiceDigest,
    server_config: Arc<ServerConfig>,
    timestamp: u64,
    addr: SocketAddr,
) -> Result<()> {
    info!(
        "try to establish control channel, client timestamp: {}, addr: {}",
        timestamp, addr
    );

    T::hint(&conn, SocketOpts::for_control_channel());

    // Generate a nonce
    let mut nonce = vec![0u8; HASH_WIDTH_IN_BYTES];
    rand::thread_rng().fill_bytes(&mut nonce);

    // Send hello
    let hello_send = ControlChannelHello(
        // if the timestamp is 0, use the current protocol version, otherwise use v2
        if timestamp == 0 {
            protocol::CURRENT_PROTO_VERSION
        } else {
            protocol::PROTO_V2
        },
        nonce.clone().try_into().unwrap(),
    );

    conn.write_all(&bincode::serialize(&hello_send).unwrap())
        .await?;
    conn.flush().await?;

    // Look up the service
    let service_config = match services.read().await.get(&service_digest) {
        Some(v) => v,
        None => {
            conn.write_all(&bincode::serialize(&Ack::ServiceNotExist).unwrap())
                .await?;
            bail!("not found service: {}", hex::encode(service_digest));
        }
    }
    .to_owned();

    let service_name = service_config.name.clone();

    // Calculate the checksum
    let mut concat = Vec::from(service_config.token.as_ref().unwrap().as_bytes());
    concat.append(&mut nonce);

    // Read auth with timeout
    let protocol::Auth(d) =
        match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), read_auth(&mut conn)).await {
            Ok(Ok(auth)) => auth,
            Ok(Err(e)) => {
                error!("Failed to read auth: {}", e);
                return Err(e);
            }
            Err(_) => {
                error!("Read auth timeout");
                bail!("Authentication timed out");
            }
        };

    // Validate
    let session_key = protocol::digest(&concat);
    if session_key != d {
        conn.write_all(&bincode::serialize(&Ack::AuthFailed).unwrap())
            .await?;
        debug!(
            "auth failed, expect {}, actual {}",
            hex::encode(session_key),
            hex::encode(d)
        );
        bail!("service {} auth failed", service_name);
    } else {
        // check if there is an existing channel with the same service and compare timestamps
        let existing_channel = {
            let control_map_guard = control_channels.read().await;
            if let Some(existing) = control_map_guard.get1(&service_digest) {
                // if the existing channel timestamp is greater than new timestamp, reject new connection
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp,
                    old_addr = %existing.addr,
                    new_ts = timestamp,
                    new_addr = %addr,
                    "found existing channel"
                );
                if existing.timestamp >= timestamp {
                    info!(
                        service = %service_name,
                        old_ts = existing.timestamp,
                        old_addr = %existing.addr,
                        new_ts = timestamp,
                        new_addr = %addr,
                        "reject connection: old_ts:{} >= new_ts:{}",
                        existing.timestamp, timestamp
                    );
                    conn.write_all(&bincode::serialize(&Ack::AuthFailed).unwrap())
                        .await?;
                    return Ok(());
                }
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp,
                    old_addr = %existing.addr,
                    new_ts = timestamp,
                    new_addr = %addr,
                    "replace existing channel"
                );
                true
            } else {
                false
            }
        };

        if existing_channel {
            info!(
                service = %service_name,
                new_ts = timestamp,
                new_addr = %addr,
                "create or replace control channel"
            );
        }

        // 1. Prepare Handle and Control Task Future
        let (handle, control_task_future) = ControlChannelHandle::prepare(
            conn,
            service_config.clone(),
            server_config.heartbeat_interval,
            timestamp,
            addr,
        );

        let nonce_for_cleanup = session_key;
        let control_channels_weak = Arc::downgrade(&control_channels);

        // 2. Insert Handle into the Map (within a write lock scope)
        {
            let mut control_map_guard = control_channels.write().await;

            if let Some(existing) = control_map_guard.get1(&service_digest) {
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp,
                    new_ts = timestamp,
                    old_addr = %existing.addr,
                    new_addr = %addr,
                    nonce = %hex::encode(nonce_for_cleanup),
                    "replace old channel, create new control channel"
                );
                let _ = control_map_guard.remove1(&service_digest);
            } else if control_map_guard.get2(&nonce_for_cleanup).is_some() {
                warn!(
                    service = %service_name,
                    nonce = %hex::encode(nonce_for_cleanup),
                    "potential session key conflict detected, remove old entry"
                );
                let _ = control_map_guard.remove2(&nonce_for_cleanup);
            }

            // Insert the new handle. `handle` is moved into the map.
            let _ = control_map_guard.insert(service_digest, nonce_for_cleanup, handle);
            info!(
                service = %service_name,
                nonce = %hex::encode(nonce_for_cleanup),
                ts = timestamp,
                addr = %addr,
                "control channel handle inserted"
            );
        }

        // Clone names again for the spans, as the original is moved into the tasks
        let service_name_for_control_span = service_name.clone();
        let nonce_for_control_span = nonce_for_cleanup;

        // 3. Spawn the Main Control Task
        let control_task_handle = tokio::spawn(
            async move {
                // The future returned by `prepare`
                // owns the connection and runs `ControlChannel::run`
                if let Err(err) = control_task_future.await {
                    // Use moved variables inside the task
                    error!(service = %service_name, nonce = %hex::encode(nonce_for_cleanup), "control channel task failed: {:#}", err);
                } else {
                    debug!(service = %service_name, nonce = %hex::encode(nonce_for_cleanup), "control channel task completed successfully");
                }
            }
            // Instrument the task - Use the newly cloned variables for the span
            .instrument(info_span!("control_task", service = %service_name_for_control_span, nonce = %hex::encode(nonce_for_control_span))),
        );

        // Clone nonce again specifically for the cleanup task's span
        let nonce_for_cleanup_span_only = nonce_for_cleanup;

        // 4. Spawn the Cleanup Task
        tokio::spawn(
            async move {
                // Wait for the main control task to complete (normally or abnormally)
                match control_task_handle.await {
                    Ok(_) => {
                        // Use moved nonce inside
                        debug!(nonce = %hex::encode(nonce_for_cleanup), "control task completed, prepare to cleanup");
                    },
                    Err(e) => {
                        // Use moved nonce inside
                        error!(nonce = %hex::encode(nonce_for_cleanup), "control task failed, prepare to cleanup: {}", e);
                    }
                }

                if let Some(map_arc) = control_channels_weak.upgrade() {
                    debug!(nonce = %hex::encode(nonce_for_cleanup), "get lock, prepare to cleanup");
                    let mut map_guard = map_arc.write().await;
                    if let Some(_removed_handle) = map_guard.remove2(&nonce_for_cleanup) {
                        info!(nonce = %hex::encode(nonce_for_cleanup), "control channel handle removed successfully");
                    } else {
                        warn!(nonce = %hex::encode(nonce_for_cleanup), "control channel handle removed before cleanup");
                    }
                } else {
                    warn!(nonce = %hex::encode(nonce_for_cleanup), "control channel map released before cleanup");
                }
            }
            // Instrument the cleanup task - Use the span-specific cloned nonce
            .instrument(info_span!("cleanup_task", nonce = %hex::encode(nonce_for_cleanup_span_only))),
        );
    }

    Ok(())
}

async fn do_data_channel_handshake<T: 'static + Transport>(
    conn: T::Stream,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    nonce: Nonce,
) -> Result<()> {
    debug!("Try to handshake a data channel");

    // Validate
    let control_channels_guard = control_channels.read().await;
    match control_channels_guard.get2(&nonce) {
        Some(handle) => {
            T::hint(&conn, SocketOpts::from_server_cfg(&handle.service));

            // Send the data channel to the corresponding control channel
            handle
                .data_ch_tx
                .send(conn)
                .await
                .with_context(|| "Data channel for a stale control channel")?;
        }
        None => {
            warn!("Data channel has incorrect nonce");
        }
    }
    Ok(())
}

pub struct ControlChannelHandle<T: Transport> {
    // Shutdown the control channel by dropping it
    _shutdown_tx: broadcast::Sender<bool>,
    data_ch_tx: mpsc::Sender<T::Stream>,
    service: ServerServiceConfig,
    // 添加时间戳字段，记录连接建立时间
    timestamp: u64,
    addr: SocketAddr,
}

impl<T> ControlChannelHandle<T>
where
    T: 'static + Transport,
{
    // Renamed `new` to `prepare`.
    // Returns the handle instance and a Future that runs the control channel logic.
    #[instrument(name = "handle_prepare", skip_all, fields(service = %service.name))]
    fn prepare(
        conn: T::Stream,
        service: ServerServiceConfig,
        heartbeat_interval: u64,
        timestamp: u64,
        addr: SocketAddr,
    ) -> (Self, impl Future<Output = Result<()>> + Send + 'static) {
        // Create shutdown channel, data channel queue, request channel (Bounded now)
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1);
        let (data_ch_tx, data_ch_rx) = mpsc::channel(CHAN_SIZE * 2);
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::channel(DATA_CHANNEL_REQUEST_BUFFER);

        // Calculate pool size (remains the same)
        let pool_size = match service.service_type {
            ServiceType::Tcp => TCP_POOL_SIZE,
            ServiceType::Udp => UDP_POOL_SIZE,
        };

        // Spawn a connection pool task (remains the same, takes ownership of relevant channels)
        let service_name_clone = service.name.clone();

        // Clone name again for spans before moving into tasks
        let service_name_for_tcp_span = service_name_clone.clone();
        let service_name_for_udp_span = service_name_clone.clone();

        match service.service_type {
            ServiceType::Tcp => {
                let shutdown_rx_clone = shutdown_tx.subscribe();
                let bind_addr = service.bind_addr.clone();
                let data_ch_req_tx_clone = data_ch_req_tx.clone(); // Clone sender for the pool task
                tokio::spawn(
                    async move {
                        // Use moved service_name_clone inside a task
                        if let Err(e) = run_tcp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx, // data_ch_rx moved here
                            data_ch_req_tx_clone,
                            shutdown_rx_clone,
                        )
                        .await
                        .with_context(|| "TCP connection pool task failed")
                        {
                            error!("{:#}", e);
                        }
                        // Use moved service_name_clone for debug log
                        debug!(service = %service_name_clone, "TCP connection pool task finished.");
                    }
                    // Use span-specific clone
                    .instrument(info_span!("tcp_pool", service = %service_name_for_tcp_span)),
                );
            }
            ServiceType::Udp => {
                let shutdown_rx_clone = shutdown_tx.subscribe();
                let bind_addr = service.bind_addr.clone();
                let data_ch_req_tx_clone = data_ch_req_tx.clone(); // Clone sender for the pool task
                tokio::spawn(
                    async move {
                        // Use moved service_name_clone inside a task
                        if let Err(e) = run_udp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,           // data_ch_rx moved here
                            data_ch_req_tx_clone, // Pass req channel
                            shutdown_rx_clone,
                        )
                        .await
                        .with_context(|| "UDP connection pool task failed")
                        {
                            error!("{:#}", e);
                        }
                        // Use moved service_name_clone for debug log
                        debug!(service = %service_name_clone, "UDP connection pool task finished.");
                    }
                    // Use span-specific clone
                    .instrument(info_span!("udp_pool", service = %service_name_for_udp_span)),
                );
            }
        };

        // Create the ControlChannel state struct
        // (takes ownership of conn, shutdown_rx, data_ch_req_rx)
        let ch = ControlChannel::<T> {
            conn,
            shutdown_rx,
            data_ch_req_rx,
            heartbeat_interval,
            pool_size,
            data_ch_req_tx: data_ch_req_tx.clone(),
        };

        // Create the handle instance (returned to caller)
        let handle = ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            data_ch_tx,
            service,
            timestamp,
            addr,
        };

        // Create the Future that will execute the control channel logic
        let control_task_future = async move { ch.run().await }.instrument(Span::current());

        // Return the handle and the future
        (handle, control_task_future)
    }
}

// Control channel, using T as the transport layer.
struct ControlChannel<T: Transport> {
    conn: T::Stream,                        // The connection of control channel
    shutdown_rx: broadcast::Receiver<bool>, // Receives the shutdown signal
    data_ch_req_rx: mpsc::Receiver<bool>,   // Receives visitor connections (Bounded Receiver)
    heartbeat_interval: u64,                // Application-layer heartbeat interval in secs
    pool_size: usize,                       // Initial pool size to request
    data_ch_req_tx: mpsc::Sender<bool>,     // Sender to request data channels (Bounded Sender)
}

impl<T: Transport> ControlChannel<T> {
    async fn write_and_flush(&mut self, data: &[u8]) -> Result<()> {
        write_and_flush(&mut self.conn, data)
            .await
            .with_context(|| "Failed to write control cmds")?;
        Ok(())
    }
    // Run a control channel
    #[instrument(skip_all)]
    async fn run(mut self) -> Result<()> {
        // Send Ack::Ok as the first action
        match self.write_and_flush(&bincode::serialize(&Ack::Ok)?).await {
            Ok(_) => {
                info!("Control channel established and acknowledged.");
            }
            Err(e) => {
                error!(
                    "Failed to send Ack::Ok to client, closing control channel: {:#}",
                    e
                );
                return Err(e);
            }
        }

        // Request initial data channels to fill the pool
        debug!(
            pool_size = self.pool_size,
            "Sending initial data channel requests..."
        );
        for i in 0..self.pool_size {
            if let Err(e) = self.data_ch_req_tx.send(true).await {
                // If the receiver is dropped (channel closed), we can't proceed.
                error!("Failed to send initial data channel request #{}/{}: {}, control channel closing", i + 1, self.pool_size, e);
                return Err(anyhow!(
                    "Data channel request queue closed unexpectedly during init: {}",
                    e
                ));
            }
        }
        debug!(
            "Sent initial {} data channel requests successfully.",
            self.pool_size
        );

        let create_ch_cmd = bincode::serialize(&ControlChannelCmd::CreateDataChannel)?;
        let heartbeat = bincode::serialize(&ControlChannelCmd::HeartBeat)?;

        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(_) => {
                            if let Err(e) = self.write_and_flush(&create_ch_cmd).await {
                                error!("{:#}", e);
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_interval)), if self.heartbeat_interval != 0 => {
                            if let Err(e) = self.write_and_flush(&heartbeat).await {
                                error!("{:#}", e);
                                break;
                            }
                }
                _ = self.shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("Control channel shutdown");

        Ok(())
    }
}

fn tcp_listen_and_send(
    addr: String,
    data_ch_req_tx: mpsc::Sender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> mpsc::Receiver<TcpStream> {
    let (tx, rx) = mpsc::channel(CHAN_SIZE);

    tokio::spawn(async move {
        let l = retry_notify_with_deadline(listen_backoff(),  || async {
            Ok(TcpListener::bind(&addr).await?)
        }, |e, duration| {
            error!("{:#}. Retry in {:?}", e, duration);
        }, &mut shutdown_rx).await
        .with_context(|| "Failed to listen for the service");

        let l: TcpListener = match l {
            Ok(v) => v,
            Err(e) => {
                error!("{:#}", e);
                return;
            }
        };

        info!("Listening at {}", &addr);

        // Retry at least every 1s
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_secs(1),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for visitors and the shutdown signal
        loop {
            tokio::select! {
                val = l.accept() => {
                    match val {
                        Err(e) => {
                            // `l` is a TCP listener, so this must be an IO error
                            // Possibly a EMFILE. So sleep for a while
                            error!("{}. Sleep for a while", e);
                            if let Some(d) = backoff.next_backoff() {
                                time::sleep(d).await;
                            } else {
                                // This branch will never be reached for current backoff policy
                                error!("Too many retries. Aborting...");
                                break;
                            }
                        }
                        Ok((incoming, addr)) => {
                            // For every visitor, request to create a data channel
                            // Use .await and handle error for the bounded channel send
                            if let Err(e) = data_ch_req_tx.send(true).await {
                                error!("Failed to send data channel request (likely control channel closed): {}. Listener exiting.", e);
                                break; // Exit the loop if send fails
                            }

                            backoff.reset();

                            debug!("New visitor from {}", addr);

                            // Send the visitor to the connection pool
                            let _ = tx.send(incoming).await;
                        }
                    }
                },
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("TCPListener shutdown");
    }.instrument(Span::current()));

    rx
}

#[instrument(skip_all)]
async fn run_tcp_connection_pool<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::Sender<bool>,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let mut visitor_rx = tcp_listen_and_send(bind_addr, data_ch_req_tx.clone(), shutdown_rx);
    let cmd = bincode::serialize(&DataChannelCmd::StartForwardTcp)?;

    'pool: while let Some(mut visitor) = visitor_rx.recv().await {
        loop {
            if let Some(mut ch) = data_ch_rx.recv().await {
                if write_and_flush(&mut ch, &cmd).await.is_ok() {
                    tokio::spawn(async move {
                        let _ = copy_bidirectional(&mut ch, &mut visitor).await;
                    });
                    break;
                } else {
                    // The Current data channel is broken.
                    // Request for a new one
                    // Use .await and handle error for the bounded channel send
                    if let Err(e) = data_ch_req_tx.send(true).await {
                        error!("Failed to send data channel request (likely control channel closed): {}. Pool exiting.", e);
                        break 'pool; // Exit the outer loop if send fails
                    }
                }
            } else {
                break 'pool;
            }
        }
    }

    info!("Shutdown");
    Ok(())
}

#[instrument(skip_all)]
async fn run_udp_connection_pool<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::Sender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    const UDP_READ_TIMEOUT: Duration = Duration::from_secs(10); // UDP Read Timeout
    const UDP_WRITE_TIMEOUT: Duration = Duration::from_secs(10); // UDP Write Timeout
    const UDP_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(300); // 5分钟

    // 绑定UDP监听socket
    let l = retry_notify_with_deadline(
        listen_backoff(),
        || async { Ok(UdpSocket::bind(&bind_addr).await?) },
        |e, duration| {
            warn!("{:#}. Retry in {:?}", e, duration);
        },
        &mut shutdown_rx,
    )
    .await
    .with_context(|| "Failed to listen for the service")?;

    info!("Listening at {}", &bind_addr);

    let cmd = bincode::serialize(&DataChannelCmd::StartForwardUdp)?;
    let mut buf = [0u8; UDP_BUFFER_SIZE];
    let mut last_activity = Instant::now();

    // 主循环：管理连接和数据转发
    'main_loop: loop {
        // 获取或重建数据通道连接
        let mut conn = match data_ch_rx.recv().await {
            Some(mut c) => {
                match timeout(UDP_WRITE_TIMEOUT, write_and_flush(&mut c, &cmd)).await {
                    Ok(Ok(_)) => {
                        debug!("UDP connection established");
                        c
                    }
                    Ok(Err(e)) => {
                        error!("Failed to send start command: {}", e);
                        // 请求新连接
                        if let Err(e) = data_ch_req_tx.send(true).await {
                            error!("Failed to request new data channel: {}", e);
                            break;
                        }
                        continue;
                    }
                    Err(_) => {
                        error!("Timeout sending start command");
                        // 请求新连接
                        if let Err(e) = data_ch_req_tx.send(true).await {
                            error!("Failed to request new data channel: {}", e);
                            break;
                        }
                        continue;
                    }
                }
            }
            None => {
                error!("Data channel receiver closed");
                break;
            }
        };

        // 数据转发循环
        'data_loop: loop {
            tokio::select! {
                // 处理从UDP socket来的数据 (inbound)
                result = timeout(UDP_READ_TIMEOUT, l.recv_from(&mut buf)) => {
                    match result {
                        Ok(Ok((n, from))) => {
                            last_activity = Instant::now();
                            match timeout(UDP_WRITE_TIMEOUT, UdpTraffic::write_slice(&mut conn, from, &buf[..n])).await {
                                Ok(Ok(_)) => {},
                                Ok(Err(e)) => {
                                    error!("Failed to write UDP traffic to client: {}", e);
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("Failed to request new data channel: {}", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                },
                                Err(_) => {
                                    error!("Timeout writing UDP traffic to client");
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("Failed to request new data channel: {}", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                }
                            }
                        },
                        Ok(Err(e)) => {
                            error!("UDP socket recv_from error: {}", e);
                        },
                        Err(_) => {
                            // 读取超时，检查整体活动超时
                            if last_activity.elapsed() > UDP_ACTIVITY_TIMEOUT {
                                warn!("UDP connection inactive for too long, reconnecting...");
                                if let Err(e) = data_ch_req_tx.send(true).await {
                                    error!("Failed to request new data channel: {}", e);
                                    break 'main_loop;
                                }
                                break 'data_loop;
                            }
                        }
                    }
                },

                // 处理从客户端来的数据 (outbound)
                result = timeout(UDP_READ_TIMEOUT, conn.read_u8()) => {
                    match result {
                        Ok(Ok(hdr_len)) => {
                            match timeout(UDP_READ_TIMEOUT, UdpTraffic::read(&mut conn, hdr_len)).await {
                                Ok(Ok(traffic)) => {
                                    last_activity = Instant::now();
                                    if let Err(e) = l.send_to(&traffic.data, traffic.from).await {
                                        error!("Failed to send UDP data to visitor: {}", e);
                                    }
                                },
                                Ok(Err(e)) => {
                                    error!("Failed to read UDP traffic from client: {}", e);
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("Failed to request new data channel: {}", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                },
                                Err(_) => {
                                    error!("Timeout reading UDP traffic from client");
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("Failed to request new data channel: {}", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                }
                            }
                        },
                        Ok(Err(e)) => {
                            error!("UDP connection read error: {}", e);
                            // 连接失败，重建
                            if let Err(e) = data_ch_req_tx.send(true).await {
                                error!("Failed to request new data channel: {}", e);
                                break 'main_loop;
                            }
                            break 'data_loop;
                        },
                        Err(_) => {
                            // 读取超时，检查整体活动超时
                            if last_activity.elapsed() > UDP_ACTIVITY_TIMEOUT {
                                warn!("UDP connection inactive for too long, reconnecting...");
                                if let Err(e) = data_ch_req_tx.send(true).await {
                                    error!("Failed to request new data channel: {}", e);
                                    break 'main_loop;
                                }
                                break 'data_loop;
                            }
                        }
                    }
                },

                // 处理关闭信号
                _ = shutdown_rx.recv() => {
                    debug!("UDP pool shutdown signal received");
                    break 'main_loop;
                }
            }
        }
    }

    debug!("UDP pool dropped");
    Ok(())
}
