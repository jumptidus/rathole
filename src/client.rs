use crate::config::{ClientConfig, ClientServiceConfig, Config, ServiceType, TransportType, DEFAULT_MUX_MAX_STREAMS};
use crate::config_watcher::{ClientServiceChange, ConfigChange};
use crate::data_channel_handler::{get_data_channel_tcp_handler, get_data_channel_udp_handler};
use crate::data_channel_limit::get_data_channel_limiter;
use crate::protocol::Hello::{self, *};
use crate::protocol::{
    self, read_ack, read_control_cmd, read_data_cmd, read_hello, write_data_channel_mode,
    write_mux_resp, Ack, Auth, ControlChannelCmd, ControlChannelMuxResp, DataChannelCmd,
    DataChannelMode, MuxRespKind, CURRENT_PROTO_VERSION, HASH_WIDTH_IN_BYTES, PROTO_V3,
};
use crate::transport::{AddrMaybeCached, SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use futures::future::poll_fn;
use futures::io::{AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite};
use std::collections::HashMap;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error, info, instrument, warn, Instrument, Span};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config as YamuxConfig, Connection as YamuxConnection, Mode as YamuxMode};

#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;

use crate::constants::run_control_chan_backoff;

// The entrypoint of running a client
pub async fn run_client(
    config: Config,
    shutdown_rx: broadcast::Receiver<bool>,
    update_rx: mpsc::Receiver<ConfigChange>,
    timestamp: u64,
) -> Result<()> {
    let config = config.client.ok_or_else(|| {
        anyhow!(
        "Try to run as a client, but the configuration is missing. Please add the `[client]` block"
    )
    })?;

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut client = Client::<TcpTransport>::from(config).await?;
            client.run(shutdown_rx, update_rx, timestamp).await
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut client = Client::<TlsTransport>::from(config).await?;
                client.run(shutdown_rx, update_rx).await
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut client = Client::<NoiseTransport>::from(config).await?;
                client.run(shutdown_rx, update_rx, timestamp).await
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut client = Client::<WebsocketTransport>::from(config).await?;
                client.run(shutdown_rx, update_rx).await
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }
}

type ServiceDigest = protocol::Digest;
type Nonce = protocol::Digest;

// Holds the state of a client
struct Client<T: Transport> {
    config: ClientConfig,
    service_handles: HashMap<String, ControlChannelHandle>,
    transport: Arc<T>,
}

impl<T: 'static + Transport> Client<T> {
    // Create a Client from `[client]` config block
    async fn from(config: ClientConfig) -> Result<Client<T>> {
        let transport =
            Arc::new(T::new(&config.transport).with_context(|| "Failed to create the transport")?);
        Ok(Client {
            config,
            service_handles: HashMap::new(),
            transport,
        })
    }

    // The entrypoint of Client
    async fn run(
        &mut self,
        mut shutdown_rx: broadcast::Receiver<bool>,
        mut update_rx: mpsc::Receiver<ConfigChange>,
        timestamp: u64,
    ) -> Result<()> {
        for (name, config) in &self.config.services {
            // Create a control channel for each service defined
            let handle = ControlChannelHandle::new(
                (*config).clone(),
                self.config.remote_addr.clone(),
                self.transport.clone(),
                self.config.heartbeat_timeout,
                timestamp,
            );
            self.service_handles.insert(name.clone(), handle);
        }

        // Wait for the shutdown signal
        loop {
            tokio::select! {
                val = shutdown_rx.recv() => {
                    match val {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Unable to listen for shutdown signal: {}", err);
                        }
                    }
                    break;
                },
                e = update_rx.recv() => {
                    if let Some(e) = e {
                        self.handle_hot_reload(e, timestamp).await;
                    }
                }
            }
        }

        // Shutdown all services
        for (_, handle) in self.service_handles.drain() {
            handle.shutdown();
        }

        Ok(())
    }

    async fn handle_hot_reload(&mut self, e: ConfigChange, timestamp: u64) {
        match e {
            ConfigChange::ClientChange(client_change) => match client_change {
                ClientServiceChange::Add(cfg) => {
                    let name = cfg.name.clone();
                    let handle = ControlChannelHandle::new(
                        cfg,
                        self.config.remote_addr.clone(),
                        self.transport.clone(),
                        self.config.heartbeat_timeout,
                        timestamp,
                    );
                    let _ = self.service_handles.insert(name, handle);
                }
                ClientServiceChange::Delete(s) => {
                    let _ = self.service_handles.remove(&s);
                }
            },
            ignored => warn!("Ignored {:?} since running as a client", ignored),
        }
    }
}

struct RunDataChannelArgs<T: Transport> {
    session_key: Nonce,
    remote_addr: AddrMaybeCached,
    connector: Arc<T>,
    socket_opts: SocketOpts,
    service: ClientServiceConfig,
}

async fn do_data_channel_handshake<T: Transport>(
    args: Arc<RunDataChannelArgs<T>>,
    mode: DataChannelMode,
) -> Result<T::Stream> {
    // Retry at least every 100ms, at most for 10 seconds
    let backoff = ExponentialBuilder::default()
        .with_factor(2.0)
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_millis(100))
        .with_total_delay(Some(Duration::from_secs(10)))
        .without_max_times()
        .with_jitter();

    // Connect to remote_addr
    let mut conn: T::Stream = (|| async {
        args.connector
            .connect(&args.remote_addr)
            .await
            .with_context(|| format!("Failed to connect to {}", &args.remote_addr))
    })
    .retry(backoff)
    .sleep(tokio::time::sleep)
    .notify(|e: &anyhow::Error, duration| {
        warn!("{:#}. Retry in {:?}", e, duration);
    })
    .await?;

    T::hint(&conn, args.socket_opts);

    // Send nonce
    let v: &[u8; HASH_WIDTH_IN_BYTES] = args.session_key[..].try_into().unwrap();
    let hello = Hello::DataChannelHello(CURRENT_PROTO_VERSION, v.to_owned());
    conn.write_all(&bincode::serialize(&hello).unwrap()).await?;
    if CURRENT_PROTO_VERSION == PROTO_V3 {
        write_data_channel_mode(&mut conn, mode).await?;
    }
    conn.flush().await?;

    Ok(conn)
}

async fn run_data_channel<T: Transport>(args: Arc<RunDataChannelArgs<T>>) -> Result<()> {
    // Do the handshake
    let mut conn = do_data_channel_handshake(args.clone(), DataChannelMode::Plain).await?;

    // Forward
    match read_data_cmd(&mut conn).await? {
        DataChannelCmd::StartForwardTcp => {
            if args.service.service_type != ServiceType::Tcp {
                bail!("Expect TCP traffic. Please check the configuration.")
            }
            run_data_channel_for_tcp(conn, &args.service.name, &args.service.local_addr)
                .await?;
        }
        DataChannelCmd::StartForwardUdp => {
            if args.service.service_type != ServiceType::Udp {
                bail!("Expect UDP traffic. Please check the configuration.")
            }
            run_data_channel_for_udp::<T>(conn, &args.service).await?;
        }
    }
    Ok(())
}

// TCP 数据通道直连处理
#[instrument(skip(conn))]
async fn run_data_channel_for_tcp<S>(
    conn: S,
    service_name: &str,
    _local_addr: &str,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    debug!("数据通道开始转发");

    if let Some(handler) = get_data_channel_tcp_handler(service_name) {
        return handler(service_name, Box::new(conn)).await;
    }
    bail!("未注册 TCP 直连处理器: {}", service_name);
}

struct MuxActiveGuard {
    active: Arc<AtomicU64>,
}

impl Drop for MuxActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Release);
    }
}

async fn run_data_mux_with_resp<T: Transport>(
    args: Arc<RunDataChannelArgs<T>>,
    active: Arc<AtomicU64>,
    max_pool: usize,
) -> ControlChannelMuxResp {
    let max_pool_u16 = (max_pool as u64).min(u16::MAX as u64) as u16;
    let current = active.fetch_add(1, Ordering::AcqRel) as usize;
    if current >= max_pool {
        active.fetch_sub(1, Ordering::Release);
        let active_u16 = active.load(Ordering::Acquire).min(u16::MAX as u64) as u16;
        warn!(
            service = %args.service.name,
            max_pool,
            "mux 连接已达上限, 忽略 CreateDataMux"
        );
        return ControlChannelMuxResp {
            kind: MuxRespKind::Rejected,
            max_pool: max_pool_u16,
            active: active_u16,
        };
    }
    let active_for_resp = Arc::clone(&active);
    let guard = MuxActiveGuard { active };

    let conn = match do_data_channel_handshake(args.clone(), DataChannelMode::Mux).await {
        Ok(conn) => conn,
        Err(e) => {
            warn!(service = %args.service.name, "mux 握手失败: {:#}", e);
            drop(guard);
            let active_u16 =
                active_for_resp.load(Ordering::Acquire).min(u16::MAX as u64) as u16;
            return ControlChannelMuxResp {
                kind: MuxRespKind::Failed,
                max_pool: max_pool_u16,
                active: active_u16,
            };
        }
    };

    let mut cfg = YamuxConfig::default();
    cfg.set_max_num_streams(DEFAULT_MUX_MAX_STREAMS);
    let yamux_conn = YamuxConnection::new(conn.compat(), cfg, YamuxMode::Client);
    let service = args.service.clone();
    tokio::spawn(async move {
        let _guard = guard;
        if let Err(e) = run_mux_client(yamux_conn, service).await {
            warn!("{:#}", e);
        }
    });

    let active_u16 = active_for_resp.load(Ordering::Acquire).min(u16::MAX as u64) as u16;
    ControlChannelMuxResp {
        kind: MuxRespKind::Accepted,
        max_pool: max_pool_u16,
        active: active_u16,
    }
}

async fn run_mux_client<T: FuturesAsyncRead + FuturesAsyncWrite + Unpin + Send + 'static>(
    mut conn: YamuxConnection<T>,
    service: ClientServiceConfig,
) -> Result<()> {
    loop {
        let inbound = poll_fn(|cx| conn.poll_next_inbound(cx)).await;
        match inbound {
            Some(Ok(stream)) => {
                let service_clone = service.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_mux_stream(stream, service_clone).await {
                        warn!("{:#}", e);
                    }
                });
            }
            Some(Err(e)) => {
                warn!("mux 连接错误: {}", e);
                break;
            }
            None => {
                break;
            }
        }
    }
    Ok(())
}

async fn handle_mux_stream(stream: yamux::Stream, service: ClientServiceConfig) -> Result<()> {
    let mut stream = stream.compat();
    match read_data_cmd(&mut stream).await? {
        DataChannelCmd::StartForwardTcp => {
            if service.service_type != ServiceType::Tcp {
                bail!("Expect TCP traffic. Please check the configuration.")
            }
            run_data_channel_for_tcp(stream, &service.name, &service.local_addr).await?;
        }
        DataChannelCmd::StartForwardUdp => {
            warn!(
                service = %service.name,
                "mux 暂不支持 UDP, 已拒绝"
            );
        }
    }
    Ok(())
}

#[instrument(skip(conn))]
async fn run_data_channel_for_udp<T: Transport>(
    conn: T::Stream,
    service: &ClientServiceConfig,
) -> Result<()> {
    debug!("New data channel starts forwarding");

    if let Some(handler) = get_data_channel_udp_handler(&service.name) {
        return handler(service.clone(), Box::new(conn)).await;
    }
    bail!("未注册 UDP 直连处理器: {}", service.name);
}

fn ensure_data_channel_handler(service: &ClientServiceConfig) -> Result<()> {
    match service.service_type {
        ServiceType::Tcp => {
            if get_data_channel_tcp_handler(&service.name).is_none() {
                bail!("未注册 TCP 直连处理器: {}", service.name);
            }
        }
        ServiceType::Udp => {
            if get_data_channel_udp_handler(&service.name).is_none() {
                bail!("未注册 UDP 直连处理器: {}", service.name);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_channel_handler::{
        unregister_data_channel_tcp_handler, unregister_data_channel_udp_handler,
    };
    use crate::config::TransportConfig;
    use crate::transport::{AddrMaybeCached, SocketOpts, Transport};
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{duplex, ReadBuf};

    struct TestStream {
        inner: tokio::io::DuplexStream,
    }

    impl TestStream {
        fn new(inner: tokio::io::DuplexStream) -> Self {
            Self { inner }
        }
    }

    impl std::fmt::Debug for TestStream {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("TestStream")
        }
    }

    impl AsyncRead for TestStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for TestStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[derive(Debug)]
    struct DummyTransport;

    #[async_trait]
    impl Transport for DummyTransport {
        type Acceptor = ();
        type RawStream = ();
        type Stream = TestStream;

        fn new(_config: &TransportConfig) -> Result<Self> {
            Ok(Self)
        }

        fn hint(_conn: &Self::Stream, _opts: SocketOpts) {}

        async fn bind<T: tokio::net::ToSocketAddrs + Send + Sync>(
            &self,
            _addr: T,
        ) -> Result<Self::Acceptor> {
            Err(anyhow!("测试用 DummyTransport 不支持 bind"))
        }

        async fn accept(&self, _a: &Self::Acceptor) -> Result<(Self::RawStream, SocketAddr)> {
            Err(anyhow!("测试用 DummyTransport 不支持 accept"))
        }

        async fn handshake(&self, _conn: Self::RawStream) -> Result<Self::Stream> {
            Err(anyhow!("测试用 DummyTransport 不支持 handshake"))
        }

        async fn connect(&self, _addr: &AddrMaybeCached) -> Result<Self::Stream> {
            Err(anyhow!("测试用 DummyTransport 不支持 connect"))
        }
    }

    #[tokio::test]
    async fn tcp_handler_missing_fast_fail() {
        let service_name = "test_tcp_handler_missing_fast_fail";
        let _ = unregister_data_channel_tcp_handler(service_name);

        let (client, _server) = duplex(64);
        let res = tokio::time::timeout(
            Duration::from_millis(50),
            run_data_channel_for_tcp(client, service_name, "127.0.0.1:0"),
        )
        .await
        .expect("测试超时");

        let err = res.expect_err("应快速失败");
        assert!(
            err.to_string().contains("未注册 TCP 直连处理器"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn udp_handler_missing_fast_fail() {
        let service_name = "test_udp_handler_missing_fast_fail";
        let _ = unregister_data_channel_udp_handler(service_name);

        let service = ClientServiceConfig {
            service_type: ServiceType::Udp,
            name: service_name.to_string(),
            local_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let (client, _server) = duplex(64);
        let conn = TestStream::new(client);

        let res = tokio::time::timeout(
            Duration::from_millis(50),
            run_data_channel_for_udp::<DummyTransport>(conn, &service),
        )
        .await
        .expect("测试超时");

        let err = res.expect_err("应快速失败");
        assert!(
            err.to_string().contains("未注册 UDP 直连处理器"),
            "unexpected error: {}",
            err
        );
    }
}

// Control channel, using T as the transport layer
struct ControlChannel<T: Transport> {
    digest: ServiceDigest,              // SHA256 of the service name
    service: ClientServiceConfig,       // `[client.services.foo]` config block
    shutdown_rx: oneshot::Receiver<u8>, // Receives the shutdown signal
    remote_addr: String,                // `client.remote_addr`
    transport: Arc<T>,                  // Wrapper around the transport layer
    heartbeat_timeout: u64,             // Application layer heartbeat timeout in secs
    mux_active: Arc<AtomicU64>,
    mux_max_pool: usize,
}

// Handle of a control channel
// Dropping it will also drop the actual control channel
struct ControlChannelHandle {
    shutdown_tx: oneshot::Sender<u8>,
}

impl<T: 'static + Transport> ControlChannel<T> {
    #[instrument(skip_all)]
    async fn run(&mut self, timestamp: u64) -> Result<()> {
        let mut remote_addr = AddrMaybeCached::new(&self.remote_addr);
        remote_addr.resolve().await?;

        let mut conn = self
            .transport
            .connect(&remote_addr)
            .await
            .with_context(|| format!("Failed to connect to {}", &self.remote_addr))?;
        T::hint(&conn, SocketOpts::for_control_channel());

        // Send hello
        debug!("Sending hello");
        let hello_send =
            Hello::ControlChannelHello(CURRENT_PROTO_VERSION, self.digest[..].try_into().unwrap());
        conn.write_all(&bincode::serialize(&hello_send).unwrap())
            .await?;

        // 0.5.1版本 增加发送 timestamp
        if CURRENT_PROTO_VERSION == PROTO_V3 {
            conn.write_all(&timestamp.to_le_bytes()).await?;
            debug!("timestamp: {}", timestamp);
        }

        conn.flush().await?;

        // Read hello
        debug!("Reading hello");
        let nonce = match read_hello(&mut conn).await? {
            ControlChannelHello(_, d) => d,
            _ => {
                bail!("Unexpected type of hello");
            }
        };

        // Send auth
        debug!("Sending auth");
        let mut concat = Vec::from(self.service.token.as_ref().unwrap().as_bytes());
        concat.extend_from_slice(&nonce);

        let session_key = protocol::digest(&concat);
        let auth = Auth(session_key);
        conn.write_all(&bincode::serialize(&auth).unwrap()).await?;
        conn.flush().await?;

        // Read ack
        debug!("Reading ack");
        match read_ack(&mut conn).await? {
            Ack::Ok => {}
            Ack::RejectedDueToTimestamp => {
                return Err(anyhow!("{}", Ack::RejectedDueToTimestamp))
                    .with_context(|| format!("认证失败(时间戳过旧): {}", self.service.name));
            }
            v => {
                return Err(anyhow!("{}", v))
                    .with_context(|| format!("Authentication failed: {}", self.service.name));
            }
        }

        // Channel ready
        info!("Control channel established");

        // Socket options for the data channel
        let socket_opts = SocketOpts::from_client_cfg(&self.service);
        let data_ch_args = Arc::new(RunDataChannelArgs {
            session_key,
            remote_addr,
            connector: self.transport.clone(),
            socket_opts,
            service: self.service.clone(),
        });

        let (mut read_half, mut write_half) = tokio::io::split(conn);
        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);
        let (resp_tx, mut resp_rx) = mpsc::channel(64);

        let reader_handle = tokio::spawn(async move {
            loop {
                match read_control_cmd(&mut read_half).await {
                    Ok(cmd) => {
                        if cmd_tx.send(cmd).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("读取控制指令失败: {:#}", e);
                        break;
                    }
                }
            }
        });

        let writer_handle = tokio::spawn(async move {
            while let Some(resp) = resp_rx.recv().await {
                if let Err(e) = write_mux_resp(&mut write_half, &resp).await {
                    warn!("写入 mux 响应失败: {:#}", e);
                    break;
                }
            }
        });

        let mut result = Ok(());
        loop {
            tokio::select! {
                val = cmd_rx.recv() => {
                    let val = match val {
                        Some(v) => v,
                        None => {
                            result = Err(anyhow!("控制指令通道已关闭"));
                            break;
                        }
                    };
                    debug!( "Received {:?}", val);
                    match val {
                        ControlChannelCmd::CreateDataChannel => {
                            if let Err(e) = ensure_data_channel_handler(&self.service) {
                                result = Err(e);
                                break;
                            }
                            let args = data_ch_args.clone();
                            if let Some(limiter) = get_data_channel_limiter(&self.service.name) {
                                if let Some(permit) = limiter.try_acquire() {
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        if let Err(e) = run_data_channel(args)
                                            .await
                                            .with_context(|| "数据通道运行失败")
                                        {
                                            warn!("{:#}", e);
                                        }
                                    }
                                    .instrument(Span::current()));
                                } else {
                                    warn!(
                                        "数据通道已达到上限, 已拒绝创建: {}",
                                        self.service.name
                                    );
                                }
                            } else {
                                tokio::spawn(async move {
                                    if let Err(e) = run_data_channel(args)
                                        .await
                                        .with_context(|| "数据通道运行失败")
                                    {
                                        warn!("{:#}", e);
                                    }
                                }
                                .instrument(Span::current()));
                            }
                        },
                        ControlChannelCmd::CreateDataMux => {
                            if self.service.service_type != ServiceType::Tcp {
                                result = Err(anyhow!("mux 仅支持 TCP 服务: {}", self.service.name));
                                break;
                            }
                            if let Err(e) = ensure_data_channel_handler(&self.service) {
                                result = Err(e);
                                break;
                            }
                            let args = data_ch_args.clone();
                            let active = self.mux_active.clone();
                            let max_pool = self.mux_max_pool;
                            let resp_tx = resp_tx.clone();
                            tokio::spawn(async move {
                                let resp = run_data_mux_with_resp(args, active, max_pool).await;
                                if let Err(e) = resp_tx.send(resp).await {
                                    warn!("发送 mux 响应失败: {:#}", e);
                                }
                            }
                            .instrument(Span::current()));
                        },
                        ControlChannelCmd::HeartBeat => ()
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_timeout)), if self.heartbeat_timeout != 0 => {
                    result = Err(anyhow!("Heartbeat timed out"));
                    break;
                }
                _ = &mut self.shutdown_rx => {
                    break;
                }
            }
        }

        reader_handle.abort();
        writer_handle.abort();

        info!("Control channel shutdown");
        result
    }
}

impl ControlChannelHandle {
    #[instrument(name="handle", skip_all, fields(service = %service.name))]
    fn new<T: 'static + Transport>(
        service: ClientServiceConfig,
        remote_addr: String,
        transport: Arc<T>,
        heartbeat_timeout: u64,
        timestamp: u64,
    ) -> ControlChannelHandle {
        let digest = protocol::digest(service.name.as_bytes());

        info!("Starting {}", hex::encode(digest));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mux_active = Arc::new(AtomicU64::new(0));
        let mux_max_pool = service.mux_max_pool;

        let retry_backoff_builder = run_control_chan_backoff(60); // 最大 60s
        let mut retry_backoff = retry_backoff_builder.build();

        let mut s = ControlChannel {
            digest,
            service,
            shutdown_rx,
            remote_addr,
            transport,
            heartbeat_timeout,
            mux_active,
            mux_max_pool,
        };

        tokio::spawn(
            async move {
                let mut start = Instant::now();

                while let Err(err) = s
                    .run(timestamp)
                    .await
                    .with_context(|| "Failed to run the control channel")
                {
                    if s.shutdown_rx.try_recv() != Err(oneshot::error::TryRecvError::Empty) {
                        break;
                    }

                    if start.elapsed() > Duration::from_secs(10) {
                        // The client runs for at least 10 secs and then disconnects
                        retry_backoff = retry_backoff_builder.build();
                    }

                    if let Some(duration) = retry_backoff.next() {
                        error!("{:#}. Retry in {:?}...", err, duration);
                        time::sleep(duration).await;
                    }

                    start = Instant::now();
                }
            }
            .instrument(Span::current()),
        );

        ControlChannelHandle { shutdown_tx }
    }

    fn shutdown(self) {
        info!("Received shutdown signal!");
        // A send failure shows that the actor has already shutdown.
        let _ = self.shutdown_tx.send(0u8);
    }
}
