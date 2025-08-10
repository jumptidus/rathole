use crate::config::{Config, ServerConfig, ServerServiceConfig, ServiceType, TransportType};
use crate::config_watcher::{ConfigChange, ServerServiceChange};
use crate::constants::{listen_backoff, UDP_BUFFER_SIZE};
use crate::health::{
    tcp_socks5_http_probe_once, udp_socks5_dns_probe_once, HEALTH_PROBE_DEFAULT_HOST,
    HEALTH_PROBE_DEFAULT_INTERVAL_SECS, HEALTH_PROBE_DEFAULT_TIMEOUT_SECS,
    HEALTH_PROBE_DEFAULT_URL,
};
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

const TCP_POOL_SIZE: usize = 8; // TCP服务的缓存连接数
const UDP_POOL_SIZE: usize = 2; // UDP服务的缓存连接数
const CHAN_SIZE: usize = 2048; // 通道的容量
const DATA_CHANNEL_REQUEST_BUFFER: usize = CHAN_SIZE / 2; // 等待数据通道请求的缓冲区
const HANDSHAKE_TIMEOUT: u64 = 5; // 握手超时时间(秒)
const CONTROL_CHANNEL_WRITE_TIMEOUT: u64 = 5; // 控制通道写入超时(秒)
                                              // 健康探测默认参数（如需覆盖，可后续做成配置项）
const HEALTH_PROBE_INTERVAL_SECS: u64 = HEALTH_PROBE_DEFAULT_INTERVAL_SECS;
const HEALTH_PROBE_TIMEOUT_SECS: u64 = HEALTH_PROBE_DEFAULT_TIMEOUT_SECS;
const DNS_IP: [u8; 4] = [114, 114, 114, 114];
const DNS_PORT: u16 = 53;

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
        info!("开始监听: {}", self.config.bind_addr);

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
                                    error!("IO错误: {:#}. 重试中... {:?}...", err, d);
                                    time::sleep(d).await;
                                } else {
                                    // This branch will never be executed according to the current retry policy
                                    error!("[预期之外的错误] 当前重试策略不应到达,重试次数过多. 终止...");
                                    break;
                                }
                            } else {
                                // If it's not an IO error, then it comes from
                                // the transport layer, so ignore it
                                // just log it
                                error!("[预期之外的错误] 传输层错误: {:#}", err);
                            }
                        }
                        Ok((conn, addr)) => {
                            backoff.reset();

                            let transport = self.transport.clone();
                            let services = self.services.clone();
                            let control_channels = self.control_channels.clone();
                            let server_config = self.config.clone();

                            tokio::spawn(async move {
                                match time::timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), transport.handshake(conn)).await {
                                    Ok(conn_result) => {
                                        match conn_result.with_context(|| "握手失败") {
                                            Ok(stream) => {
                                                if let Err(err) = handle_connection(stream, services, control_channels, server_config, addr).await {
                                                    error!("握手成功, 处理连接失败: {:#}", err);
                                                }
                                            }, Err(e) => {
                                                error!("握手成功, 处理连接失败: {:#}", e);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        error!("握手超时: {}", e);
                                    }
                                }
                            }.instrument(info_span!("握手成功", %addr))); // 握手成功后，进入握手成功处理流程
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
    // 读取握手消息
    let hello = match timeout(
        Duration::from_secs(HANDSHAKE_TIMEOUT),
        read_hello(&mut conn),
    )
    .await
    {
        Ok(Ok(hello)) => hello,
        Ok(Err(e)) => {
            let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
            error!("读取握手消息失败: {}, 显式关闭连接以防资源泄露", e);
            return Err(e);
        }
        Err(_) => {
            let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
            error!("握手超时, 显式关闭连接以防资源泄露");
            bail!("握手超时");
        }
    };

    // 处理握手消息
    match hello {
        ControlChannelHello(protocol_version, service_digest) => {
            // 根据协议版本处理时间戳
            let timestamp = if protocol_version == PROTO_V2 {
                // 对于V2协议, 客户端必须发送时间戳
                match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), conn.read_u64_le()).await {
                    Ok(Ok(ts)) => {
                        debug!("成功读取V2客户端时间戳: {}", ts);
                        ts
                    }
                    Ok(Err(e)) => {
                        error!("读取V2客户端时间戳失败: {}, 中断连接", e);
                        let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
                        return Err(e.into());
                    }
                    Err(_) => {
                        error!("读取V2客户端时间戳超时, 中断连接");
                        let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
                        bail!("读取时间戳超时");
                    }
                }
            } else {
                // 对于旧版协议, 时间戳为0
                0
            };

            info!("客户端时间戳: {}", timestamp);

            // 处理控制通道握手
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
        "尝试建立控制通道, 客户端时间戳: {}, 地址: {}",
        timestamp, addr
    );

    T::hint(&conn, SocketOpts::for_control_channel());

    // 生成一个nonce
    let mut nonce = vec![0u8; HASH_WIDTH_IN_BYTES];
    rand::thread_rng().fill_bytes(&mut nonce);

    // 发送握手消息
    let nonce_array: [u8; HASH_WIDTH_IN_BYTES] =
        nonce.clone().try_into().map_err(|v: Vec<u8>| {
            anyhow::anyhow!(
                "Nonce 长度不匹配. 期望: {}, 实际: {}",
                HASH_WIDTH_IN_BYTES,
                v.len()
            )
        })?;
    let hello_send = ControlChannelHello(
        // 如果时间戳为0, 使用当前协议版本, 否则使用v2协议
        if timestamp == 0 {
            protocol::CURRENT_PROTO_VERSION
        } else {
            protocol::PROTO_V2
        },
        nonce_array,
    );
    conn.write_all(&bincode::serialize(&hello_send)?).await?;
    conn.flush().await?;

    // 查找服务
    let service_config = match services.read().await.get(&service_digest) {
        Some(v) => v,
        None => {
            conn.write_all(&bincode::serialize(&Ack::ServiceNotExist)?)
                .await?;
            bail!("未找到服务: {}", hex::encode(service_digest));
        }
    }
    .to_owned();

    let service_name = service_config.name.clone();

    // 计算校验和
    let token = match service_config.token.as_ref() {
        Some(t) => t.as_bytes(),
        None => {
            error!(service = %service_name, "认证失败: 服务未配置token");
            conn.write_all(&bincode::serialize(&Ack::AuthFailed)?)
                .await?;
            bail!("服务 {} 未配置token", service_name);
        }
    };
    let mut concat = Vec::from(token);
    concat.append(&mut nonce);

    // 读取认证
    let protocol::Auth(d) =
        match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), read_auth(&mut conn)).await {
            Ok(Ok(auth)) => auth,
            Ok(Err(e)) => {
                error!("读取认证失败: {}", e);
                return Err(e);
            }
            Err(_) => {
                error!("读取认证超时");
                bail!("认证超时");
            }
        };

    // 验证
    let session_key = protocol::digest(&concat);
    if session_key != d {
        conn.write_all(&bincode::serialize(&Ack::AuthFailed)?)
            .await?;
        debug!(
            "认证失败, 期望 {}, 实际 {}",
            hex::encode(session_key),
            hex::encode(d)
        );
        bail!("服务 {} 认证失败", service_name);
    } else {
        // 检查是否存在一个相同的通道, 并比较时间戳
        let existing_channel = {
            let control_map_guard = control_channels.read().await;
            if let Some(existing) = control_map_guard.get1(&service_digest) {
                // 如果旧的时间戳大于新时间戳, 拒绝新连接
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp,
                    old_addr = %existing.addr,
                    new_ts = timestamp,
                    new_addr = %addr,
                    "找到一个相同的通道"
                );
                if existing.timestamp >= timestamp {
                    info!(
                        service = %service_name,
                        old_ts = existing.timestamp,
                        old_addr = %existing.addr,
                        new_ts = timestamp,
                        new_addr = %addr,
                        "拒绝连接: 旧时间戳:{} >= 新时间戳:{}",
                        existing.timestamp, timestamp
                    );
                    conn.write_all(&bincode::serialize(&Ack::AuthFailed)?)
                        .await?;
                    return Ok(());
                }
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp,
                    old_addr = %existing.addr,
                    new_ts = timestamp,
                    new_addr = %addr,
                    "替换旧通道"
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
                "创建或替换控制通道"
            );
        }

        // 准备控制通道句柄和控制任务
        let (handle, control_task_future) = ControlChannelHandle::prepare(
            conn,
            service_config.clone(),
            server_config.heartbeat_interval,
            timestamp,
            addr,
        );

        let control_channels_weak = Arc::downgrade(&control_channels); // 弱引用控制通道句柄
        let control_channels_weak_for_cleanup = control_channels_weak.clone();

        // 插入句柄到映射中(在写锁范围内)
        {
            let mut control_map_guard = control_channels.write().await;

            if let Some(existing) = control_map_guard.get1(&service_digest) {
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp,
                    new_ts = timestamp,
                    old_addr = %existing.addr,
                    new_addr = %addr,
                    session_key = %hex::encode(session_key),
                    "替换旧通道, 创建新控制通道"
                );
                let _ = control_map_guard.remove1(&service_digest);
            } else if control_map_guard.get2(&session_key).is_some() {
                warn!(
                    service = %service_name,
                    session_key = %hex::encode(session_key),
                    "检测到潜在的会话密钥冲突, 移除旧条目"
                );
                let _ = control_map_guard.remove2(&session_key);
            }

            // 插入新句柄. `handle` 被移动到映射中
            let _ = control_map_guard.insert(service_digest, session_key, handle);
            info!(
                service = %service_name,
                session_key = %hex::encode(session_key),
                ts = timestamp,
                addr = %addr,
                "控制通道句柄插入成功"
            );
        }

        // 克隆名称再次用于跨度, 因为原始名称被移动到任务中
        let service_name_for_control_span = service_name.clone();

        // 启动控制任务
        let service_name_for_control_task = service_name.clone();
        let control_task_handle = tokio::spawn(
            async move {
                // `prepare` 返回的 future 拥有连接并运行 `ControlChannel::run`
                if let Err(err) = control_task_future.await {
                    error!(service = %service_name_for_control_task, session_key = %hex::encode(session_key), "控制通道任务失败: {:#}", err);
                } else {
                    debug!(service = %service_name_for_control_task, session_key = %hex::encode(session_key), "控制通道任务完成");
                }
            }
            // 使用新克隆的变量进行跨度
            .instrument(info_span!("控制通道任务", service = %service_name_for_control_span, session_key = %hex::encode(session_key))),
        );

        // 为清理任务克隆会话密钥
        let session_key_for_cleanup = session_key;

        // 启动清理任务
        tokio::spawn(
            async move {
                // 等待主控制任务完成(正常或异常)
                match control_task_handle.await {
                    Ok(_) => {
                        debug!(session_key = %hex::encode(session_key_for_cleanup), "控制通道任务完成, 准备清理");
                    },
                    Err(e) => {
                        error!(session_key = %hex::encode(session_key_for_cleanup), "控制通道任务失败, 准备清理: {}", e);
                    }
                }

                if let Some(map_arc) = control_channels_weak_for_cleanup.upgrade() {
                    debug!(session_key = %hex::encode(session_key_for_cleanup), "获取锁, 准备清理");
                    let mut map_guard = map_arc.write().await;
                    if let Some(_removed_handle) = map_guard.remove2(&session_key_for_cleanup) {
                        info!(session_key = %hex::encode(session_key_for_cleanup), "控制通道句柄清理成功");
                    } else {
                        warn!(session_key = %hex::encode(session_key_for_cleanup), "控制通道句柄在清理前被移除");
                    }
                } else {
                    warn!(session_key = %hex::encode(session_key_for_cleanup), "控制通道映射在清理前被释放");
                }
            }
            // 使用特定于跨度的克隆的会话密钥来记录清理任务
            .instrument(info_span!("cleanup_task", session_key = %hex::encode(session_key_for_cleanup))),
        );

        // 启动健康探测任务（按服务类型）
        {
            let control_channels_weak_for_probe = control_channels_weak.clone();
            let session_key_for_probe = session_key_for_cleanup;
            let service_name_for_probe_log = service_name.clone();
            let service_name_for_probe_span = service_name.clone();
            let bind_addr_for_probe = service_config.bind_addr.clone();

            match service_config.service_type {
                ServiceType::Tcp => {
                    tokio::spawn(
                        async move {
                            let interval = Duration::from_secs(HEALTH_PROBE_INTERVAL_SECS);
                            loop {
                                time::sleep(interval).await;
                                match tcp_socks5_http_probe_once(
                                    &bind_addr_for_probe,
                                    HEALTH_PROBE_DEFAULT_HOST,
                                    HEALTH_PROBE_DEFAULT_URL,
                                    HEALTH_PROBE_TIMEOUT_SECS,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        debug!(service = %service_name_for_probe_log, "TCP 健康探测成功");
                                    }
                                    Err(e) => {
                                        warn!(service = %service_name_for_probe_log, "TCP 健康探测失败: {:#}", e);
                                        if let Some(map_arc) = control_channels_weak_for_probe.upgrade() {
                                            let mut map = map_arc.write().await;
                                            if map.remove2(&session_key_for_probe).is_some() {
                                                error!(service = %service_name_for_probe_log, "健康探测失败，移除控制通道，等待客户端重连");
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                        .instrument(info_span!("tcp_health_probe", service = %service_name_for_probe_span)),
                    );
                }
                ServiceType::Udp => {
                    tokio::spawn(
                        async move {
                            let interval = Duration::from_secs(HEALTH_PROBE_INTERVAL_SECS);
                            loop {
                                time::sleep(interval).await;
                                match udp_socks5_dns_probe_once(
                                    &bind_addr_for_probe,
                                    DNS_IP,
                                    DNS_PORT,
                                    HEALTH_PROBE_DEFAULT_HOST,
                                    HEALTH_PROBE_TIMEOUT_SECS,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        debug!(service = %service_name_for_probe_log, "UDP 健康探测成功");
                                    }
                                    Err(e) => {
                                        warn!(service = %service_name_for_probe_log, "UDP 健康探测失败: {:#}", e);
                                        if let Some(map_arc) = control_channels_weak_for_probe.upgrade() {
                                            let mut map = map_arc.write().await;
                                            if map.remove2(&session_key_for_probe).is_some() {
                                                error!(service = %service_name_for_probe_log, "健康探测失败，移除控制通道，等待客户端重连");
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                        .instrument(info_span!("udp_health_probe", service = %service_name_for_probe_span)),
                    );
                }
            }
        }
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
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1); // 关闭channel
        let (data_ch_tx, data_ch_rx) = mpsc::channel(CHAN_SIZE * 2); // 数据channel队列
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::channel(DATA_CHANNEL_REQUEST_BUFFER); // 缓冲区

        // 获得 TCP 或 UDP 的服务池大小
        let pool_size = match service.service_type {
            ServiceType::Tcp => TCP_POOL_SIZE,
            ServiceType::Udp => UDP_POOL_SIZE,
        };

        // 启动连接池任务 (获取相关通道的所有权)
        let service_name_clone = service.name.clone();

        // 克隆服务名称用于跨度
        let service_name_for_tcp_span = service_name_clone.clone();
        let service_name_for_udp_span = service_name_clone.clone();

        match service.service_type {
            ServiceType::Tcp => {
                let shutdown_rx_clone = shutdown_tx.subscribe(); // 订阅关闭channel
                let bind_addr = service.bind_addr.clone(); // 绑定地址
                let data_ch_req_tx_clone = data_ch_req_tx.clone(); // 克隆发送者
                tokio::spawn(
                    async move {
                        // 运行TCP连接池任务
                        if let Err(e) = run_tcp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,
                            data_ch_req_tx_clone,
                            shutdown_rx_clone,
                        )
                        .await
                        .with_context(|| "TCP 连接池任务失败")
                        {
                            error!("TCP 连接池任务失败: {:#}", e);
                        }
                        debug!(service = %service_name_clone, "TCP 连接池任务结束.");
                    }
                    .instrument(info_span!("tcp_pool", service = %service_name_for_tcp_span)),
                );
            }
            ServiceType::Udp => {
                let shutdown_rx_clone = shutdown_tx.subscribe();
                let bind_addr = service.bind_addr.clone();
                let data_ch_req_tx_clone = data_ch_req_tx.clone(); // Clone sender for the pool task
                tokio::spawn(
                    async move {
                        if let Err(e) = run_udp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,           // data_ch_rx moved here
                            data_ch_req_tx_clone, // Pass req channel
                            shutdown_rx_clone,
                        )
                        .await
                        .with_context(|| "UDP 连接池任务失败")
                        {
                            error!("UDP 连接池任务失败: {:#}", e);
                        }
                        // Use moved service_name_clone for debug log
                        debug!(service = %service_name_clone, "UDP 连接池任务结束.");
                    }
                    // Use span-specific clone
                    .instrument(info_span!("udp_pool", service = %service_name_for_udp_span)),
                );
            }
        };

        // 创建控制通道状态结构
        // (takes ownership of conn, shutdown_rx, data_ch_req_rx)
        let ch = ControlChannel::<T> {
            conn,
            shutdown_rx,
            data_ch_req_rx,
            heartbeat_interval,
            pool_size,
            data_ch_req_tx: data_ch_req_tx.clone(),
        };

        // 创建控制通道句柄实例（返回给调用者）
        let handle = ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            data_ch_tx,
            service,
            timestamp,
            addr,
        };

        // 创建控制通道 Future，将执行控制通道逻辑
        let control_task_future = async move { ch.run().await }.instrument(Span::current());

        // 创建控制通道句柄实例（返回给调用者）
        (handle, control_task_future)
    }
}

// Control channel, using T as the transport layer.
struct ControlChannel<T: Transport> {
    conn: T::Stream, // The connection of control channel // 控制通道连接
    shutdown_rx: broadcast::Receiver<bool>, // Receives the shutdown signal // 接收关闭信号
    data_ch_req_rx: mpsc::Receiver<bool>, // Receives visitor connections (Bounded Receiver) // 接收访客连接请求（有界接收器）
    heartbeat_interval: u64, // Application-layer heartbeat interval in secs // 应用层心跳间隔（秒）
    pool_size: usize,        // Initial pool size to request // 初始池大小请求
    data_ch_req_tx: mpsc::Sender<bool>, // Sender to request data channels (Bounded Sender) // 发送器请求数据通道（有界发送器）
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
                info!("控制通道建立成功并已确认");
            }
            Err(e) => {
                error!("发送 Ack::Ok 失败, 关闭控制通道: {:#}", e);
                return Err(e);
            }
        }

        // 发送初始数据通道请求...
        debug!(
            pool_size = self.pool_size,
            "发送初始数据通道请求..., 池大小: {}", self.pool_size
        );
        for i in 0..self.pool_size {
            if let Err(e) = self.data_ch_req_tx.send(true).await {
                error!(
                    "发送初始数据通道请求失败 #{}/{}: {}, 控制通道关闭",
                    i + 1,
                    self.pool_size,
                    e
                );
                return Err(anyhow!("数据通道请求队列在初始化期间意外关闭: {}", e));
            }
        }
        debug!("发送初始数据通道请求成功, 数量: {}", self.pool_size);

        let create_ch_cmd = bincode::serialize(&ControlChannelCmd::CreateDataChannel)?;
        let heartbeat = bincode::serialize(&ControlChannelCmd::HeartBeat)?;

        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(_) => {
                            let write_future = self.write_and_flush(&create_ch_cmd);
                            match time::timeout(Duration::from_secs(CONTROL_CHANNEL_WRITE_TIMEOUT), write_future).await {
                                Ok(Ok(_)) => {}, // 写入成功
                                Ok(Err(e)) => {
                                    error!("写入 创建数据通道 失败: {:#}", e);
                                    break;
                                }
                                Err(_) => {
                                    error!("写入 创建数据通道 超时");
                                    break;
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_interval)), if self.heartbeat_interval != 0 => {
                    let write_future = self.write_and_flush(&heartbeat);
                    match time::timeout(Duration::from_secs(CONTROL_CHANNEL_WRITE_TIMEOUT), write_future).await {
                        Ok(Ok(_)) => {}, // 写入成功
                        Ok(Err(e)) => {
                            error!("写入心跳失败: {:#}", e);
                            break;
                        }
                        Err(_) => {
                            error!("写入心跳超时");
                            break;
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("控制通道关闭");

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
            error!("{:#}. 重试间隔: {:?}", e, duration);
        }, &mut shutdown_rx).await
        .with_context(|| "监听服务失败");

        let l: TcpListener = match l {
            Ok(v) => v,
            Err(e) => {
                error!("监听服务失败: {:#}", e);
                return;
            }
        };

        info!("开始监听: {}", &addr);

        // 重试至少每1秒
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_secs(1),
            max_elapsed_time: None,
            ..Default::default()
        };

		// 主循环
        loop {
            tokio::select! {
                val = l.accept() => {
                    match val {
                        Err(e) => {
                            // `l` 是 TCP 监听器, 所以这必须是 IO 错误
                            // 可能是 EMFILE. 所以等待一段时间
                            error!("{}. 等待一段时间", e);
                            if let Some(d) = backoff.next_backoff() {
                                time::sleep(d).await;
                            } else {
                                // 重试次数太多, 退出
                                error!("[FOCUS ERROR] TCP 监听 Accept 重试次数到达极限. 退出...");
                                break;
                            }
                        }
                        Ok((incoming, addr)) => {
                            // 对于每个访问者, 请求创建一个数据通道
                            // 使用 .await 和处理有界通道发送的错误
                            if let Err(e) = data_ch_req_tx.send(true).await {
								error!("发送数据通道请求失败 (可能控制通道已关闭): {}. 监听器退出.", e);
                                break; // 如果发送失败, 退出循环
                            }
                            backoff.reset(); // 重置重试计数器
                            debug!("新的客户端连接: {}", addr);

                            // 将访问者发送到连接池
                            let _ = tx.send(incoming).await;
                        }
                    }
                },
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("TCP监听器关闭");
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
                // 写入开始传输Tcp数据指令
                if write_and_flush(&mut ch, &cmd).await.is_ok() {
                    // 开始传输数据
                    tokio::spawn(async move {
                        let _ = copy_bidirectional(&mut ch, &mut visitor).await;
                    });
                    break;
                } else {
                    // The Current data channel is broken.
                    // Request for a new one
                    // Use .await and handle error for the bounded channel send
                    // 如果当前数据通道已损坏，则请求一个新的数据通道
                    // 使用 .await 并且处理错误
                    if let Err(e) = data_ch_req_tx.send(true).await {
                        error!(
                            "发送新建数据通道请求失败(控制通道可能已关闭): {}. 关闭连接池.",
                            e
                        );
                        break 'pool; // 控制通道可能已关闭,数据通道无法新建,现在关闭连接池.
                    }
                }
            } else {
                break 'pool;
            }
        }
    }

    info!("[FOCUS ERROR] TCP连接池已关闭");
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
    const UDP_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(60 * 5); // 5分钟

    // 绑定UDP监听socket
    let l = retry_notify_with_deadline(
        listen_backoff(),
        || async { Ok(UdpSocket::bind(&bind_addr).await?) },
        |e, duration| {
            warn!("{:#}. UDP 绑定错误 {:?}", e, duration);
        },
        &mut shutdown_rx,
    )
    .await
    .with_context(|| "监听失败 Udp service")?;

    info!("UDP 监听在 {}", &bind_addr);

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
                        debug!("UDP 连接建立...");
                        c
                    }
                    Ok(Err(e)) => {
                        error!("发送开始传输命令失败: {},数据通道可能损坏,请求新通道", e);
                        if let Err(e) = data_ch_req_tx.send(true).await {
                            error!("请求新数据通道失败: {},控制通道可能已关闭.", e);
                            break;
                        }
                        continue;
                    }
                    Err(_) => {
                        error!("发送开始传输命令超时,数据通道可能已损坏,请求新通道.");
                        // 请求新连接
                        if let Err(e) = data_ch_req_tx.send(true).await {
                            error!("请求新数据通道失败: {},控制通道可能已关闭.", e);
                            break;
                        }
                        continue;
                    }
                }
            }
            None => {
                error!("数据通道接收器已关闭");
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
                                    error!("写入UDP数据失败: {},数据通道可能损坏,请求新数据通道", e);
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("请求新数据通道失败: {},关闭循环", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                },
                                Err(_) => {
                                    error!("写入UDP数据超时,数据通道可能损坏,请求新数据通道");
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("请求新数据通道失败: {}", e);
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
                                warn!("UDP连接已闲置超过5分钟，重新连接...");
                                if let Err(e) = data_ch_req_tx.send(true).await {
                                    error!("请求新数据通道失败: {}", e);
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
                                        error!("发送UDP数据到客户端失败: {}", e);
                                    }
                                },
                                Ok(Err(e)) => {
                                    error!("从客户端接收UDP数据失败: {}", e);
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("请求数据通道失败: {}", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                },
                                Err(_) => {
                                    error!("从客户端接收UDP数据失败, 连接失败，重建");
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(true).await {
                                        error!("请求数据通道失败: {}", e);
                                        break 'main_loop;
                                    }
                                    break 'data_loop;
                                }
                            }
                        },
                        Ok(Err(e)) => {
                            error!("UDP连接读取错误: {}", e);
                            // 连接失败，重建
                            if let Err(e) = data_ch_req_tx.send(true).await {
                                error!("请求数据通道失败: {}", e);
                                break 'main_loop;
                            }
                            break 'data_loop;
                        },
                        Err(_) => {
                            // 读取超时，检查整体活动超时
                            if last_activity.elapsed() > UDP_ACTIVITY_TIMEOUT {
                                warn!("UDP 连接超时，重建...");
                                if let Err(e) = data_ch_req_tx.send(true).await {
                                    error!("请求数据通道失败: {}", e);
                                    break 'main_loop;
                                }
                                break 'data_loop;
                            }
                        }
                    }
                },

                // 处理关闭信号
                _ = shutdown_rx.recv() => {
                    debug!("UDP 连接池收到关闭信号,正在关闭...");
                    break 'main_loop;
                }
            }
        }
    }

    debug!("UDP pool dropped");
    Ok(())
}
