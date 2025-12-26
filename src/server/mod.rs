mod control;
mod handshake;
mod health;
mod tcp_pool;
mod udp_pool;
#[cfg(test)]
mod test_support;

use crate::config::{Config, ServerConfig, ServerServiceConfig, TransportType};
use crate::config_watcher::{ConfigChange, ServerServiceChange};
use crate::multi_map::MultiMap;
use crate::protocol;
#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;
use crate::transport::{TcpTransport, Transport};
use anyhow::{anyhow, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{error, info, info_span, warn, Instrument};

use handshake::handle_connection;

type ServiceDigest = protocol::Digest; // SHA256 of a service name
type Nonce = protocol::Digest; // Also called `session_key`

type ControlChannelMap<T> = MultiMap<ServiceDigest, Nonce, control::ControlChannelHandle<T>>;

const TCP_POOL_SIZE: usize = 8; // TCP服务的缓存连接数
const UDP_POOL_SIZE: usize = 2; // UDP服务的缓存连接数
const CHAN_SIZE: usize = 2048; // 通道的容量
const HANDSHAKE_TIMEOUT: u64 = 5; // 握手超时时间(秒)
const SHUTDOWN_GRACE_SECS: u64 = 10; // 优雅关闭最长等待时间(秒)

// The entrypoint of running a server
pub async fn run_server(
    config: Config,
    shutdown_rx: broadcast::Receiver<bool>,
    update_rx: mpsc::Receiver<ConfigChange>,
) -> Result<()> {
    let config = match config.server {
        Some(config) => config,
        None => {
            return Err(anyhow!(
                "Try to run as a server, but the configuration is missing. Please add the `[server]` block"
            ))
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

        let max_inflight_handshakes = self.config.max_inflight_handshakes;
        let handshake_semaphore = if max_inflight_handshakes == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(
                max_inflight_handshakes as usize,
            )))
        };

        let mut update_enabled = true;
        let mut handshake_tasks = JoinSet::new();

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

                            let permit = match handshake_semaphore.as_ref() {
                                Some(sem) => match sem.clone().try_acquire_owned() {
                                    Ok(permit) => Some(permit),
                                    Err(_) => {
                                        warn!(addr = %addr, "握手并发已达上限, 拒绝连接");
                                        continue;
                                    }
                                },
                                None => None,
                            };

                            let transport = self.transport.clone();
                            let services = self.services.clone();
                            let control_channels = self.control_channels.clone();
                            let server_config = self.config.clone();

                            handshake_tasks.spawn(async move {
                                let _permit = permit;
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
                res = handshake_tasks.join_next(), if !handshake_tasks.is_empty() => {
                    match res {
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            warn!("握手任务异常退出: {}", err);
                        }
                        None => {}
                    }
                },
                // Wait for the shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("开始优雅关闭...");
                    break;
                },
                e = update_rx.recv(), if update_enabled => {
                    match e {
                        Some(e) => {
                            self.handle_hot_reload(e).await;
                        }
                        None => {
                            update_enabled = false;
                            warn!("配置热更新通道已关闭, 将继续运行但不再处理热更新");
                        }
                    }
                },
            }
        }

        let control_channels = self.shutdown_control_channels().await;
        if control_channels > 0 {
            info!("已发送关闭信号到{}个控制通道", control_channels);
        }

        let graceful = self.wait_for_shutdown(&mut handshake_tasks).await;
        if !graceful {
            let remaining = self.control_channels.read().await.len();
            warn!(remaining, "优雅关闭超时, 存在未关闭的控制通道");
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
                    if let Some(handle) = wg.remove1(&hash) {
                        handle.shutdown();
                    }
                }
                ServerServiceChange::Delete(s) => {
                    let hash = protocol::digest(s.as_bytes());
                    let _ = self.services.write().await.remove(&hash);

                    let mut wg = self.control_channels.write().await;
                    if let Some(handle) = wg.remove1(&hash) {
                        handle.shutdown();
                    }
                }
            },
            ignored => warn!(
                "[预期之外的操作] 忽略 {:?} 因为运行着作为服务的服务器",
                ignored
            ),
        }
    }

    async fn shutdown_control_channels(&self) -> usize {
        let control_channels = self.control_channels.read().await;
        let total = control_channels.len();
        control_channels.for_each_value(|handle| {
            handle.shutdown();
        });
        total
    }

    async fn wait_for_shutdown(&self, handshake_tasks: &mut JoinSet<()>) -> bool {
        let result = time::timeout(Duration::from_secs(SHUTDOWN_GRACE_SECS), async {
            while let Some(res) = handshake_tasks.join_next().await {
                if let Err(err) = res {
                    warn!("握手任务异常退出: {}", err);
                }
            }

            loop {
                if self.control_channels.read().await.is_empty() {
                    break;
                }
                time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        result.is_ok()
    }
}
