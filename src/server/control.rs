use crate::config::{ServerServiceConfig, ServiceType};
use crate::helper::write_and_flush;
use crate::protocol::{Ack, ControlChannelCmd, PROTO_V3};
use crate::transport::{SocketOpts, Transport};
use anyhow::{anyhow, Context, Result};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tracing::{debug, error, info, info_span, instrument, Instrument, Span};

use super::{mux::MuxPool, DataChannelRequest, CHAN_SIZE, TCP_POOL_SIZE, UDP_POOL_SIZE};
use super::tcp_pool::run_tcp_connection_pool;
use super::udp_pool::run_udp_connection_pool;

const CONTROL_CHANNEL_WRITE_TIMEOUT: u64 = 5; // 控制通道写入超时(秒)

pub(super) struct ControlChannelHandle<T: Transport> {
    // Shutdown the control channel by dropping it
    _shutdown_tx: broadcast::Sender<bool>,
    data_ch_tx: mpsc::Sender<T::Stream>,
    service: ServerServiceConfig,
    // 添加时间戳字段，记录连接建立时间
    timestamp: u64,
    addr: SocketAddr,
    mux_pool: Option<Arc<MuxPool>>,
}

impl<T> ControlChannelHandle<T>
where
    T: 'static + Transport,
{
    // Renamed `new` to `prepare`.
    // Returns the handle instance and a Future that runs the control channel logic.
    #[instrument(name = "handle_prepare", skip_all, fields(service = %service.name))]
    pub(super) fn prepare(
        conn: T::Stream,
        service: ServerServiceConfig,
        heartbeat_interval: u64,
        data_channel_wait_timeout: u64,
        protocol_version: u8,
        timestamp: u64,
        addr: SocketAddr,
    ) -> (Self, impl Future<Output = Result<()>> + Send + 'static) {
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1); // 关闭channel
        let (data_ch_tx, data_ch_rx) = mpsc::channel(CHAN_SIZE * 2); // 数据channel队列
        let data_channel_request_buffer = CHAN_SIZE / 2;
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::channel(data_channel_request_buffer); // 缓冲区

        // 获得 TCP 或 UDP 的服务池大小
        let mux_enabled = service.service_type == ServiceType::Tcp
            && service.enable_mux
            && protocol_version == PROTO_V3;
        let pool_size = match service.service_type {
            ServiceType::Tcp => {
                if mux_enabled {
                    service.mux_pool_size
                } else {
                    TCP_POOL_SIZE
                }
            }
            ServiceType::Udp => UDP_POOL_SIZE,
        };

        let mux_pool = if mux_enabled {
            let pool = MuxPool::new(
                service.mux_select,
                service.mux_pool_size,
                service.mux_max_streams,
                service.mux_idle_timeout,
                data_ch_req_tx.clone(),
            );
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                pool_clone.ensure_target().await;
            });
            Some(pool)
        } else {
            None
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
                let mux_pool_for_tcp = mux_pool.clone();
                tokio::spawn(
                    async move {
                        // 运行TCP连接池任务
                        if let Err(e) = run_tcp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,
                            data_ch_req_tx_clone,
                            mux_pool_for_tcp,
                            service.stream_idle_timeout,
                            data_channel_wait_timeout,
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
                        debug!(service = %service_name_clone, "UDP 连接池任务结束.");
                    }
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
            mux_enabled,
            protocol_version,
        };

        // 创建控制通道句柄实例（返回给调用者）
        let handle = ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            data_ch_tx,
            service,
            timestamp,
            addr,
            mux_pool,
        };

        // 创建控制通道 Future，将执行控制通道逻辑
        let control_task_future = async move { ch.run().await }.instrument(Span::current());

        // 创建控制通道句柄实例（返回给调用者）
        (handle, control_task_future)
    }

    pub(super) fn shutdown(&self) {
        // Broadcast shutdown; ignore error if there are no active subscribers
        let _ = self._shutdown_tx.send(true);
    }

    pub(super) fn subscribe_shutdown(&self) -> broadcast::Receiver<bool> {
        self._shutdown_tx.subscribe()
    }

    pub(super) fn data_channel_sender(&self) -> mpsc::Sender<T::Stream> {
        self.data_ch_tx.clone()
    }

    pub(super) fn socket_opts(&self) -> SocketOpts {
        SocketOpts::from_server_cfg(&self.service)
    }

    pub(super) fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub(super) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(super) fn mux_pool(&self) -> Option<Arc<MuxPool>> {
        self.mux_pool.clone()
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        shutdown_tx: broadcast::Sender<bool>,
        data_ch_tx: mpsc::Sender<T::Stream>,
        service: ServerServiceConfig,
        timestamp: u64,
        addr: SocketAddr,
    ) -> Self {
        ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            data_ch_tx,
            service,
            timestamp,
            addr,
            mux_pool: None,
        }
    }
}

// Control channel, using T as the transport layer.
struct ControlChannel<T: Transport> {
    conn: T::Stream, // The connection of control channel // 控制通道连接
    shutdown_rx: broadcast::Receiver<bool>, // Receives the shutdown signal // 接收关闭信号
    data_ch_req_rx: mpsc::Receiver<DataChannelRequest>, // Receives visitor connections (Bounded Receiver) // 接收访客连接请求（有界接收器）
    heartbeat_interval: u64, // Application-layer heartbeat interval in secs // 应用层心跳间隔（秒）
    pool_size: usize,        // Initial pool size to request // 初始池大小请求
    data_ch_req_tx: mpsc::Sender<DataChannelRequest>, // Sender to request data channels (Bounded Sender) // 发送器请求数据通道（有界发送器）
    mux_enabled: bool,
    protocol_version: u8,
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
        let initial_req = if self.mux_enabled {
            DataChannelRequest::Mux
        } else {
            DataChannelRequest::Plain
        };
        for i in 0..self.pool_size {
            if let Err(e) = self.data_ch_req_tx.send(initial_req).await {
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
        let create_mux_cmd = bincode::serialize(&ControlChannelCmd::CreateDataMux)?;
        let heartbeat = bincode::serialize(&ControlChannelCmd::HeartBeat)?;

        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(req) => {
                            let cmd = match req {
                                DataChannelRequest::Mux if self.mux_enabled && self.protocol_version == PROTO_V3 => &create_mux_cmd,
                                _ => &create_ch_cmd,
                            };
                            let write_future = self.write_and_flush(cmd);
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
