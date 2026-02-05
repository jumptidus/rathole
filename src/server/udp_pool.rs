use crate::constants::UDP_BUFFER_SIZE;
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::protocol::{DataChannelCmd, UdpTraffic};
use crate::transport::Transport;
use anyhow::{Context, Result};
use backon::{BackoffBuilder, ExponentialBuilder};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{timeout, Instant};
use tracing::{debug, error, info, instrument, warn};

#[cfg(not(test))]
const UDP_READ_TIMEOUT: Duration = Duration::from_secs(10); // UDP Read Timeout
#[cfg(test)]
const UDP_READ_TIMEOUT: Duration = Duration::from_millis(100); // 测试缩短
#[cfg(not(test))]
const UDP_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(60 * 5); // 5分钟
#[cfg(test)]
const UDP_ACTIVITY_TIMEOUT: Duration = Duration::from_millis(300); // 测试缩短

#[instrument(skip_all)]
pub(super) async fn run_udp_connection_pool<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::Sender<super::DataChannelRequest>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    const UDP_WRITE_TIMEOUT: Duration = Duration::from_secs(10); // UDP Write Timeout

    // 绑定UDP监听socket
    let l = retry_notify_with_deadline(
        crate::constants::listen_backoff(),
        || async { Ok::<UdpSocket, std::io::Error>(UdpSocket::bind(&bind_addr).await?) },
        |e: &std::io::Error, duration| {
            warn!("{:#}. UDP 绑定错误 {:?}", e, duration);
        },
        &mut shutdown_rx,
    )
    .await
    .with_context(|| "监听失败 Udp service")?;

    info!("UDP 监听在 {}", &bind_addr);

    let cmd = bincode::serialize(&DataChannelCmd::StartForwardUdp)?;
    let mut buf = [0u8; UDP_BUFFER_SIZE];

    let backoff_builder = ExponentialBuilder::default()
        .with_factor(1.5)
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(5))
        .without_max_times()
        .with_jitter();
    let mut request_backoff = backoff_builder.build();
    let request_sleep = tokio::time::sleep(Duration::from_millis(0));
    tokio::pin!(request_sleep);
    let mut request_sleep_armed = false;

    // 主循环：管理连接和数据转发
    'main_loop: loop {
        // 获取或重建数据通道连接
        let mut conn = loop {
            tokio::select! {
                val = data_ch_rx.recv() => {
                    match val {
                        Some(mut c) => {
                            match timeout(UDP_WRITE_TIMEOUT, write_and_flush(&mut c, &cmd)).await {
                                Ok(Ok(_)) => {
                                    debug!("UDP 连接建立...");
                                    request_backoff = backoff_builder.build();
                                    break c;
                                }
                                Ok(Err(e)) => {
                                    error!("发送开始传输命令失败: {},数据通道可能损坏,请求新通道", e);
                                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                        error!("请求新数据通道失败: {},控制通道可能已关闭.", e);
                                        break 'main_loop;
                                    }
                                    if let Some(delay) = request_backoff.next() {
                                        request_sleep.as_mut().reset(Instant::now() + delay);
                                        request_sleep_armed = true;
                                    } else {
                                        request_sleep_armed = false;
                                    }
                                }
                                Err(_) => {
                                    error!("发送开始传输命令超时,数据通道可能已损坏,请求新通道.");
                                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                        error!("请求新数据通道失败: {},控制通道可能已关闭.", e);
                                        break 'main_loop;
                                    }
                                    if let Some(delay) = request_backoff.next() {
                                        request_sleep.as_mut().reset(Instant::now() + delay);
                                        request_sleep_armed = true;
                                    } else {
                                        request_sleep_armed = false;
                                    }
                                }
                            }
                        }
                        None => {
                            error!("数据通道接收器已关闭");
                            break 'main_loop;
                        }
                    }
                }
                _ = &mut request_sleep, if request_sleep_armed => {
                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                        error!("请求新数据通道失败: {},控制通道可能已关闭.", e);
                        break 'main_loop;
                    }
                    if let Some(delay) = request_backoff.next() {
                        request_sleep.as_mut().reset(Instant::now() + delay);
                        request_sleep_armed = true;
                    } else {
                        request_sleep_armed = false;
                    }
                }
                _ = shutdown_rx.recv() => {
                    debug!("UDP 连接池收到关闭信号,正在关闭...");
                    break 'main_loop;
                }
            }
        };

        let mut last_activity = Instant::now();

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
                                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                        error!("请求新数据通道失败: {},关闭循环", e);
                                        break 'main_loop;
                                    }
                                    if let Some(delay) = request_backoff.next() {
                                        request_sleep.as_mut().reset(Instant::now() + delay);
                                        request_sleep_armed = true;
                                    } else {
                                        request_sleep_armed = false;
                                    }
                                    break 'data_loop;
                                },
                                Err(_) => {
                                    error!("写入UDP数据超时,数据通道可能损坏,请求新数据通道");
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                        error!("请求新数据通道失败: {}", e);
                                        break 'main_loop;
                                    }
                                    if let Some(delay) = request_backoff.next() {
                                        request_sleep.as_mut().reset(Instant::now() + delay);
                                        request_sleep_armed = true;
                                    } else {
                                        request_sleep_armed = false;
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
                                if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                    error!("请求新数据通道失败: {}", e);
                                    break 'main_loop;
                                }
                                if let Some(delay) = request_backoff.next() {
                                    request_sleep.as_mut().reset(Instant::now() + delay);
                                    request_sleep_armed = true;
                                } else {
                                    request_sleep_armed = false;
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
                                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                        error!("请求数据通道失败: {}", e);
                                        break 'main_loop;
                                    }
                                    if let Some(delay) = request_backoff.next() {
                                        request_sleep.as_mut().reset(Instant::now() + delay);
                                        request_sleep_armed = true;
                                    } else {
                                        request_sleep_armed = false;
                                    }
                                    break 'data_loop;
                                },
                                Err(_) => {
                                    error!("从客户端接收UDP数据失败, 连接失败，重建");
                                    // 连接失败，重建
                                    if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                        error!("请求数据通道失败: {}", e);
                                        break 'main_loop;
                                    }
                                    if let Some(delay) = request_backoff.next() {
                                        request_sleep.as_mut().reset(Instant::now() + delay);
                                        request_sleep_armed = true;
                                    } else {
                                        request_sleep_armed = false;
                                    }
                                    break 'data_loop;
                                }
                            }
                        },
                        Ok(Err(e)) => {
                            error!("UDP连接读取错误: {}", e);
                            // 连接失败，重建
                            if let Err(e) = data_ch_req_tx.send(super::DataChannelRequest::Plain).await {
                                error!("请求数据通道失败: {}", e);
                                break 'main_loop;
                            }
                            if let Some(delay) = request_backoff.next() {
                                request_sleep.as_mut().reset(Instant::now() + delay);
                                request_sleep_armed = true;
                            } else {
                                request_sleep_armed = false;
                            }
                            break 'data_loop;
                        },
                        Err(_) => {
                            // 读取超时，检查整体活动超时
                            if last_activity.elapsed() > UDP_ACTIVITY_TIMEOUT {
                                warn!("UDP 连接超时，重建...");
                                if let Err(e) = data_ch_req_tx
                                    .send(super::DataChannelRequest::Plain)
                                    .await
                                {
                                    error!("请求数据通道失败: {}", e);
                                    break 'main_loop;
                                }
                                if let Some(delay) = request_backoff.next() {
                                    request_sleep.as_mut().reset(Instant::now() + delay);
                                    request_sleep_armed = true;
                                } else {
                                    request_sleep_armed = false;
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

#[cfg(test)]
mod tests {
    use super::{run_udp_connection_pool, UDP_ACTIVITY_TIMEOUT, UDP_READ_TIMEOUT};
    use crate::server::test_support::TestTransport;
    use anyhow::Result;
    use std::time::Duration;
    use tokio::io::duplex;
    use tokio::sync::{broadcast, mpsc};
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_udp_pool_does_not_request_on_startup() -> Result<()> {
        let (data_ch_tx, data_ch_rx) = mpsc::channel(4);
        let (data_ch_req_tx, mut data_ch_req_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let pool_task = tokio::spawn(run_udp_connection_pool::<TestTransport>(
            "127.0.0.1:0".to_string(),
            data_ch_rx,
            data_ch_req_tx,
            shutdown_rx,
        ));

        let premature = timeout(Duration::from_millis(300), data_ch_req_rx.recv()).await;
        assert!(premature.is_err());

        let _ = shutdown_tx.send(true);
        drop(data_ch_tx);

        let result = timeout(Duration::from_secs(1), pool_task).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_udp_idle_reconnect_resets_last_activity() -> Result<()> {
        let (data_ch_tx, data_ch_rx) = mpsc::channel(4);
        let (data_ch_req_tx, mut data_ch_req_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let pool_task = tokio::spawn(run_udp_connection_pool::<TestTransport>(
            "127.0.0.1:0".to_string(),
            data_ch_rx,
            data_ch_req_tx,
            shutdown_rx,
        ));

        let (conn, peer) = duplex(1024);
        let _peer_guard = peer;
        data_ch_tx.send(conn).await.unwrap();

        let first_timeout = UDP_ACTIVITY_TIMEOUT + UDP_READ_TIMEOUT + UDP_READ_TIMEOUT;
        let first_req = timeout(first_timeout, data_ch_req_rx.recv()).await?;
        assert_eq!(first_req, Some(crate::server::DataChannelRequest::Plain));

        let (conn2, peer2) = duplex(1024);
        let _peer_guard2 = peer2;
        data_ch_tx.send(conn2).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let premature = timeout(UDP_READ_TIMEOUT + UDP_READ_TIMEOUT, data_ch_req_rx.recv()).await;
        assert!(premature.is_err());

        let _ = shutdown_tx.send(true);
        drop(data_ch_tx);

        let result = timeout(Duration::from_secs(1), pool_task).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
        Ok(())
    }
}
