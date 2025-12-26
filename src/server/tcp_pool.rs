use crate::constants::listen_backoff;
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::protocol::DataChannelCmd;
use crate::transport::Transport;
use anyhow::{Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use std::time::Duration;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, timeout};
use tracing::{debug, error, info, instrument, warn, Instrument, Span};

use super::CHAN_SIZE;

async fn bind_tcp_listener_with_backoff(
    addr: &str,
    shutdown_rx: &mut broadcast::Receiver<bool>,
    backoff: ExponentialBackoff,
) -> Result<TcpListener> {
    retry_notify_with_deadline(
        backoff,
        || async { Ok(TcpListener::bind(addr).await?) },
        |e, duration| {
            error!("{:#}. 重试间隔: {:?}", e, duration);
        },
        shutdown_rx,
    )
    .await
    .with_context(|| "监听服务失败")
}

fn spawn_tcp_accept_loop(
    listener: TcpListener,
    data_ch_req_tx: mpsc::Sender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> mpsc::Receiver<TcpStream> {
    let (tx, rx) = mpsc::channel(CHAN_SIZE);

    tokio::spawn(async move {
        // 重试至少每1秒
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_secs(1),
            max_elapsed_time: None,
            ..Default::default()
        };

        // 主循环
        loop {
            tokio::select! {
                val = listener.accept() => {
                    match val {
                        Err(e) => {
                            // `listener` 是 TCP 监听器, 所以这必须是 IO 错误
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
                            // 使用 try_send 避免阻塞 accept
                            match data_ch_req_tx.try_send(true) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!("数据通道请求队列已满, 拒绝新连接: {}", addr);
                                    continue;
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    error!("发送数据通道请求失败: 请求通道已关闭, 监听器退出.");
                                    break; // 控制通道已关闭, 退出循环
                                }
                            }
                            // 将访问者发送到连接池
                            match tx.try_send(incoming) {
                                Ok(()) => {
                                    backoff.reset(); // 重置重试计数器
                                    debug!("新的客户端连接: {}", addr);
                                }
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!("访客队列已满, 拒绝新连接: {}", addr);
                                    continue;
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    error!("访客队列已关闭, 监听器退出.");
                                    break;
                                }
                            }
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

async fn run_tcp_connection_pool_with_backoff<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::Sender<bool>,
    data_channel_wait_timeout: u64,
    shutdown_rx: broadcast::Receiver<bool>,
    backoff: ExponentialBackoff,
) -> Result<()> {
    let mut shutdown_rx = shutdown_rx;
    let listener = bind_tcp_listener_with_backoff(&bind_addr, &mut shutdown_rx, backoff).await?;
    info!("开始监听: {}", &bind_addr);

    let mut visitor_rx =
        spawn_tcp_accept_loop(listener, data_ch_req_tx.clone(), shutdown_rx);
    let cmd = bincode::serialize(&DataChannelCmd::StartForwardTcp)?;
    let data_ch_wait_timeout = if data_channel_wait_timeout == 0 {
        None
    } else {
        Some(Duration::from_secs(data_channel_wait_timeout))
    };

    'pool: while let Some(mut visitor) = visitor_rx.recv().await {
        loop {
            let ch = match data_ch_wait_timeout {
                Some(timeout_duration) => match timeout(timeout_duration, data_ch_rx.recv()).await {
                    Ok(v) => v,
                    Err(_) => {
                        warn!(
                            bind_addr = %bind_addr,
                            timeout_secs = data_channel_wait_timeout,
                            "等待数据通道超时, 关闭访客连接"
                        );
                        break;
                    }
                },
                None => data_ch_rx.recv().await,
            };
            if let Some(mut ch) = ch {
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
pub(super) async fn run_tcp_connection_pool<T: Transport>(
    bind_addr: String,
    data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::Sender<bool>,
    data_channel_wait_timeout: u64,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    run_tcp_connection_pool_with_backoff::<T>(
        bind_addr,
        data_ch_rx,
        data_ch_req_tx,
        data_channel_wait_timeout,
        shutdown_rx,
        listen_backoff(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{run_tcp_connection_pool, run_tcp_connection_pool_with_backoff};
    use crate::server::test_support::{connect_with_retry, pick_unused_port, TestTransport};
    use anyhow::Result;
    use backoff::ExponentialBackoff;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, mpsc};
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_tcp_data_channel_wait_timeout_closes_visitor() -> Result<()> {
        let (data_ch_tx, data_ch_rx) = mpsc::channel(1);
        let (data_ch_req_tx, mut data_ch_req_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let port = pick_unused_port()?;
        let bind_addr = format!("127.0.0.1:{}", port);

        let pool_task = tokio::spawn(run_tcp_connection_pool::<TestTransport>(
            bind_addr.clone(),
            data_ch_rx,
            data_ch_req_tx,
            1,
            shutdown_rx,
        ));

        let mut client = connect_with_retry(&bind_addr, Duration::from_secs(1)).await?;
        let req = timeout(Duration::from_secs(1), data_ch_req_rx.recv()).await?;
        assert_eq!(req, Some(true));

        let mut buf = [0u8; 1];
        let read_result = timeout(Duration::from_secs(2), client.read(&mut buf)).await;
        assert!(read_result.is_ok());
        let n = read_result.unwrap()?;
        assert_eq!(n, 0);

        let _ = shutdown_tx.send(true);
        drop(data_ch_tx);

        let result = timeout(Duration::from_secs(2), pool_task).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_tcp_listen_failure_propagates() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let bind_addr = format!("{}:{}", addr.ip(), addr.port());

        let (_data_ch_tx, data_ch_rx) = mpsc::channel(1);
        let (data_ch_req_tx, _data_ch_req_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_millis(50)),
            max_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let result = run_tcp_connection_pool_with_backoff::<TestTransport>(
            bind_addr,
            data_ch_rx,
            data_ch_req_tx,
            1,
            shutdown_rx,
            backoff,
        )
        .await;

        assert!(result.is_err());
        drop(listener);
        Ok(())
    }
}
