use crate::constants::listen_backoff;
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::protocol::DataChannelCmd;
use crate::transport::Transport;
use anyhow::{Context, Result};
use backon::{BackoffBuilder, ExponentialBuilder};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{self, copy_bidirectional, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, timeout, Instant};
use tracing::{debug, error, info, instrument, warn, Instrument, Span};

use super::{mux::MuxPool, DataChannelRequest, CHAN_SIZE};
use tokio_util::compat::FuturesAsyncReadCompatExt;

async fn bind_tcp_listener_with_backoff(
    addr: &str,
    shutdown_rx: &mut broadcast::Receiver<bool>,
    backoff: ExponentialBuilder,
) -> Result<TcpListener> {
    retry_notify_with_deadline(
        backoff,
        || async { Ok::<TcpListener, std::io::Error>(TcpListener::bind(addr).await?) },
        |e: &std::io::Error, duration| {
            error!("{:#}. 重试间隔: {:?}", e, duration);
        },
        shutdown_rx,
    )
    .await
    .with_context(|| "监听服务失败")
}

fn spawn_tcp_accept_loop(
    listener: TcpListener,
    data_ch_req_tx: mpsc::Sender<DataChannelRequest>,
    use_mux: bool,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> mpsc::Receiver<TcpStream> {
    let (tx, rx) = mpsc::channel(CHAN_SIZE);

    tokio::spawn(async move {
        // 重试至少每1秒
        let backoff_builder = ExponentialBuilder::default()
            .with_factor(1.5)
            .with_min_delay(Duration::from_millis(500))
            .with_max_delay(Duration::from_secs(1))
            .without_max_times()
            .with_jitter();
        let mut backoff = backoff_builder.build();

        // 主循环
        loop {
            tokio::select! {
                val = listener.accept() => {
                    match val {
                        Err(e) => {
                            // `listener` 是 TCP 监听器, 所以这必须是 IO 错误
                            // 可能是 EMFILE. 所以等待一段时间
                            error!("{}. 等待一段时间", e);
                            if let Some(d) = backoff.next() {
                                time::sleep(d).await;
                            }
                        }
                        Ok((incoming, addr)) => {
                            if !use_mux {
                                // 对于每个访问者, 请求创建一个数据通道
                                // 使用 try_send 避免阻塞 accept
                                match data_ch_req_tx.try_send(DataChannelRequest::Plain) {
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
                            }
                            // 将访问者发送到连接池
                            match tx.try_send(incoming) {
                                Ok(()) => {
                                    backoff = backoff_builder.build(); // 重置重试计数器
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

async fn copy_with_activity<R, W>(
    mut reader: R,
    mut writer: W,
    last_activity: Arc<AtomicU64>,
    start: Instant,
) -> Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0u8; 16 * 1024];
    let mut total = 0u64;

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            let _ = writer.shutdown().await;
            return Ok(total);
        }
        writer.write_all(&buf[..n]).await?;
        total += n as u64;
        last_activity.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
    }
}

async fn copy_bidirectional_with_idle<A, B>(
    left: A,
    right: B,
    idle_timeout: Option<Duration>,
) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    B: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Some(idle_timeout) = idle_timeout else {
        let mut left = left;
        let mut right = right;
        let _ = copy_bidirectional(&mut left, &mut right).await;
        return Ok(());
    };

    let start = Instant::now();
    let last_activity = Arc::new(AtomicU64::new(start.elapsed().as_millis() as u64));
    let idle_timeout_ms = idle_timeout.as_millis() as u64;
    let mut idle_ticker = time::interval(Duration::from_secs(1));

    let (left_rd, left_wr) = io::split(left);
    let (right_rd, right_wr) = io::split(right);

    let mut left_to_right =
        tokio::spawn(copy_with_activity(left_rd, right_wr, last_activity.clone(), start));
    let mut right_to_left =
        tokio::spawn(copy_with_activity(right_rd, left_wr, last_activity.clone(), start));

    let mut left_done = false;
    let mut right_done = false;

    loop {
        tokio::select! {
            res = &mut left_to_right, if !left_done => {
                left_done = true;
                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        debug!("数据通道转发失败: {}", err);
                        if !right_done {
                            right_to_left.abort();
                            right_done = true;
                        }
                    }
                    Err(err) => {
                        debug!("数据通道转发任务异常: {}", err);
                        if !right_done {
                            right_to_left.abort();
                            right_done = true;
                        }
                    }
                }
            }
            res = &mut right_to_left, if !right_done => {
                right_done = true;
                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        debug!("数据通道转发失败: {}", err);
                        if !left_done {
                            left_to_right.abort();
                            left_done = true;
                        }
                    }
                    Err(err) => {
                        debug!("数据通道转发任务异常: {}", err);
                        if !left_done {
                            left_to_right.abort();
                            left_done = true;
                        }
                    }
                }
            }
            _ = idle_ticker.tick() => {
                let now_ms = start.elapsed().as_millis() as u64;
                let last_ms = last_activity.load(Ordering::Relaxed);
                if now_ms.saturating_sub(last_ms) >= idle_timeout_ms {
                    debug!("数据通道空闲超时({:?})，主动关闭", idle_timeout);
                    if !left_done {
                        left_to_right.abort();
                    }
                    if !right_done {
                        right_to_left.abort();
                    }
                    break;
                }
            }
        }

        if left_done && right_done {
            break;
        }
    }

    let _ = left_to_right.await;
    let _ = right_to_left.await;

    Ok(())
}

async fn run_tcp_connection_pool_with_backoff<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::Sender<DataChannelRequest>,
    mux_pool: Option<Arc<MuxPool>>,
    stream_idle_timeout: u64,
    data_channel_wait_timeout: u64,
    shutdown_rx: broadcast::Receiver<bool>,
    backoff: ExponentialBuilder,
) -> Result<()> {
    let mut shutdown_rx = shutdown_rx;
    let listener = bind_tcp_listener_with_backoff(&bind_addr, &mut shutdown_rx, backoff).await?;
    info!("开始监听: {}", &bind_addr);

    let use_mux = mux_pool.is_some();
    let mut visitor_rx =
        spawn_tcp_accept_loop(listener, data_ch_req_tx.clone(), use_mux, shutdown_rx);
    let cmd = bincode::serialize(&DataChannelCmd::StartForwardTcp)?;
    let data_ch_wait_timeout = if data_channel_wait_timeout == 0 {
        None
    } else {
        Some(Duration::from_secs(data_channel_wait_timeout))
    };
    let stream_idle_timeout = if stream_idle_timeout == 0 {
        None
    } else {
        Some(Duration::from_secs(stream_idle_timeout))
    };

    'pool: while let Some(mut visitor) = visitor_rx.recv().await {
        if let Some(pool) = mux_pool.clone() {
            let stream = match data_ch_wait_timeout {
                Some(timeout_duration) => match timeout(timeout_duration, pool.open_stream(None)).await {
                    Ok(v) => v,
                    Err(_) => {
                        warn!(
                            bind_addr = %bind_addr,
                            timeout_secs = data_channel_wait_timeout,
                            "等待 mux stream 超时, 关闭访客连接"
                        );
                        continue;
                    }
                },
                None => pool.open_stream(None).await,
            };
            let mut stream = match stream {
                Ok(v) => v.compat(),
                Err(e) => {
                    warn!("打开 mux stream 失败: {:#}", e);
                    continue;
                }
            };

            if write_and_flush(&mut stream, &cmd).await.is_ok() {
                tokio::spawn(async move {
                    let _ = copy_bidirectional_with_idle(stream, visitor, stream_idle_timeout).await;
                });
            } else {
                warn!("写入 StartForwardTcp 失败, 关闭访客连接");
            }
            continue;
        }

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
                    if let Err(e) = data_ch_req_tx.send(DataChannelRequest::Plain).await {
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
    data_ch_req_tx: mpsc::Sender<DataChannelRequest>,
    mux_pool: Option<Arc<MuxPool>>,
    stream_idle_timeout: u64,
    data_channel_wait_timeout: u64,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    run_tcp_connection_pool_with_backoff::<T>(
        bind_addr,
        data_ch_rx,
        data_ch_req_tx,
        mux_pool,
        stream_idle_timeout,
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
    use backon::ExponentialBuilder;
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
            None,
            0,
            1,
            shutdown_rx,
        ));

        let mut client = connect_with_retry(&bind_addr, Duration::from_secs(1)).await?;
        let req = timeout(Duration::from_secs(1), data_ch_req_rx.recv()).await?;
        assert_eq!(req, Some(crate::server::DataChannelRequest::Plain));

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

        let backoff = ExponentialBuilder::default()
            .with_factor(1.5)
            .with_min_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(10))
            .with_total_delay(Some(Duration::from_millis(50)))
            .without_max_times()
            .with_jitter();

        let result = run_tcp_connection_pool_with_backoff::<TestTransport>(
            bind_addr,
            data_ch_rx,
            data_ch_req_tx,
            None,
            0,
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
