use crate::config::MuxSelect;
use crate::server::DataChannelRequest;
use anyhow::{anyhow, Result};
use futures::future::poll_fn;
use futures::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, Notify, RwLock};
use tokio::time::timeout;
use tracing::{error, warn};
use yamux::{Connection, ConnectionError, Stream};

static MUX_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct MuxStream {
    inner: Stream,
    in_use: Arc<AtomicUsize>,
    last_active: Arc<AtomicU64>,
    start: Instant,
    notify: Arc<Notify>,
}

impl MuxStream {
    pub fn new(
        inner: Stream,
        in_use: Arc<AtomicUsize>,
        last_active: Arc<AtomicU64>,
        start: Instant,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            inner,
            in_use,
            last_active,
            start,
            notify,
        }
    }
}

impl Drop for MuxStream {
    fn drop(&mut self) {
        self.in_use.fetch_sub(1, Ordering::Release);
        let now_ms = self.start.elapsed().as_millis() as u64;
        self.last_active.store(now_ms, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl AsyncRead for MuxStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_read(cx, buf)
    }
}

impl AsyncWrite for MuxStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_close(cx)
    }
}

#[derive(Debug)]
pub(super) struct OpenStreamRequest {
    resp: oneshot::Sender<Result<Stream, ConnectionError>>,
}

struct StreamPermit {
    in_use: Arc<AtomicUsize>,
    last_active: Arc<AtomicU64>,
    start: Instant,
    released: bool,
    notify: Arc<Notify>,
}

impl StreamPermit {
    fn new(session: &MuxSession) -> Option<Self> {
        session.touch();
        let inflight = session.in_use.fetch_add(1, Ordering::AcqRel);
        if inflight >= session.max_streams {
            session.in_use.fetch_sub(1, Ordering::Release);
            return None;
        }

        Some(Self {
            in_use: Arc::clone(&session.in_use),
            last_active: Arc::clone(&session.last_active),
            start: session.start,
            released: false,
            notify: Arc::clone(&session.notify),
        })
    }

    fn into_stream(mut self, stream: Stream) -> MuxStream {
        self.released = true;
        MuxStream::new(
            stream,
            Arc::clone(&self.in_use),
            Arc::clone(&self.last_active),
            self.start,
            Arc::clone(&self.notify),
        )
    }
}

impl Drop for StreamPermit {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.in_use.fetch_sub(1, Ordering::Release);
        let now_ms = self.start.elapsed().as_millis() as u64;
        self.last_active.store(now_ms, Ordering::Release);
        self.notify.notify_waiters();
    }
}

#[derive(Debug)]
pub(super) struct MuxSession {
    id: u64,
    open_tx: mpsc::Sender<OpenStreamRequest>,
    in_use: Arc<AtomicUsize>,
    last_active: Arc<AtomicU64>,
    start: Instant,
    max_streams: usize,
    notify: Arc<Notify>,
}

impl MuxSession {
    fn new(
        open_tx: mpsc::Sender<OpenStreamRequest>,
        max_streams: usize,
        notify: Arc<Notify>,
    ) -> Arc<Self> {
        let start = Instant::now();
        Arc::new(Self {
            id: MUX_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            open_tx,
            in_use: Arc::new(AtomicUsize::new(0)),
            last_active: Arc::new(AtomicU64::new(0)),
            start,
            max_streams,
            notify,
        })
    }

    fn can_open(&self) -> bool {
        self.in_use.load(Ordering::Acquire) < self.max_streams
    }

    fn inflight(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }

    fn touch(&self) {
        let now_ms = self.start.elapsed().as_millis() as u64;
        self.last_active.store(now_ms, Ordering::Release);
    }

    fn idle_for(&self, timeout: Duration) -> bool {
        if timeout.is_zero() {
            return false;
        }
        if self.in_use.load(Ordering::Acquire) != 0 {
            return false;
        }
        let now_ms = self.start.elapsed().as_millis() as u64;
        let last_ms = self.last_active.load(Ordering::Acquire);
        now_ms.saturating_sub(last_ms) >= timeout.as_millis() as u64
    }
}

#[derive(Debug)]
pub struct MuxPool {
    sessions: RwLock<Vec<Arc<MuxSession>>>,
    select: MuxSelect,
    target: usize,
    max_streams: usize,
    idle_timeout: Option<Duration>,
    request_tx: mpsc::Sender<DataChannelRequest>,
    notify: Arc<Notify>,
    rr: AtomicUsize,
}

impl MuxPool {
    pub fn new(
        select: MuxSelect,
        target: usize,
        max_streams: usize,
        idle_timeout: u64,
        request_tx: mpsc::Sender<DataChannelRequest>,
    ) -> Arc<Self> {
        let idle_timeout = if idle_timeout == 0 {
            None
        } else {
            Some(Duration::from_secs(idle_timeout))
        };
        let notify = Arc::new(Notify::new());
        Arc::new(Self {
            sessions: RwLock::new(Vec::new()),
            select,
            target,
            max_streams,
            idle_timeout,
            request_tx,
            notify,
            rr: AtomicUsize::new(0),
        })
    }

    pub async fn ensure_target(&self) {
        let current = self.sessions.read().await.len();
        if current >= self.target {
            return;
        }
        let missing = self.target - current;
        for _ in 0..missing {
            let _ = self.request_tx.send(DataChannelRequest::Mux).await;
        }
    }

    pub async fn add_session(&self, open_tx: mpsc::Sender<OpenStreamRequest>) -> Arc<MuxSession> {
        let session = MuxSession::new(open_tx, self.max_streams, Arc::clone(&self.notify));
        self.sessions.write().await.push(session.clone());
        self.notify.notify_waiters();
        session
    }

    pub fn max_streams(&self) -> usize {
        self.max_streams
    }

    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    pub async fn remove_session(&self, id: u64) {
        let mut sessions = self.sessions.write().await;
        let before = sessions.len();
        sessions.retain(|s| s.id != id);
        if before != sessions.len() {
            self.notify.notify_waiters();
        }
    }

    pub async fn open_stream(
        &self,
        wait_timeout: Option<Duration>,
    ) -> Result<MuxStream> {
        let open = async {
            loop {
                if let Some(session) = self.pick_session().await {
                    let Some(permit) = StreamPermit::new(&session) else {
                        continue;
                    };

                    let (resp_tx, resp_rx) = oneshot::channel();
                    if session
                        .open_tx
                        .send(OpenStreamRequest { resp: resp_tx })
                        .await
                        .is_err()
                    {
                        return Err(anyhow!("mux session channel closed"));
                    }

                    match resp_rx.await {
                        Ok(Ok(stream)) => {
                            return Ok(permit.into_stream(stream));
                        }
                        Ok(Err(e)) => {
                            return Err(anyhow!("open mux stream failed: {}", e));
                        }
                        Err(_) => {
                            return Err(anyhow!("mux session dropped"));
                        }
                    }
                }

                self.ensure_target().await;
                self.notify.notified().await;
            }
        };

        match wait_timeout {
            Some(dur) => timeout(dur, open).await?,
            None => open.await,
        }
    }

    async fn pick_session(&self) -> Option<Arc<MuxSession>> {
        let sessions = self.sessions.read().await;
        if sessions.is_empty() {
            return None;
        }

        match self.select {
            MuxSelect::LeastStreams => sessions
                .iter()
                .filter(|s| s.can_open())
                .min_by_key(|s| s.inflight())
                .cloned(),
            MuxSelect::RoundRobin => {
                let mut idx = self.rr.fetch_add(1, Ordering::Relaxed);
                for _ in 0..sessions.len() {
                    let s = &sessions[idx % sessions.len()];
                    idx += 1;
                    if s.can_open() {
                        return Some(Arc::clone(s));
                    }
                }
                None
            }
        }
    }
}

pub async fn run_mux_server<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    conn: Connection<T>,
    mut open_rx: mpsc::Receiver<OpenStreamRequest>,
    pool: Arc<MuxPool>,
    session: Arc<MuxSession>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) {
    let mut conn = conn;
    let idle_timeout = pool.idle_timeout();
    let mut idle_tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        enum Event {
            Open(OpenStreamRequest),
            Inbound(Option<Result<Stream, ConnectionError>>),
            Closed,
            Shutdown,
            IdleCheck,
        }

        let event = tokio::select! {
            evt = poll_fn(|cx| {
                if let Poll::Ready(req) = open_rx.poll_recv(cx) {
                    return match req {
                        Some(req) => Poll::Ready(Event::Open(req)),
                        None => Poll::Ready(Event::Closed),
                    };
                }

                match conn.poll_next_inbound(cx) {
                    Poll::Ready(v) => Poll::Ready(Event::Inbound(v)),
                    Poll::Pending => Poll::Pending,
                }
            }) => evt,
            _ = shutdown_rx.recv() => Event::Shutdown,
            _ = idle_tick.tick(), if idle_timeout.is_some() => Event::IdleCheck,
        };

        match event {
            Event::Open(req) => {
                session.touch();
                let stream = poll_fn(|cx| conn.poll_new_outbound(cx)).await;
                let _ = req.resp.send(stream);
            }
            Event::Inbound(Some(Ok(_stream))) => {
                session.touch();
                warn!("服务端收到意外的 inbound mux stream，已忽略");
            }
            Event::Inbound(Some(Err(e))) => {
                error!("mux 连接错误: {}", e);
                break;
            }
            Event::IdleCheck => {
                if let Some(timeout) = idle_timeout {
                    if session.idle_for(timeout) {
                        warn!(session_id = session.id, "mux 空闲超时, 主动关闭连接");
                        break;
                    }
                }
            }
            Event::Inbound(None) | Event::Closed | Event::Shutdown => {
                break;
            }
        }
    }

    pool.remove_session(session.id).await;
    pool.ensure_target().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Result};
    use futures::future::poll_fn;
    use std::sync::Arc;
    use tokio::io::duplex;
    use tokio::time::{sleep, timeout, Duration};
    use tokio_util::compat::TokioAsyncReadCompatExt;
    use yamux::{Config as YamuxConfig, Connection as YamuxConnection, Mode as YamuxMode};

    #[tokio::test]
    async fn test_open_stream_waits_until_stream_released() -> Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(8);
        let pool = MuxPool::new(MuxSelect::LeastStreams, 1, 1, 0, request_tx);

        let _request_drain = tokio::spawn(async move {
            while request_rx.recv().await.is_some() {}
        });

        let (client_io, server_io) = duplex(1024);
        let mut client =
            YamuxConnection::new(client_io.compat(), YamuxConfig::default(), YamuxMode::Client);
        let mut server =
            YamuxConnection::new(server_io.compat(), YamuxConfig::default(), YamuxMode::Server);

        let client_task = tokio::spawn(async move {
            loop {
                match poll_fn(|cx| client.poll_next_inbound(cx)).await {
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        });

        let (open_tx, mut open_rx) = mpsc::channel(4);
        let _session = pool.add_session(open_tx).await;

        let server_task = tokio::spawn(async move {
            while let Some(req) = open_rx.recv().await {
                let stream = poll_fn(|cx| server.poll_new_outbound(cx)).await;
                let _ = req.resp.send(stream.map_err(|e| e));
            }
        });

        let first = pool.open_stream(Some(Duration::from_millis(200))).await?;
        let pool_clone = Arc::clone(&pool);
        let second_task = tokio::spawn(async move {
            pool_clone.open_stream(Some(Duration::from_millis(200))).await
        });

        sleep(Duration::from_millis(20)).await;
        assert!(!second_task.is_finished());

        drop(first);

        let second = timeout(Duration::from_millis(200), second_task)
            .await
            .map_err(|_| anyhow!("等待第二个 mux stream 超时"))?
            .map_err(|e| anyhow!("第二个 mux stream 任务失败: {}", e))??;
        drop(second);

        server_task.abort();
        client_task.abort();

        Ok(())
    }
}
