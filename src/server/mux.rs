use crate::config::MuxSelect;
use crate::protocol::{ControlChannelMuxResp, MuxRespKind};
use crate::server::DataChannelRequest;
use anyhow::{anyhow, Result};
use backon::{BackoffBuilder, ExponentialBuilder};
use futures::future::poll_fn;
use futures::io::{AsyncRead, AsyncWrite};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, Notify, RwLock};
use tokio::time::{sleep, timeout};
use tracing::{error, warn};
use yamux::{Connection, ConnectionError, Stream};

static MUX_SESSION_ID: AtomicU64 = AtomicU64::new(1);
const DEFAULT_PENDING_TIMEOUT_SECS: u64 = 30;

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
struct RequestBackoff {
    builder: ExponentialBuilder,
    backoff: backon::ExponentialBackoff,
}

impl RequestBackoff {
    fn new() -> Self {
        let builder = ExponentialBuilder::default()
            .with_factor(1.5)
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(5))
            .without_max_times()
            .with_jitter();
        let backoff = builder.build();
        Self { builder, backoff }
    }

    fn reset(&mut self) {
        self.backoff = self.builder.build();
    }

    fn next_delay(&mut self) -> Option<Duration> {
        self.backoff.next()
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
    pending: AtomicUsize,
    client_max_pool: AtomicU16,
    pending_timeout: Duration,
    pending_timestamps: Mutex<VecDeque<Instant>>,
    rr: AtomicUsize,
    request_lock: Mutex<()>,
    request_backoff: Mutex<RequestBackoff>,
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
            pending: AtomicUsize::new(0),
            client_max_pool: AtomicU16::new(u16::MAX),
            pending_timeout: Duration::from_secs(DEFAULT_PENDING_TIMEOUT_SECS),
            pending_timestamps: Mutex::new(VecDeque::new()),
            rr: AtomicUsize::new(0),
            request_lock: Mutex::new(()),
            request_backoff: Mutex::new(RequestBackoff::new()),
        })
    }

    fn calc_effective_and_target(&self, sessions_len: usize) -> (usize, usize, usize) {
        let pending = self.pending.load(Ordering::Acquire);
        let effective = sessions_len + pending;
        let client_limit = self.client_max_pool.load(Ordering::Acquire) as usize;
        let actual_target = if client_limit == u16::MAX as usize {
            self.target
        } else {
            self.target.min(client_limit)
        };
        (effective, actual_target, pending)
    }

    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub async fn ensure_target(&self) {
        let _guard = self.request_lock.lock().await;
        self.cleanup_stale_pending().await;

        let sessions_len = self.sessions.read().await.len();
        let (effective, actual_target, pending) = self.calc_effective_and_target(sessions_len);
        if pending > 0 || effective >= actual_target {
            if effective >= actual_target {
                self.request_backoff.lock().await.reset();
            }
            return;
        }
        let _ = self.request_tx.send(DataChannelRequest::Mux).await;
    }

    async fn ensure_target_with_backoff(&self) {
        let _guard = self.request_lock.lock().await;
        self.cleanup_stale_pending().await;

        let sessions_len = self.sessions.read().await.len();
        let (effective, actual_target, pending) = self.calc_effective_and_target(sessions_len);
        if pending > 0 || effective >= actual_target {
            if effective >= actual_target {
                self.request_backoff.lock().await.reset();
            }
            return;
        }

        let delay = self
            .request_backoff
            .lock()
            .await
            .next_delay()
            .unwrap_or(Duration::from_millis(0));
        if !delay.is_zero() {
            sleep(delay).await;
        }

        self.cleanup_stale_pending().await;
        let sessions_len = self.sessions.read().await.len();
        let (effective, actual_target, pending) = self.calc_effective_and_target(sessions_len);
        if pending > 0 || effective >= actual_target {
            if effective >= actual_target {
                self.request_backoff.lock().await.reset();
            }
            return;
        }

        let _ = self.request_tx.send(DataChannelRequest::Mux).await;
    }

    pub async fn add_session(&self, open_tx: mpsc::Sender<OpenStreamRequest>) -> Arc<MuxSession> {
        let session = MuxSession::new(open_tx, self.max_streams, Arc::clone(&self.notify));
        self.decrement_pending().await;
        self.sessions.write().await.push(session.clone());
        self.request_backoff.lock().await.reset();
        self.notify.notify_waiters();
        self.ensure_target().await;
        session
    }

    pub fn max_streams(&self) -> usize {
        self.max_streams
    }

    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    pub async fn on_mux_request_sent(&self) {
        self.pending.fetch_add(1, Ordering::Release);
        let mut timestamps = self.pending_timestamps.lock().await;
        timestamps.push_back(Instant::now());
    }

    pub async fn on_mux_resp(&self, resp: ControlChannelMuxResp) {
        self.client_max_pool.store(resp.max_pool, Ordering::Release);
        match resp.kind {
            MuxRespKind::Accepted => {}
            MuxRespKind::Rejected | MuxRespKind::Failed => {
                if self.decrement_pending().await {
                    self.notify.notify_waiters();
                }
            }
        }
    }

    pub async fn cleanup_stale_pending(&self) {
        let now = Instant::now();
        let removed = {
            let mut timestamps = self.pending_timestamps.lock().await;
            let mut removed = 0usize;
            while let Some(ts) = timestamps.front() {
                if now.saturating_duration_since(*ts) >= self.pending_timeout {
                    timestamps.pop_front();
                    removed += 1;
                } else {
                    break;
                }
            }
            removed
        };
        if removed == 0 {
            return;
        }
        let dec = self.decrease_pending_by(removed);
        if dec > 0 {
            self.notify.notify_waiters();
        }
    }

    async fn decrement_pending(&self) -> bool {
        let mut current = self.pending.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return false;
            }
            match self.pending.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let mut timestamps = self.pending_timestamps.lock().await;
                    if !timestamps.is_empty() {
                        timestamps.pop_front();
                    }
                    return true;
                }
                Err(v) => current = v,
            }
        }
    }

    fn decrease_pending_by(&self, amount: usize) -> usize {
        let mut current = self.pending.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return 0;
            }
            let dec = amount.min(current);
            match self.pending.compare_exchange(
                current,
                current - dec,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return dec,
                Err(v) => current = v,
            }
        }
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
            let mut retries_left = 1usize;
            let mut exclude_id = None;
            let mut last_err: Option<anyhow::Error> = None;

            loop {
                if let Some(session) = self.pick_session(exclude_id).await {
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
                        let err = anyhow!("mux session channel closed");
                        if retries_left > 0 {
                            retries_left -= 1;
                            exclude_id = Some(session.id);
                            last_err = Some(err);
                            continue;
                        }
                        return Err(err);
                    }

                    return match resp_rx.await {
                        Ok(Ok(stream)) => {
                            Ok(permit.into_stream(stream))
                        }
                        Ok(Err(e)) => {
                            let err = anyhow!("open mux stream failed: {}", e);
                            if retries_left > 0 {
                                retries_left -= 1;
                                exclude_id = Some(session.id);
                                last_err = Some(err);
                                continue;
                            }
                            Err(err)
                        }
                        Err(_) => {
                            let err = anyhow!("mux session dropped");
                            if retries_left > 0 {
                                retries_left -= 1;
                                exclude_id = Some(session.id);
                                last_err = Some(err);
                                continue;
                            }
                            Err(err)
                        }
                    }
                }

                if let Some(err) = last_err.take() {
                    return Err(err);
                }

                self.ensure_target_with_backoff().await;
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = sleep(self.pending_timeout) => {
                        self.cleanup_stale_pending().await;
                    }
                }
            }
        };

        match wait_timeout {
            Some(dur) => timeout(dur, open).await?,
            None => open.await,
        }
    }

    async fn pick_session(&self, exclude_id: Option<u64>) -> Option<Arc<MuxSession>> {
        let sessions = self.sessions.read().await;
        if sessions.is_empty() {
            return None;
        }

        match self.select {
            MuxSelect::LeastStreams => sessions
                .iter()
                .filter(|s| s.can_open() && exclude_id.is_none_or(|id| s.id != id))
                .min_by_key(|s| s.inflight())
                .cloned(),
            MuxSelect::RoundRobin => {
                let mut idx = self.rr.fetch_add(1, Ordering::Relaxed);
                for _ in 0..sessions.len() {
                    let s = &sessions[idx % sessions.len()];
                    idx += 1;
                    if exclude_id.is_some_and(|id| s.id == id) {
                        continue;
                    }
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
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Instant;
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

    #[tokio::test]
    async fn test_open_stream_retries_next_session_on_channel_closed() -> Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(8);
        let pool = MuxPool::new(MuxSelect::LeastStreams, 1, 1, 0, request_tx);

        let _request_drain = tokio::spawn(async move {
            while request_rx.recv().await.is_some() {}
        });

        let (bad_tx, bad_rx) = mpsc::channel(1);
        drop(bad_rx);
        let _bad_session = pool.add_session(bad_tx).await;

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

        let (good_tx, mut good_rx) = mpsc::channel(4);
        let _good_session = pool.add_session(good_tx).await;

        let server_task = tokio::spawn(async move {
            while let Some(req) = good_rx.recv().await {
                let stream = poll_fn(|cx| server.poll_new_outbound(cx)).await;
                let _ = req.resp.send(stream.map_err(|e| e));
            }
        });

        let stream = pool.open_stream(Some(Duration::from_millis(200))).await?;
        drop(stream);

        server_task.abort();
        client_task.abort();

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_stale_pending_removes_expired() -> Result<()> {
        let (request_tx, _request_rx) = mpsc::channel(1);
        let pool = MuxPool::new(MuxSelect::LeastStreams, 1, 1, 0, request_tx);

        pool.pending.store(1, Ordering::Release);
        {
            let mut timestamps = pool.pending_timestamps.lock().await;
            timestamps.push_back(Instant::now() - Duration::from_secs(DEFAULT_PENDING_TIMEOUT_SECS + 1));
        }

        pool.cleanup_stale_pending().await;
        assert_eq!(pool.pending.load(Ordering::Acquire), 0);
        Ok(())
    }
}
