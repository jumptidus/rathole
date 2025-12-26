use crate::config::{ServerServiceConfig, TransportConfig};
use crate::transport::{AddrMaybeCached, SocketOpts, Transport};
use anyhow::Result;
use async_trait::async_trait;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::{sleep, Instant};

#[derive(Debug)]
pub(super) struct TestTransport;

#[async_trait]
impl Transport for TestTransport {
    type Acceptor = ();
    type RawStream = ();
    type Stream = tokio::io::DuplexStream;

    fn new(_config: &TransportConfig) -> Result<Self> {
        unimplemented!("测试用: 不应调用");
    }

    fn hint(_conn: &Self::Stream, _opts: SocketOpts) {}

    async fn bind<T: ToSocketAddrs + Send + Sync>(&self, _addr: T) -> Result<Self::Acceptor> {
        unimplemented!("测试用: 不应调用");
    }

    async fn accept(&self, _a: &Self::Acceptor) -> Result<(Self::RawStream, SocketAddr)> {
        unimplemented!("测试用: 不应调用");
    }

    async fn handshake(&self, _conn: Self::RawStream) -> Result<Self::Stream> {
        unimplemented!("测试用: 不应调用");
    }

    async fn connect(&self, _addr: &AddrMaybeCached) -> Result<Self::Stream> {
        unimplemented!("测试用: 不应调用");
    }
}

pub(super) fn build_service_config() -> ServerServiceConfig {
    let mut service = ServerServiceConfig::with_name("test_service");
    service.bind_addr = "127.0.0.1:0".to_string();
    service
}

pub(super) fn pick_unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

pub(super) async fn connect_with_retry(addr: &str, wait: Duration) -> Result<TcpStream> {
    let deadline = Instant::now() + wait;
    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e.into());
                }
                sleep(Duration::from_millis(20)).await;
            }
        }
    }
}
