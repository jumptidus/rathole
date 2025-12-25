use anyhow::Result;
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::error;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncReadWrite for T {}

pub type DataChannelTcpHandler = dyn Fn(
        &str,
        Box<dyn AsyncReadWrite + Unpin + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
    + Send
    + Sync;

lazy_static! {
    static ref TCP_HANDLER_REGISTRY: RwLock<HashMap<String, Arc<DataChannelTcpHandler>>> =
        RwLock::new(HashMap::new());
}

pub fn register_data_channel_tcp_handler(
    service_name: &str,
    handler: Arc<DataChannelTcpHandler>,
) -> Arc<DataChannelTcpHandler> {
    let mut registry = TCP_HANDLER_REGISTRY.write().unwrap_or_else(|poisoned| {
        error!("数据通道处理器注册表写锁已被 poison，继续使用已持有的锁");
        poisoned.into_inner()
    });
    registry.insert(service_name.to_string(), Arc::clone(&handler));
    handler
}

pub fn unregister_data_channel_tcp_handler(
    service_name: &str,
) -> Option<Arc<DataChannelTcpHandler>> {
    let mut registry = TCP_HANDLER_REGISTRY.write().unwrap_or_else(|poisoned| {
        error!("数据通道处理器注册表写锁已被 poison，继续使用已持有的锁");
        poisoned.into_inner()
    });
    registry.remove(service_name)
}

pub(crate) fn get_data_channel_tcp_handler(
    service_name: &str,
) -> Option<Arc<DataChannelTcpHandler>> {
    let registry = TCP_HANDLER_REGISTRY.read().unwrap_or_else(|poisoned| {
        error!("数据通道处理器注册表读锁已被 poison，继续使用已持有的锁");
        poisoned.into_inner()
    });
    registry.get(service_name).cloned()
}
