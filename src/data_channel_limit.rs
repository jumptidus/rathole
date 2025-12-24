use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tracing::error;

pub struct DataChannelLimiter {
    limit: AtomicUsize,
    in_use: AtomicUsize,
}

pub(crate) struct DataChannelPermit {
    limiter: Arc<DataChannelLimiter>,
}

impl Drop for DataChannelPermit {
    fn drop(&mut self) {
        self.limiter.in_use.fetch_sub(1, Ordering::Release);
    }
}

impl DataChannelLimiter {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: AtomicUsize::new(limit),
            in_use: AtomicUsize::new(0),
        })
    }

    pub fn update_limit(&self, limit: usize) -> bool {
        self.limit.swap(limit, Ordering::Relaxed) != limit
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<DataChannelPermit> {
        loop {
            let limit = self.limit.load(Ordering::Acquire);
            let current = self.in_use.load(Ordering::Acquire);
            if current >= limit {
                return None;
            }

            if self
                .in_use
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(DataChannelPermit {
                    limiter: Arc::clone(self),
                });
            }
        }
    }
}

lazy_static! {
    static ref REGISTRY: RwLock<HashMap<String, Arc<DataChannelLimiter>>> =
        RwLock::new(HashMap::new());
}

pub fn register_data_channel_limiter(
    service_name: &str,
    limiter: Arc<DataChannelLimiter>,
) -> Arc<DataChannelLimiter> {
    let mut registry = match REGISTRY.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!("数据通道注册表写锁已被 poison，继续使用已持有的锁");
            poisoned.into_inner()
        }
    };
    if let Some(existing) = registry.get(service_name) {
        return Arc::clone(existing);
    }
    registry.insert(service_name.to_string(), Arc::clone(&limiter));
    limiter
}

pub fn unregister_data_channel_limiter(service_name: &str) -> Option<Arc<DataChannelLimiter>> {
    let mut registry = match REGISTRY.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!("数据通道注册表写锁已被 poison，继续使用已持有的锁");
            poisoned.into_inner()
        }
    };
    registry.remove(service_name)
}

pub(crate) fn get_data_channel_limiter(service_name: &str) -> Option<Arc<DataChannelLimiter>> {
    let registry = match REGISTRY.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!("数据通道注册表读锁已被 poison，继续使用已持有的锁");
            poisoned.into_inner()
        }
    };
    registry.get(service_name).cloned()
}
