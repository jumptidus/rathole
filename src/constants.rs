use backon::ExponentialBuilder;
use std::time::Duration;

// FIXME: Determine reasonable size
/// UDP MTU. Currently far larger than necessary
pub const UDP_BUFFER_SIZE: usize = 2048;
pub const UDP_SENDQ_SIZE: usize = 1024;
// TCP 空闲超时（秒）
pub const TCP_IDLE_TIMEOUT: u64 = 30;

pub fn listen_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_factor(1.5)
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(1))
        .without_max_times()
        .with_jitter()
}

pub fn run_control_chan_backoff(max_interval: u64) -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_factor(3.0)
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(max_interval))
        .without_max_times()
        .with_jitter()
}
