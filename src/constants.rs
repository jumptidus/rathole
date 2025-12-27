use backon::ExponentialBuilder;
use std::time::Duration;

// FIXME: Determine reasonable size
/// UDP MTU. Currently far larger than necessary
pub const UDP_BUFFER_SIZE: usize = 2048;
#[allow(dead_code)]
pub const UDP_SENDQ_SIZE: usize = 1024;
#[allow(dead_code)]
pub const UDP_TIMEOUT: u64 = 60;

pub fn listen_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_factor(1.5)
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(1))
        .without_max_times()
        .with_jitter()
}

#[allow(dead_code)]
pub fn run_control_chan_backoff(interval: u64) -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_factor(3.0)
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(interval))
        .without_max_times()
        .with_jitter()
}
