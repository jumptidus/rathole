use backon::ExponentialBuilder;
use std::time::Duration;

/// UDP 接收缓冲区大小，按 SOCKS5 UDP 负载上限对齐
pub const UDP_BUFFER_SIZE: usize = 0x10000;
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
