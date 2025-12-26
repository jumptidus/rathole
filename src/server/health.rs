use crate::health::{run_health_probe_task, HealthProbeConfig};
use crate::transport::Transport;
use std::sync::Weak;
use tokio::sync::{broadcast, RwLock};
use tracing::{error, info_span, Instrument};

use super::{ControlChannelMap, Nonce};

pub(super) fn spawn_health_probe_task<T: Transport + 'static>(
    service_name: String,
    bind_addr: String,
    is_tcp: bool,
    shutdown_rx: broadcast::Receiver<bool>,
    control_channels_weak: Weak<RwLock<ControlChannelMap<T>>>,
    session_key: Nonce,
) {
    // 使用默认配置，后续可以从服务配置中读取
    let probe_config = HealthProbeConfig::default();

    let probe_span = if is_tcp {
        info_span!("health_probe", service = %service_name, proto = "TCP")
    } else {
        info_span!("health_probe", service = %service_name, proto = "UDP")
    };

    tokio::spawn(
        run_health_probe_task(
            service_name.clone(),
            bind_addr,
            probe_config,
            is_tcp,
            shutdown_rx,
            move || {
                // 失败处理闭包
                let control_channels_weak = control_channels_weak.clone();
                let session_key = session_key;
                let service_name = service_name.clone();

                tokio::spawn(async move {
                    if let Some(map_arc) = control_channels_weak.upgrade() {
                        let mut map = map_arc.write().await;
                        if let Some(handle) = map.remove2(&session_key) {
                            handle.shutdown();
                            error!(
                                service = %service_name,
                                "健康探测失败达到阈值，已关闭并移除控制通道，等待客户端重连"
                            );
                        }
                    }
                });
            },
        )
        .instrument(probe_span),
    );
}
