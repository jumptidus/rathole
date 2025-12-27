use crate::config::{ServerConfig, ServerServiceConfig, ServiceType};
use crate::helper::write_and_flush;
use crate::protocol::Hello::{ControlChannelHello, DataChannelHello};
use crate::protocol::{
    self, read_auth, read_hello, Ack, DataChannelMode, HASH_WIDTH_IN_BYTES, PROTO_V2, PROTO_V3,
};
use crate::transport::{SocketOpts, Transport};
use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, info, info_span, warn, Instrument};

use super::control::ControlChannelHandle;
use super::health::spawn_health_probe_task;
use super::{mux::run_mux_server, ControlChannelMap, Nonce, ServiceDigest, CHAN_SIZE, HANDSHAKE_TIMEOUT};
use tokio_util::compat::TokioAsyncReadCompatExt;
use yamux::{Config as YamuxConfig, Connection as YamuxConnection, Mode as YamuxMode};

// Handle connections to `server.bind_addr`
pub(super) async fn handle_connection<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    server_config: Arc<ServerConfig>,
    addr: SocketAddr,
) -> Result<()> {
    // 读取握手消息
    let hello = match timeout(
        Duration::from_secs(HANDSHAKE_TIMEOUT),
        read_hello(&mut conn),
    )
    .await
    {
        Ok(Ok(hello)) => hello,
        Ok(Err(e)) => {
            let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
            error!("读取握手消息失败: {}, 显式关闭连接以防资源泄露", e);
            return Err(e);
        }
        Err(_) => {
            let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
            error!("握手超时, 显式关闭连接以防资源泄露");
            bail!("握手超时");
        }
    };

    // 处理握手消息
    match hello {
        ControlChannelHello(protocol_version, service_digest) => {
            // 根据协议版本处理时间戳
            let timestamp = if protocol_version == PROTO_V2 || protocol_version == PROTO_V3 {
                // 对于V2/V3协议, 客户端必须发送时间戳
                match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), conn.read_u64_le()).await {
                    Ok(Ok(ts)) => {
                        debug!("成功读取V2客户端时间戳: {}", ts);
                        ts
                    }
                    Ok(Err(e)) => {
                        error!("读取V2客户端时间戳失败: {}, 中断连接", e);
                        let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
                        return Err(e.into());
                    }
                    Err(_) => {
                        error!("读取V2客户端时间戳超时, 中断连接");
                        let _ = timeout(Duration::from_secs(3), conn.shutdown()).await;
                        bail!("读取时间戳超时");
                    }
                }
            } else {
                // 对于旧版协议, 时间戳为0
                0
            };

            info!("客户端时间戳: {}", timestamp);

            // 处理控制通道握手
            do_control_channel_handshake(
                conn,
                services,
                control_channels,
                service_digest,
                server_config,
                protocol_version,
                timestamp,
                addr,
            )
            .await?;
        }
        DataChannelHello(protocol_version, nonce) => {
            do_data_channel_handshake(conn, control_channels, nonce, protocol_version).await?;
        }
    }
    Ok(())
}

async fn write_all_with_timeout<T>(
    conn: &mut T,
    data: &[u8],
    label: &str,
) -> Result<()>
where
    T: AsyncWrite + Unpin,
{
    match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), conn.write_all(data)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            error!("{}失败: {}", label, e);
            Err(e.into())
        }
        Err(_) => {
            error!("{}超时", label);
            bail!("{}超时", label);
        }
    }
}

async fn write_and_flush_with_timeout<T>(
    conn: &mut T,
    data: &[u8],
    label: &str,
) -> Result<()>
where
    T: AsyncWrite + Unpin,
{
    match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), write_and_flush(conn, data)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            error!("{}失败: {:#}", label, e);
            Err(e)
        }
        Err(_) => {
            error!("{}超时", label);
            bail!("{}超时", label);
        }
    }
}

async fn do_control_channel_handshake<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    service_digest: ServiceDigest,
    server_config: Arc<ServerConfig>,
    protocol_version: u8,
    timestamp: u64,
    addr: SocketAddr,
) -> Result<()> {
    info!(
        "尝试建立控制通道, 客户端时间戳: {}, 地址: {}",
        timestamp, addr
    );

    T::hint(&conn, SocketOpts::for_control_channel());

    // 生成一个nonce
    let mut nonce = vec![0u8; HASH_WIDTH_IN_BYTES];
    rand::thread_rng().fill_bytes(&mut nonce);

    // 发送握手消息
    let nonce_array: [u8; HASH_WIDTH_IN_BYTES] =
        nonce.clone().try_into().map_err(|v: Vec<u8>| {
            anyhow!(
                "Nonce 长度不匹配. 期望: {}, 实际: {}",
                HASH_WIDTH_IN_BYTES,
                v.len()
            )
        })?;
    let hello_send = ControlChannelHello(protocol_version, nonce_array);
    let hello_bytes = bincode::serialize(&hello_send)?;
    write_and_flush_with_timeout(&mut conn, &hello_bytes, "发送握手消息").await?;

    // 查找服务
    let service_config = match services.read().await.get(&service_digest) {
        Some(v) => v,
        None => {
            write_all_with_timeout(
                &mut conn,
                &bincode::serialize(&Ack::ServiceNotExist)?,
                "发送 Ack::ServiceNotExist",
            )
            .await?;
            bail!("未找到服务: {}", hex::encode(service_digest));
        }
    }
    .to_owned();

    let service_name = service_config.name.clone();

    // 计算校验和
    let token = match service_config.token.as_ref() {
        Some(t) => t.as_bytes(),
        None => {
            error!(service = %service_name, "认证失败: 服务未配置token");
            write_all_with_timeout(
                &mut conn,
                &bincode::serialize(&Ack::AuthFailed)?,
                "发送 Ack::AuthFailed",
            )
            .await?;
            bail!("服务 {} 未配置token", service_name);
        }
    };
    let mut concat = Vec::from(token);
    concat.append(&mut nonce);

    // 读取认证
    let protocol::Auth(d) =
        match timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), read_auth(&mut conn)).await {
            Ok(Ok(auth)) => auth,
            Ok(Err(e)) => {
                error!("读取认证失败: {}", e);
                return Err(e);
            }
            Err(_) => {
                error!("读取认证超时");
                bail!("认证超时");
            }
        };

    // 验证
    let session_key = protocol::digest(&concat);
    if session_key != d {
        write_all_with_timeout(
            &mut conn,
            &bincode::serialize(&Ack::AuthFailed)?,
            "发送 Ack::AuthFailed",
        )
        .await?;
        debug!(
            "认证失败, 期望 {}, 实际 {}",
            hex::encode(session_key),
            hex::encode(d)
        );
        bail!("服务 {} 认证失败", service_name);
    } else {
        // 检查是否存在一个相同的通道, 并比较时间戳
        let existing_channel = {
            let control_map_guard = control_channels.read().await;
            if let Some(existing) = control_map_guard.get1(&service_digest) {
                // 如果旧的时间戳大于新时间戳, 拒绝新连接
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp(),
                    old_addr = %existing.addr(),
                    new_ts = timestamp,
                    new_addr = %addr,
                    "找到一个相同的通道"
                );
                if existing.timestamp() > timestamp {
                    info!(
                        service = %service_name,
                        old_ts = existing.timestamp(),
                        old_addr = %existing.addr(),
                        new_ts = timestamp,
                        new_addr = %addr,
                        "拒绝连接: 旧时间戳:{} >= 新时间戳:{}",
                        existing.timestamp(), timestamp
                    );
                    write_all_with_timeout(
                        &mut conn,
                        &bincode::serialize(&Ack::AuthFailed)?,
                        "发送 Ack::AuthFailed",
                    )
                    .await?;
                    return Ok(());
                }
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp(),
                    old_addr = %existing.addr(),
                    new_ts = timestamp,
                    new_addr = %addr,
                    "替换旧通道"
                );
                true
            } else {
                false
            }
        };

        if existing_channel {
            info!(
                service = %service_name,
                new_ts = timestamp,
                new_addr = %addr,
                "创建或替换控制通道"
            );
        }

        // 准备控制通道句柄和控制任务
        let (handle, control_task_future) = ControlChannelHandle::prepare(
            conn,
            service_config.clone(),
            server_config.heartbeat_interval,
            server_config.data_channel_wait_timeout,
            protocol_version,
            timestamp,
            addr,
        );
        let shutdown_rx_for_probe = handle.subscribe_shutdown();

        let control_channels_weak = Arc::downgrade(&control_channels); // 弱引用控制通道句柄
        let control_channels_weak_for_cleanup = control_channels_weak.clone();

        // 插入句柄到映射中(在写锁范围内)
        {
            let mut control_map_guard = control_channels.write().await;

            if let Some(existing) = control_map_guard.get1(&service_digest) {
                info!(
                    service = %service_name,
                    old_ts = existing.timestamp(),
                    new_ts = timestamp,
                    old_addr = %existing.addr(),
                    new_addr = %addr,
                    session_key = %hex::encode(session_key),
                    "替换旧通道, 创建新控制通道"
                );
                if let Some(old) = control_map_guard.remove1(&service_digest) {
                    old.shutdown();
                }
            } else if control_map_guard.get2(&session_key).is_some() {
                warn!(
                    service = %service_name,
                    session_key = %hex::encode(session_key),
                    "检测到潜在的会话密钥冲突, 移除旧条目"
                );
                if let Some(old) = control_map_guard.remove2(&session_key) {
                    old.shutdown();
                }
            }

            // 插入新句柄. `handle` 被移动到映射中
            let _ = control_map_guard.insert(service_digest, session_key, handle);
            info!(
                service = %service_name,
                session_key = %hex::encode(session_key),
                ts = timestamp,
                addr = %addr,
                "控制通道句柄插入成功"
            );
        }

        // 克隆名称再次用于跨度, 因为原始名称被移动到任务中
        let service_name_for_control_span = service_name.clone();

        // 启动控制任务
        let service_name_for_control_task = service_name.clone();
        let control_task_handle = tokio::spawn(
            async move {
                // `prepare` 返回的 future 拥有连接并运行 `ControlChannel::run`
                if let Err(err) = control_task_future.await {
                    error!(service = %service_name_for_control_task, session_key = %hex::encode(session_key), "控制通道任务失败: {:#}", err);
                } else {
                    debug!(service = %service_name_for_control_task, session_key = %hex::encode(session_key), "控制通道任务完成");
                }
            }
            // 使用新克隆的变量进行跨度
            .instrument(info_span!("控制通道任务", service = %service_name_for_control_span, session_key = %hex::encode(session_key))),
        );

        // 为清理任务克隆会话密钥
        let session_key_for_cleanup = session_key;

        // 启动清理任务
        tokio::spawn(
            async move {
                // 等待主控制任务完成(正常或异常)
                match control_task_handle.await {
                    Ok(_) => {
                        debug!(session_key = %hex::encode(session_key_for_cleanup), "控制通道任务完成, 准备清理");
                    },
                    Err(e) => {
                        error!(session_key = %hex::encode(session_key_for_cleanup), "控制通道任务失败, 准备清理: {}", e);
                    }
                }

                if let Some(map_arc) = control_channels_weak_for_cleanup.upgrade() {
                    debug!(session_key = %hex::encode(session_key_for_cleanup), "获取锁, 准备清理");
                    let mut map_guard = map_arc.write().await;
                    if let Some(_removed_handle) = map_guard.remove2(&session_key_for_cleanup) {
                        info!(session_key = %hex::encode(session_key_for_cleanup), "控制通道句柄清理成功");
                    } else {
                        warn!(session_key = %hex::encode(session_key_for_cleanup), "控制通道句柄在清理前被移除");
                    }
                } else {
                    warn!(session_key = %hex::encode(session_key_for_cleanup), "控制通道映射在清理前被释放");
                }
            }
            // 使用特定于跨度的克隆的会话密钥来记录清理任务
            .instrument(info_span!("cleanup_task", session_key = %hex::encode(session_key_for_cleanup))),
        );

        // 启动健康探测任务
        {
            let control_channels_weak_for_probe = control_channels_weak.clone();
            let session_key_for_probe = session_key_for_cleanup;
            let service_name_for_probe = service_name.clone();
            let bind_addr_for_probe = service_config.bind_addr.clone();
            let is_tcp = matches!(service_config.service_type, ServiceType::Tcp);

            spawn_health_probe_task(
                service_name_for_probe,
                bind_addr_for_probe,
                is_tcp,
                shutdown_rx_for_probe,
                control_channels_weak_for_probe,
                session_key_for_probe,
            );
        }
    }

    Ok(())
}

async fn do_data_channel_handshake<T: 'static + Transport>(
    mut conn: T::Stream,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    nonce: Nonce,
    protocol_version: u8,
) -> Result<()> {
    debug!("Try to handshake a data channel");

    let mode = if protocol_version == PROTO_V3 {
        protocol::read_data_channel_mode(&mut conn).await?
    } else {
        DataChannelMode::Plain
    };

    // Validate
    let data_channel = {
        let control_channels_guard = control_channels.read().await;
        control_channels_guard.get2(&nonce).map(|handle| {
            (
                handle.data_channel_sender(),
                handle.socket_opts(),
                handle.mux_pool(),
                handle.subscribe_shutdown(),
            )
        })
    };
    match data_channel {
        Some((data_ch_tx, socket_opts, mux_pool, shutdown_rx)) => {
            T::hint(&conn, socket_opts);

            match mode {
                DataChannelMode::Plain => {
                    // Send the data channel to the corresponding control channel
                    data_ch_tx
                        .send(conn)
                        .await
                        .with_context(|| "Data channel for a stale control channel")?;
                }
                DataChannelMode::Mux => {
                    let Some(pool) = mux_pool else {
                        warn!("收到 Mux 数据通道但 mux 未启用, 已丢弃");
                        return Ok(());
                    };
                    let mut cfg = YamuxConfig::default();
                    cfg.set_max_num_streams(pool.max_streams());

                    let yamux_conn = YamuxConnection::new(conn.compat(), cfg, YamuxMode::Server);
                    let (open_tx, open_rx) = mpsc::channel(CHAN_SIZE / 2);
                    let session = pool.add_session(open_tx).await;
                    tokio::spawn(run_mux_server(
                        yamux_conn,
                        open_rx,
                        pool.clone(),
                        session,
                        shutdown_rx,
                    ));
                }
            }
        }
        None => {
            warn!("Data channel has incorrect nonce");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::do_data_channel_handshake;
    use crate::protocol::HASH_WIDTH_IN_BYTES;
    use crate::server::control::ControlChannelHandle;
    use crate::server::test_support::{build_service_config, TestTransport};
    use crate::server::ControlChannelMap;
    use crate::protocol::{PROTO_V2, PROTO_V3};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{duplex, AsyncWriteExt};
    use tokio::sync::{broadcast, mpsc, RwLock};
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_data_channel_handshake_releases_read_lock_on_backpressure() {
        let control_channels = Arc::new(RwLock::new(ControlChannelMap::new()));
        let (data_ch_tx, mut data_ch_rx) = mpsc::channel(1);
        let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);

        let handle = ControlChannelHandle::new_for_test(
            shutdown_tx,
            data_ch_tx.clone(),
            build_service_config(),
            0,
            "127.0.0.1:0".parse().unwrap(),
        );

        let nonce = [0u8; HASH_WIDTH_IN_BYTES];
        let service_digest = [1u8; HASH_WIDTH_IN_BYTES];

        {
            let mut map = control_channels.write().await;
            assert!(map.insert(service_digest, nonce, handle).is_ok());
        }

        let (fill_stream, _fill_peer) = duplex(64);
        data_ch_tx.send(fill_stream).await.unwrap();

        let (conn, _peer) = duplex(64);
        let control_channels_for_task = control_channels.clone();
        let handshake_task = tokio::spawn(async move {
            do_data_channel_handshake::<TestTransport>(conn, control_channels_for_task, nonce, PROTO_V2)
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handshake_task.is_finished());

        let lock_result = timeout(Duration::from_millis(100), control_channels.write()).await;
        assert!(lock_result.is_ok());
        drop(lock_result.unwrap());

        let _ = data_ch_rx.recv().await;
        let _ = data_ch_rx.recv().await;

        let result = timeout(Duration::from_secs(1), handshake_task).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_v3_mux_mode_without_pool_is_ignored() {
        let control_channels = Arc::new(RwLock::new(ControlChannelMap::new()));
        let (data_ch_tx, mut data_ch_rx) = mpsc::channel(1);
        let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);

        let handle = ControlChannelHandle::new_for_test(
            shutdown_tx,
            data_ch_tx.clone(),
            build_service_config(),
            0,
            "127.0.0.1:0".parse().unwrap(),
        );

        let nonce = [0u8; HASH_WIDTH_IN_BYTES];
        let service_digest = [1u8; HASH_WIDTH_IN_BYTES];

        {
            let mut map = control_channels.write().await;
            assert!(map.insert(service_digest, nonce, handle).is_ok());
        }

        let (mut client, server) = duplex(8);
        client.write_u8(1).await.unwrap(); // DataChannelMode::Mux

        let task = tokio::spawn(async move {
            do_data_channel_handshake::<TestTransport>(server, control_channels, nonce, PROTO_V3)
                .await
                .unwrap();
        });

        let recv = timeout(Duration::from_millis(200), data_ch_rx.recv()).await;
        assert!(recv.is_err());

        task.await.unwrap();
    }
}
