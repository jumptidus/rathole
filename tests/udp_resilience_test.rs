use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    net::UdpSocket,
    sync::broadcast,
    time::{sleep, timeout, Instant},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

mod common;

const TEST_UDP_SERVICE_ADDR: &str = "127.0.0.1:8090";
const TEST_UDP_EXPOSED_ADDR: &str = "127.0.0.1:2340";
const TEST_SERVER_CONFIG: &str = "tests/udp_test_server.toml";
const TEST_CLIENT_CONFIG: &str = "tests/udp_test_client.toml";

fn init() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from("debug")),
        )
        .try_init();
}

/// 创建测试用的配置文件
async fn create_test_configs() -> Result<()> {
    use tokio::fs;
    
    let server_config = r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "test_token_123"

[server.services.udp_test]
type = "udp"
bind_addr = "0.0.0.0:2340"
"#;

    let client_config = r#"
[client]
remote_addr = "127.0.0.1:2333"
default_token = "test_token_123"

[client.services.udp_test]
type = "udp"
local_addr = "127.0.0.1:8090"
"#;

    fs::write(TEST_SERVER_CONFIG, server_config).await?;
    fs::write(TEST_CLIENT_CONFIG, client_config).await?;
    
    Ok(())
}

/// 清理测试配置文件
async fn cleanup_test_configs() -> Result<()> {
    use tokio::fs;
    let _ = fs::remove_file(TEST_SERVER_CONFIG).await;
    let _ = fs::remove_file(TEST_CLIENT_CONFIG).await;
    Ok(())
}

/// 模拟的有问题UDP服务器 - 可以按需断开连接
struct FlaktyUdpServer {
    socket: UdpSocket,
    enabled: Arc<AtomicBool>,
    packet_count: Arc<AtomicUsize>,
}

impl FlaktyUdpServer {
    async fn new(addr: &str) -> Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self {
            socket,
            enabled: Arc::new(AtomicBool::new(true)),
            packet_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
        info!("Flaky UDP server disabled");
    }

    fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
        info!("Flaky UDP server enabled");
    }

    fn get_packet_count(&self) -> usize {
        self.packet_count.load(Ordering::Relaxed)
    }

    async fn run(&self) -> Result<()> {
        let mut buf = [0u8; 1024];
        info!("Flaky UDP server started");

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((n, addr)) => {
                    self.packet_count.fetch_add(1, Ordering::Relaxed);
                    
                    if self.enabled.load(Ordering::Relaxed) {
                        // Echo back the data
                        if let Err(e) = self.socket.send_to(&buf[..n], addr).await {
                            error!("Failed to send response: {}", e);
                        } else {
                            debug!("Echoed {} bytes to {}", n, addr);
                        }
                    } else {
                        // Drop the packet when disabled
                        debug!("Dropped {} bytes from {} (server disabled)", n, addr);
                    }
                }
                Err(e) => {
                    error!("UDP server recv error: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}

/// 基础功能测试 - 确保UDP连接池在正常情况下工作
#[tokio::test]
async fn test_udp_basic_functionality() -> Result<()> {
    init();
    create_test_configs().await?;

    // 启动测试UDP服务器
    let server = FlaktyUdpServer::new(TEST_UDP_SERVICE_ADDR).await?;
    let server_task = tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // 启动rathole服务器和客户端
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);

    let rathole_server = tokio::spawn(async move {
        common::run_rathole_server(TEST_SERVER_CONFIG, server_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(500)).await;

    let rathole_client = tokio::spawn(async move {
        common::run_rathole_client(TEST_CLIENT_CONFIG, client_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(1)).await;

    // 发送测试数据
    let test_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let test_data = b"Hello UDP World!";
    
    test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await?;
    
    let mut response_buf = [0u8; 1024];
    let (n, _) = timeout(Duration::from_secs(5), test_socket.recv_from(&mut response_buf)).await??;
    
    assert_eq!(&response_buf[..n], test_data);
    info!("Basic functionality test passed");

    // 清理
    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    server_task.abort();
    
    cleanup_test_configs().await?;
    Ok(())
}

/// 连接中断恢复测试 - 模拟服务器暂时不可用然后恢复
#[tokio::test]
async fn test_udp_connection_recovery() -> Result<()> {
    init();
    create_test_configs().await?;

    let server = Arc::new(FlaktyUdpServer::new(TEST_UDP_SERVICE_ADDR).await?);
    let server_clone = server.clone();
    
    let server_task = tokio::spawn(async move {
        server_clone.run().await.unwrap();
    });

    // 启动rathole
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);

    let rathole_server = tokio::spawn(async move {
        common::run_rathole_server(TEST_SERVER_CONFIG, server_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(500)).await;

    let rathole_client = tokio::spawn(async move {
        common::run_rathole_client(TEST_CLIENT_CONFIG, client_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(1)).await;

    let test_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let test_data = b"Test Recovery";

    // 第一阶段：正常通信
    info!("Phase 1: Normal communication");
    test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await?;
    let mut buf = [0u8; 1024];
    let (n, _) = timeout(Duration::from_secs(5), test_socket.recv_from(&mut buf)).await??;
    assert_eq!(&buf[..n], test_data);
    
    // 第二阶段：禁用服务器（模拟连接中断）
    info!("Phase 2: Disable server (simulate connection interruption)");
    server.disable();
    
    // 发送数据应该超时（因为服务器被禁用）
    test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await?;
    let timeout_result = timeout(Duration::from_secs(3), test_socket.recv_from(&mut buf)).await;
    assert!(timeout_result.is_err(), "Expected timeout when server is disabled");
    
    // 第三阶段：重新启用服务器（模拟连接恢复）
    info!("Phase 3: Re-enable server (simulate connection recovery)");
    server.enable();
    
    // 等待重连机制生效
    sleep(Duration::from_secs(2)).await;
    
    // 现在通信应该恢复
    test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await?;
    let (n, _) = timeout(Duration::from_secs(10), test_socket.recv_from(&mut buf)).await??;
    assert_eq!(&buf[..n], test_data);
    
    info!("Connection recovery test passed");

    // 清理
    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    server_task.abort();
    
    cleanup_test_configs().await?;
    Ok(())
}

/// 客户端重启测试 - 模拟客户端崩溃重启场景
#[tokio::test]
async fn test_client_restart_recovery() -> Result<()> {
    init();
    create_test_configs().await?;

    let server = Arc::new(FlaktyUdpServer::new(TEST_UDP_SERVICE_ADDR).await?);
    let server_clone = server.clone();
    
    let server_task = tokio::spawn(async move {
        server_clone.run().await.unwrap();
    });

    // 启动rathole服务器
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let rathole_server = tokio::spawn(async move {
        common::run_rathole_server(TEST_SERVER_CONFIG, server_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(500)).await;

    let test_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let test_data = b"Test Client Restart";

    // 第一阶段：启动客户端并测试
    info!("Phase 1: Start client and test");
    let (client_shutdown_tx1, client_shutdown_rx1) = broadcast::channel(1);
    let rathole_client1 = tokio::spawn(async move {
        common::run_rathole_client(TEST_CLIENT_CONFIG, client_shutdown_rx1)
            .await
            .unwrap();
    });
    
    sleep(Duration::from_secs(1)).await;
    
    test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await?;
    let mut buf = [0u8; 1024];
    let (n, _) = timeout(Duration::from_secs(5), test_socket.recv_from(&mut buf)).await??;
    assert_eq!(&buf[..n], test_data);

    // 第二阶段：杀掉客户端
    info!("Phase 2: Kill client");
    client_shutdown_tx1.send(true)?;
    sleep(Duration::from_millis(500)).await;

    // 第三阶段：重启客户端
    info!("Phase 3: Restart client");
    let (client_shutdown_tx2, client_shutdown_rx2) = broadcast::channel(1);
    let rathole_client2 = tokio::spawn(async move {
        common::run_rathole_client(TEST_CLIENT_CONFIG, client_shutdown_rx2)
            .await
            .unwrap();
    });
    
    sleep(Duration::from_secs(2)).await;

    // 第四阶段：验证服务恢复
    info!("Phase 4: Verify service recovery");
    test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await?;
    let (n, _) = timeout(Duration::from_secs(10), test_socket.recv_from(&mut buf)).await??;
    assert_eq!(&buf[..n], test_data);
    
    info!("Client restart recovery test passed");

    // 清理
    server_shutdown_tx.send(true)?;
    client_shutdown_tx2.send(true)?;
    server_task.abort();
    
    cleanup_test_configs().await?;
    Ok(())
}

/// 长期稳定性测试 - 持续发送数据测试连接池的稳定性
#[tokio::test]
async fn test_long_term_stability() -> Result<()> {
    init();
    create_test_configs().await?;

    let server = Arc::new(FlaktyUdpServer::new(TEST_UDP_SERVICE_ADDR).await?);
    let server_clone = server.clone();
    
    let server_task = tokio::spawn(async move {
        server_clone.run().await.unwrap();
    });

    // 启动rathole
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);

    let rathole_server = tokio::spawn(async move {
        common::run_rathole_server(TEST_SERVER_CONFIG, server_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(500)).await;

    let rathole_client = tokio::spawn(async move {
        common::run_rathole_client(TEST_CLIENT_CONFIG, client_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(1)).await;

    let test_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let test_data = b"Stability Test";
    
    const TEST_DURATION: u64 = 30; // 30秒测试
    const SEND_INTERVAL_MS: u64 = 100; // 每100ms发送一次
    
    let start_time = Instant::now();
    let mut successful_rounds = 0;
    let mut failed_rounds = 0;

    info!("Starting {} second stability test", TEST_DURATION);

    while start_time.elapsed().as_secs() < TEST_DURATION {
        let round_start = Instant::now();
        
        // 发送数据
        if let Err(e) = test_socket.send_to(test_data, TEST_UDP_EXPOSED_ADDR).await {
            error!("Failed to send data: {}", e);
            failed_rounds += 1;
            sleep(Duration::from_millis(SEND_INTERVAL_MS)).await;
            continue;
        }

        // 尝试接收响应
        let mut buf = [0u8; 1024];
        match timeout(Duration::from_secs(2), test_socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if &buf[..n] == test_data {
                    successful_rounds += 1;
                } else {
                    warn!("Received corrupted data");
                    failed_rounds += 1;
                }
            }
            Ok(Err(e)) => {
                error!("UDP recv error: {}", e);
                failed_rounds += 1;
            }
            Err(_) => {
                warn!("Response timeout");
                failed_rounds += 1;
            }
        }

        // 控制发送频率
        let elapsed = round_start.elapsed();
        if elapsed < Duration::from_millis(SEND_INTERVAL_MS) {
            sleep(Duration::from_millis(SEND_INTERVAL_MS) - elapsed).await;
        }
    }

    let total_rounds = successful_rounds + failed_rounds;
    let success_rate = if total_rounds > 0 {
        (successful_rounds as f64 / total_rounds as f64) * 100.0
    } else {
        0.0
    };

    info!(
        "Stability test completed: {}/{} successful ({:.1}%)",
        successful_rounds, total_rounds, success_rate
    );

    // 要求成功率至少95%
    assert!(success_rate >= 95.0, "Success rate too low: {:.1}%", success_rate);
    
    info!("Long-term stability test passed");

    // 清理
    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    server_task.abort();
    
    cleanup_test_configs().await?;
    Ok(())
}

/// 并发测试 - 多个客户端同时发送数据
#[tokio::test]
async fn test_concurrent_clients() -> Result<()> {
    init();
    create_test_configs().await?;

    let server = Arc::new(FlaktyUdpServer::new(TEST_UDP_SERVICE_ADDR).await?);
    let server_clone = server.clone();
    
    let server_task = tokio::spawn(async move {
        server_clone.run().await.unwrap();
    });

    // 启动rathole
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);

    let rathole_server = tokio::spawn(async move {
        common::run_rathole_server(TEST_SERVER_CONFIG, server_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(500)).await;

    let rathole_client = tokio::spawn(async move {
        common::run_rathole_client(TEST_CLIENT_CONFIG, client_shutdown_rx)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(1)).await;

    const NUM_CLIENTS: usize = 5;
    const MESSAGES_PER_CLIENT: usize = 10;
    
    info!("Starting concurrent test with {} clients, {} messages each", NUM_CLIENTS, MESSAGES_PER_CLIENT);

    let mut client_tasks = Vec::new();
    
    for client_id in 0..NUM_CLIENTS {
        let task = tokio::spawn(async move {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut successful = 0;
            
            for msg_id in 0..MESSAGES_PER_CLIENT {
                let test_data = format!("Client-{}-Msg-{}", client_id, msg_id);
                
                if socket.send_to(test_data.as_bytes(), TEST_UDP_EXPOSED_ADDR).await.is_ok() {
                    let mut buf = [0u8; 1024];
                    if let Ok(Ok((n, _))) = timeout(Duration::from_secs(3), socket.recv_from(&mut buf)).await {
                        if &buf[..n] == test_data.as_bytes() {
                            successful += 1;
                        }
                    }
                }
                
                sleep(Duration::from_millis(50)).await;
            }
            
            (client_id, successful)
        });
        
        client_tasks.push(task);
    }

    // 等待所有客户端完成
    let mut total_successful = 0;
    for task in client_tasks {
        let (client_id, successful) = task.await?;
        info!("Client {} completed: {}/{} successful", client_id, successful, MESSAGES_PER_CLIENT);
        total_successful += successful;
    }

    let total_expected = NUM_CLIENTS * MESSAGES_PER_CLIENT;
    let success_rate = (total_successful as f64 / total_expected as f64) * 100.0;
    
    info!("Concurrent test completed: {}/{} successful ({:.1}%)", total_successful, total_expected, success_rate);
    
    // 要求成功率至少90%（并发情况下可能有些竞争）
    assert!(success_rate >= 90.0, "Concurrent success rate too low: {:.1}%", success_rate);
    
    info!("Concurrent clients test passed");

    // 清理
    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    server_task.abort();
    
    cleanup_test_configs().await?;
    Ok(())
}
