use anyhow::{bail, Context, Result};
use rand::Rng;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;
use tracing::{debug, trace, warn};

// 默认探测参数
pub const HEALTH_PROBE_DEFAULT_INTERVAL_SECS: u64 = 20; // 探测间隔
pub const HEALTH_PROBE_DEFAULT_TIMEOUT_SECS: u64 = 5; // 单次探测超时
pub const HEALTH_PROBE_DEFAULT_MAX_FAILURES: u32 = 3; // 最大连续失败次数

// 默认探测目标
pub const HEALTH_PROBE_DEFAULT_TCP_HOSTS: &[&str] = &["www.bing.com", "www.baidu.com"];
pub const HEALTH_PROBE_DEFAULT_DNS_SERVERS: &[&str] = &[
    "8.8.8.8:53",         // Google
    "114.114.114.114:53", // OpenDNS
];
pub const HEALTH_PROBE_DEFAULT_DNS_QUERY: &str = "www.baidu.com";

/// 健康探测配置
#[derive(Clone, Debug)]
pub struct HealthProbeConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub timeout_secs: u64,
    pub max_failures: u32,
    pub tcp_hosts: Vec<String>,
    pub dns_servers: Vec<String>,
    pub dns_query_domain: String,
}

impl Default for HealthProbeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: HEALTH_PROBE_DEFAULT_INTERVAL_SECS,
            timeout_secs: HEALTH_PROBE_DEFAULT_TIMEOUT_SECS,
            max_failures: HEALTH_PROBE_DEFAULT_MAX_FAILURES,
            tcp_hosts: HEALTH_PROBE_DEFAULT_TCP_HOSTS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            dns_servers: HEALTH_PROBE_DEFAULT_DNS_SERVERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            dns_query_domain: HEALTH_PROBE_DEFAULT_DNS_QUERY.to_string(),
        }
    }
}

/// 通过 SOCKS5 CONNECT 发起 HTTP GET，验证 TCP 通道可经由 client 访问互联网
/// bind_addr: server 监听的 TCP 端口（对应某服务的 `bind_addr`），server 自身作为访客连接此地址
/// host: 目标主机名
/// timeout_secs: 超时时间（秒）
pub async fn tcp_socks5_http_probe_once(
    bind_addr: &str,
    host: &str,
    timeout_secs: u64,
) -> Result<()> {
    let mut s = timeout(
        Duration::from_secs(timeout_secs),
        TcpStream::connect(bind_addr),
    )
    .await
    .with_context(|| "TCP 健康探测: 连接超时")??;

    // SOCKS5 方法协商: 版本5, 1种方法, 无认证(0x00)
    let req = [0x05u8, 0x01, 0x00];
    timeout(Duration::from_secs(timeout_secs), s.write_all(&req)).await??;
    let mut resp = [0u8; 2];
    timeout(Duration::from_secs(timeout_secs), s.read_exact(&mut resp)).await??;
    if resp != [0x05, 0x00] {
        bail!("TCP 健康探测: SOCKS5 无认证协商失败: {:?}", resp);
    }

    // SOCKS5 CONNECT 请求: 目标 host:80
    let host_bytes = host.as_bytes();
    let mut buf = Vec::with_capacity(4 + 1 + host_bytes.len() + 2);
    buf.extend_from_slice(&[0x05, 0x01, 0x00, 0x03]); // VER, CMD=CONNECT, RSV, ATYP=DOMAIN
    buf.push(host_bytes.len() as u8);
    buf.extend_from_slice(host_bytes);
    buf.extend_from_slice(&80u16.to_be_bytes());
    timeout(Duration::from_secs(timeout_secs), s.write_all(&buf)).await??;

    // 读取 CONNECT 响应
    let mut head = [0u8; 4];
    timeout(Duration::from_secs(timeout_secs), s.read_exact(&mut head)).await??; // VER, REP, RSV, ATYP
    if head[1] != 0x00 {
        bail!("TCP 健康探测: SOCKS5 CONNECT 失败: REP={:#x}", head[1]);
    }
    // 丢弃 BND.ADDR + BND.PORT
    let addr_len = match head[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut l = [0u8; 1];
            timeout(Duration::from_secs(timeout_secs), s.read_exact(&mut l)).await??;
            l[0] as usize
        }
        _ => bail!("TCP 健康探测: 非法 ATYP"),
    };
    let mut discard = vec![0u8; addr_len + 2];
    timeout(
        Duration::from_secs(timeout_secs),
        s.read_exact(&mut discard),
    )
    .await??;

    // 发送 HTTP GET 请求
    let http = format!(
        "GET / HTTP/1.1\r\nHost: {}\r\nUser-Agent: rathole-health\r\nConnection: close\r\n\r\n",
        host
    );
    timeout(
        Duration::from_secs(timeout_secs),
        s.write_all(http.as_bytes()),
    )
    .await??;

    // 读取响应首行
    let mut buf = [0u8; 256];
    let n = timeout(Duration::from_secs(timeout_secs), s.read(&mut buf)).await??;
    if n == 0 {
        bail!("TCP 健康探测: 无响应体");
    }
    let line = String::from_utf8_lossy(&buf[..n]);
    if !(line.starts_with("HTTP/1.1 2")
        || line.starts_with("HTTP/1.1 3")
        || line.starts_with("HTTP/1.0 2")
        || line.starts_with("HTTP/1.0 3"))
    {
        bail!(
            "TCP 健康探测: HTTP 首行异常: {}",
            line.lines().next().unwrap_or("")
        );
    }
    Ok(())
}

// 构造简单 DNS 查询（A 记录，RD=1）
fn build_dns_query(qname: &str, id: u16) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&id.to_be_bytes()); // ID
    v.extend_from_slice(&0x0100u16.to_be_bytes()); // 标志 RD=1
    v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    for label in qname.split('.') {
        v.push(label.len() as u8);
        v.extend_from_slice(label.as_bytes());
    }
    v.push(0); // 结尾
    v.extend_from_slice(&1u16.to_be_bytes()); // QTYPE=A
    v.extend_from_slice(&1u16.to_be_bytes()); // QCLASS=IN
    v
}

// 构造 SOCKS5 UDP 包（直投模式）。部分实现可能要求先做 UDP ASSOCIATE，此处采用通用直投。
fn build_socks5_udp_packet(dst_ip: [u8; 4], dst_port: u16, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(3 + 1 + 4 + 2 + payload.len());
    v.extend_from_slice(&[0x00, 0x00]); // RSV
    v.push(0x00); // FRAG
    v.push(0x01); // ATYP=IPv4
    v.extend_from_slice(&dst_ip);
    v.extend_from_slice(&dst_port.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

// 解析 SOCKS5 UDP 包，返回内嵌 payload
fn parse_socks5_udp_payload(pkt: &[u8]) -> Option<&[u8]> {
    if pkt.len() < 3 {
        return None;
    }
    let atyp_off = 3;
    let (addr_len, off) = match pkt[atyp_off] {
        0x01 => (4usize, atyp_off + 1),
        0x04 => (16usize, atyp_off + 1),
        0x03 => {
            if pkt.len() < atyp_off + 2 {
                return None;
            }
            let l = pkt[atyp_off + 1] as usize;
            (1 + l, atyp_off + 1) // len 字节 + 域名
        }
        _ => return None,
    };
    let hdr_len = off + addr_len + 2; // +PORT
    if pkt.len() <= hdr_len {
        return None;
    }
    Some(&pkt[hdr_len..])
}

/// 通过 SOCKS5 UDP（直投）发送 DNS 查询并验证响应
/// bind_addr: server 监听的 UDP 端口（对应某服务的 `bind_addr`），server 自身作为访客向此地址发包
/// dns_server: DNS 服务器地址 (如 "1.1.1.1:53")
/// qname: 要查询的域名
/// timeout_secs: 超时时间（秒）
pub async fn udp_socks5_dns_probe_once(
    bind_addr: &str,
    dns_server: &str,
    qname: &str,
    timeout_secs: u64,
) -> Result<()> {
    // 解析 DNS 服务器地址
    let dns_addr: SocketAddr = dns_server
        .parse()
        .with_context(|| format!("无效的 DNS 服务器地址: {}", dns_server))?;

    // 提取 IP 和端口
    let (dns_ip, dns_port) = match dns_addr {
        SocketAddr::V4(addr) => {
            let octets = addr.ip().octets();
            (octets, addr.port())
        }
        SocketAddr::V6(_) => {
            bail!("暂不支持 IPv6 DNS 服务器");
        }
    };

    let s = UdpSocket::bind("0.0.0.0:0").await?;
    let id: u16 = rand::thread_rng().gen();
    let dns = build_dns_query(qname, id);
    let pkt = build_socks5_udp_packet(dns_ip, dns_port, &dns);

    timeout(
        Duration::from_secs(timeout_secs),
        s.send_to(&pkt, bind_addr),
    )
    .await??;

    let mut buf = [0u8; 1500];
    let (n, _) = timeout(Duration::from_secs(timeout_secs), s.recv_from(&mut buf)).await??;
    let payload = parse_socks5_udp_payload(&buf[..n])
        .ok_or_else(|| anyhow::anyhow!("解析 SOCKS5 UDP 响应失败"))?;

    // 打印 UDP 原始缓冲与解析后的载荷（以 HEX 打印，避免二进制导致乱码）

    // 检查 DNS 响应头
    if payload.len() < 12 {
        bail!("DNS 响应过短");
    }
    let rid = u16::from_be_bytes([payload[0], payload[1]]);
    let flags = u16::from_be_bytes([payload[2], payload[3]]);
    let rcode = flags & 0x000F;
    if rid != id {
        bail!("DNS 响应 ID 不匹配");
    }
    if (flags & 0x8000) == 0 {
        bail!("DNS 响应 QR 位异常");
    }
    if rcode != 0 {
        bail!("DNS 响应 RCODE 非 0: {}", rcode);
    }
    Ok(())
}

/// TCP 健康探测器 - 带重试机制和多目标支持
pub async fn tcp_health_probe_with_retry(
    bind_addr: &str,
    config: &HealthProbeConfig,
) -> Result<()> {
    let mut last_error = None;

    // 尝试多个目标主机
    for host in &config.tcp_hosts {
        trace!("尝试 TCP 探测目标: {}", host);

        match tcp_socks5_http_probe_once(bind_addr, host, config.timeout_secs).await {
            Ok(()) => {
                trace!("TCP 探测成功: {}", host);
                return Ok(());
            }
            Err(e) => {
                debug!("TCP 探测失败 {}: {}", host, e);
                last_error = Some(e);
            }
        }
    }

    // 所有目标都失败
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("没有可用的 TCP 探测目标")))
}

/// UDP 健康探测器 - 带重试机制和多 DNS 服务器支持
pub async fn udp_health_probe_with_retry(
    bind_addr: &str,
    config: &HealthProbeConfig,
) -> Result<()> {
    let mut last_error = None;

    // 尝试多个 DNS 服务器
    for dns_server in &config.dns_servers {
        trace!("尝试 UDP 探测 DNS 服务器: {}", dns_server);

        match udp_socks5_dns_probe_once(
            bind_addr,
            dns_server,
            &config.dns_query_domain,
            config.timeout_secs,
        )
        .await
        {
            Ok(()) => {
                trace!("UDP 探测成功: {}", dns_server);
                return Ok(());
            }
            Err(e) => {
                debug!("UDP 探测失败 {}: {}", dns_server, e);
                last_error = Some(e);
            }
        }
    }

    // 所有 DNS 服务器都失败
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("没有可用的 DNS 服务器")))
}

/// 运行持续的健康探测任务
pub async fn run_health_probe_task(
    service_name: String,
    bind_addr: String,
    config: HealthProbeConfig,
    is_tcp: bool,
    on_failure: impl Fn() + Send + 'static,
) {
    if !config.enabled {
        debug!("服务 {} 的健康探测已禁用", service_name);
        return;
    }

    let interval = Duration::from_secs(config.interval_secs);
    let mut consecutive_failures = 0u32;

    loop {
        tokio::time::sleep(interval).await;

        let probe_result = if is_tcp {
            tcp_health_probe_with_retry(&bind_addr, &config).await
        } else {
            udp_health_probe_with_retry(&bind_addr, &config).await
        };

        match probe_result {
            Ok(()) => {
                if consecutive_failures > 0 {
                    debug!(
                        "服务 {} 健康探测恢复正常 (之前失败 {} 次)",
                        service_name, consecutive_failures
                    );
                }
                consecutive_failures = 0;
            }
            Err(e) => {
                consecutive_failures += 1;
                warn!(
                    "服务 {} 健康探测失败 ({}/{}): {}",
                    service_name, consecutive_failures, config.max_failures, e
                );

                if consecutive_failures >= config.max_failures {
                    warn!(
                        "服务 {} 健康探测连续失败 {} 次，触发失败处理",
                        service_name, consecutive_failures
                    );
                    on_failure();
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpStream, UdpSocket};

    // 真实 TCP 访问 bing.com，校验返回 2xx/3xx
    #[tokio::test]
    async fn test_tcp_http_bing_reachable() -> Result<()> {
        let test_host = "www.bing.com";
        let mut s = timeout(
            Duration::from_secs(HEALTH_PROBE_DEFAULT_TIMEOUT_SECS),
            TcpStream::connect((test_host, 80)),
        )
        .await
        .with_context(|| "TCP 健康探测: 连接超时")??;

        let http = format!(
            "GET / HTTP/1.1\r\nHost: {}\r\nUser-Agent: rathole-health\r\nConnection: close\r\n\r\n",
            test_host
        );
        timeout(
            Duration::from_secs(HEALTH_PROBE_DEFAULT_TIMEOUT_SECS),
            s.write_all(http.as_bytes()),
        )
        .await??;

        let mut buf = [0u8; 256];
        let n = timeout(
            Duration::from_secs(HEALTH_PROBE_DEFAULT_TIMEOUT_SECS),
            s.read(&mut buf),
        )
        .await??;
        if n == 0 {
            bail!("TCP 健康探测: 无响应体");
        }
        let line = String::from_utf8_lossy(&buf[..n]);
        assert!(
            line.starts_with("HTTP/1.1 2")
                || line.starts_with("HTTP/1.1 3")
                || line.starts_with("HTTP/1.0 2")
                || line.starts_with("HTTP/1.0 3"),
            "HTTP 首行异常: {}",
            line.lines().next().unwrap_or("")
        );
        Ok(())
    }

    // 真实 UDP 向 1.0.0.1 发送 DNS 查询，校验 QR=1 且 RCODE=0
    #[tokio::test]
    async fn test_udp_dns_cloudflare_reachable() -> Result<()> {
        let s = UdpSocket::bind("0.0.0.0:0").await?;
        let id: u16 = rand::thread_rng().gen();
        let dns = build_dns_query("example.com", id);
        timeout(
            Duration::from_secs(HEALTH_PROBE_DEFAULT_TIMEOUT_SECS + 4),
            s.send_to(&dns, ("114.114.114.114", 53)),
        )
        .await??;

        let mut buf = [0u8; 1500];
        let (n, _peer) = timeout(
            Duration::from_secs(HEALTH_PROBE_DEFAULT_TIMEOUT_SECS + 4),
            s.recv_from(&mut buf),
        )
        .await??;
        if n < 12 {
            bail!("DNS 响应过短");
        }
        let rid = u16::from_be_bytes([buf[0], buf[1]]);
        let flags = u16::from_be_bytes([buf[2], buf[3]]);
        let rcode = flags & 0x000F;
        assert_eq!(rid, id, "DNS 响应 ID 不匹配");
        assert_ne!(flags & 0x8000, 0, "DNS 响应 QR 位异常");
        assert_eq!(rcode, 0, "DNS 响应 RCODE 非 0: {}", rcode);
        Ok(())
    }
}
