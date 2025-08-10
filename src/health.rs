use anyhow::{bail, Context, Result};
use rand::Rng;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;

// 默认探测参数，可在调用端覆写
pub const HEALTH_PROBE_DEFAULT_INTERVAL_SECS: u64 = 30; // 探测间隔
pub const HEALTH_PROBE_DEFAULT_TIMEOUT_SECS: u64 = 6; // 单次探测超时
pub const HEALTH_PROBE_DEFAULT_HOST: &str = "bing.com";
pub const HEALTH_PROBE_DEFAULT_URL: &str = "http://bing.com/";

/// 通过 SOCKS5 CONNECT 发起 HTTP GET，验证 TCP 通道可经由 client 访问互联网
/// bind_addr: server 监听的 TCP 端口（对应某服务的 `bind_addr`），server 自身作为访客连接此地址
pub async fn tcp_socks5_http_probe_once(
    bind_addr: &str,
    host: &str,
    url: &str,
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

    // 发送 HTTP GET（绝对路径，兼容代理）
    let http = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: rathole-health\r\nConnection: close\r\n\r\n",
        url, host
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
pub async fn udp_socks5_dns_probe_once(
    bind_addr: &str,
    dns_ip: [u8; 4],
    dns_port: u16,
    qname: &str,
    timeout_secs: u64,
) -> Result<()> {
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
