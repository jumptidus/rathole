# 健康探测功能设计文档

## 概述

健康探测功能用于监控 client 端的 SOCKS5 代理服务是否正常工作。Server 端会定期通过已建立的隧道连接到 client 端的 SOCKS5 服务，验证其能否正常转发网络请求。

## 架构设计

### 工作原理

1. **Client 端配置**：
   - TCP 服务：Client 端运行 SOCKS5 TCP 代理服务
   - UDP 服务：Client 端运行修改过的 SOCKS5 UDP 转发服务

2. **Server 端探测**：
   - 定期连接到本地监听的端口（bind_addr）
   - 通过隧道访问 client 端的 SOCKS5 服务
   - 验证代理服务是否能成功转发请求

### 探测策略

#### TCP 探测
- 通过 SOCKS5 CONNECT 方法连接到外部 HTTP 服务器
- 发送 HTTP GET 请求并验证响应
- 支持多个目标主机，任意一个成功即认为探测成功

#### UDP 探测  
- 通过 SOCKS5 UDP 直投模式发送 DNS 查询
- 验证 DNS 响应的有效性
- 支持多个 DNS 服务器，任意一个成功即认为探测成功

## 优化改进

### 1. 配置化支持

```rust
pub struct HealthProbeConfig {
    pub enabled: bool,              // 是否启用探测
    pub interval_secs: u64,          // 探测间隔（秒）
    pub timeout_secs: u64,           // 单次探测超时（秒）
    pub max_failures: u32,           // 最大连续失败次数
    pub tcp_hosts: Vec<String>,      // TCP 探测目标
    pub dns_servers: Vec<String>,    // DNS 服务器列表
    pub dns_query_domain: String,    // DNS 查询域名
}
```

### 2. 重试机制

- **连续失败计数**：只有连续失败达到阈值才触发断线
- **多目标支持**：尝试多个探测目标，提高可靠性
- **失败恢复**：探测恢复正常时记录日志

### 3. 探测目标

**默认 TCP 目标**：
- www.google.com
- www.cloudflare.com  
- www.bing.com

**默认 DNS 服务器**：
- 1.1.1.1:53 (Cloudflare)
- 8.8.8.8:53 (Google)
- 208.67.222.222:53 (OpenDNS)

### 4. 日志优化

- **trace 级别**：成功的探测详情
- **debug 级别**：探测失败的详细信息
- **warn 级别**：连续失败警告
- **error 级别**：达到失败阈值，触发断线

## 使用示例

### 基本使用

```rust
// 使用默认配置
let config = HealthProbeConfig::default();

// 自定义配置
let config = HealthProbeConfig {
    enabled: true,
    interval_secs: 30,
    timeout_secs: 6,
    max_failures: 3,
    tcp_hosts: vec!["www.example.com".to_string()],
    dns_servers: vec!["1.1.1.1:53".to_string()],
    dns_query_domain: "example.com".to_string(),
};
```

### 运行探测任务

```rust
run_health_probe_task(
    service_name,
    bind_addr,
    config,
    is_tcp,
    || {
        // 失败处理逻辑
        println!("健康探测失败，断开连接");
    }
).await;
```

## 测试

### 单元测试

```bash
# 运行健康探测测试
cargo test health --lib -- --nocapture
```

### 测试覆盖

1. **TCP 探测测试**：直接连接到 bing.com 验证 HTTP 响应
2. **UDP 探测测试**：向 DNS 服务器发送查询验证响应
3. **SOCKS5 协议测试**：验证 SOCKS5 握手和数据转发

## 未来改进

1. **配置文件支持**：允许在 TOML 配置文件中设置探测参数
2. **指标收集**：记录探测成功率、延迟等指标
3. **自适应间隔**：根据网络状况动态调整探测间隔
4. **更多协议支持**：支持 HTTPS、SOCKS5 认证等
5. **健康状态 API**：提供 HTTP API 查询服务健康状态

## 注意事项

1. **网络环境**：某些网络环境可能阻止访问特定的探测目标
2. **性能影响**：频繁的探测可能影响性能，需要合理设置间隔
3. **防火墙规则**：确保防火墙允许探测流量通过
4. **日志级别**：生产环境建议使用 warn 级别以上，避免过多日志
