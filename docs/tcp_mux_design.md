# rathole V3 TCP 多路复用设计（方案 A + 固定小池）

## 背景与问题

当前 TCP 转发采用“访客连接 = 一条数据通道”模型，服务端在接入高并发时会快速消耗 FD。`route_proxy` 已有配额与隔离方案，但数据通道 FD 仍会成为硬瓶颈。

目标是引入 TCP 多路复用，显著降低“服务端⇄客户端”之间的数据通道 FD 数量，并减少握手与连接建立成本。

## 目标

- 新协议（V3）支持 TCP 数据通道多路复用。
- 新客户端只连接新服务端；老客户端可能连接新服务端，必须保持可用。
- 采用固定小池：每个服务维持少量 data-mux 连接（默认 4）。
- 复用只针对 TCP；UDP 维持原有单通道模型。
- 当前仅覆盖 TCP + Noise + UDP 传输通道，不引入 TLS/WebSocket 等额外复杂性。

## 非目标

- 不要求 V1/V2 兼容多路复用。
- 不支持跨服务的全局单 mux。
- 不在本阶段引入 QoS 或复杂的流量整形。

## 现状摘要

- 控制通道为 per-service 的单一连接，用于下发 CreateDataChannel 和心跳。
- 每个访客 TCP 连接触发 CreateDataChannel，客户端再建立一条数据通道。
- FD 成本随并发线性增长，且 TLS/Noise 握手重复。

## 方案 A：每服务固定小池 data-mux

### 核心思路

- 保留控制通道。
- V3 下，服务端在控制通道上请求 **data-mux** 连接，客户端按请求建立 TCP 连接。
- 每个 data-mux 连接使用 `yamux`，服务端为每个访客连接打开一个新 stream。
- 每个服务默认维持 `mux_pool_size = 4` 条 data-mux。

### 数据通道角色

- TCP：走 yamux stream。
- UDP：继续使用独立 UDP data channel（不复用）。

## 协议升级（V3）

### 版本策略

- 服务端仅接受 V2 / V3（明确拒绝 V1）。
- V3 客户端只连接 V3 服务端，且默认启用多路复用。
- V2 客户端连接新服务端时走原有数据通道逻辑。
- 服务端返回的 Hello 版本必须与客户端一致，否则客户端会直接报错并断开。

### 帧长度与兼容性约束

- 当前实现 `read_hello` 使用固定长度读取（由 `CURRENT_PROTO_VERSION` 计算），因此 **V3 的 Hello 长度必须与 V2 保持一致**。
- **不要**把新增字段合并进 Hello 结构体，否则会改变长度导致 V2/V3 互连时阻塞或反序列化失败。
- 新增字段应当放在 Hello 之后，按版本条件读取（不会引入额外 RTT，仅增加一次 read）。
- V3 仍允许 `DataChannelMode::Plain`，但仅用于 UDP 数据通道；TCP 走 mux。

### 控制通道握手（V3）

```text
Client -> Server: Hello(ControlChannelHello, v3, service_digest)
Client -> Server: timestamp_u64_le
Server -> Client: Hello(ControlChannelHello, v3, nonce)
Client -> Server: Auth(session_key)
Server -> Client: Ack(Ok)
Server -> Client: ControlChannelCmd::CreateDataMux
```

### 控制通道握手（V2）

```text
Client -> Server: Hello(ControlChannelHello, v2, service_digest)
Client -> Server: timestamp_u64_le
Server -> Client: Hello(ControlChannelHello, v2, nonce)
Client -> Server: Auth(session_key)
Server -> Client: Ack(Ok)
```

说明：
- V2 控制通道命令仅包含 `CreateDataChannel` 与 `HeartBeat`，不发送 `CreateDataMux`。
- 若服务端发现旧连接 `timestamp > 新连接 timestamp`，返回 `Ack::AuthFailed` 并拒绝新连接。

### data-mux 握手（V3）

```text
Client -> Server: Hello(DataChannelHello, v3, session_key)
Client -> Server: DataChannelMode::Mux
<yamux handshake>
```

### 数据通道握手（V2）

```text
Client -> Server: Hello(DataChannelHello, v2, session_key)
Server -> Client: DataChannelCmd::StartForwardTcp | DataChannelCmd::StartForwardUdp
```

说明：
- V2 不发送 `DataChannelMode`。

### 数据流初始化

```text
Server opens new yamux stream
Server -> Client (stream): DataChannelCmd::StartForwardTcp
Server <-> Client: copy_bidirectional(visitor, stream)
```

### 新增协议元素

```text
enum ControlChannelCmd {
  CreateDataChannel,
  CreateDataMux,
  HeartBeat,
}

enum DataChannelMode {
  Plain = 0,
  Mux = 1,
}
```

### 兼容策略

- V2 不发送 DataChannelMode，按原逻辑处理。
- V3 DataChannelHello 后必须读取 DataChannelMode。
- 新增 opcode 只在 V3 路径发送；控制通道命令在 V2/V3 需要明确区分编码，避免旧客户端误解。
- 控制通道 Ack 若新增变体（如 `RejectedDueToTimestamp`），客户端枚举必须同步新增，否则 bincode 反序列化会失败。
- V3/TCP 若收到 `DataChannelMode::Plain`，服务端应直接拒绝并关闭连接。

### DataChannelMode 序列化格式

- 采用 **1 字节 opcode**（u8）直接写入，不使用 bincode。
- 读写时机：`DataChannelHello` 之后立即读写 1 字节（仅 V3）。
- 取值：`0 = Plain`，`1 = Mux`。

## 固定小池设计

### 基本策略

- 每个 TCP 服务维持 `mux_pool_size` 条 data-mux（默认 4）。
- 若 data-mux 断线或不可用，服务端通过控制通道请求补齐。
- 如果暂时没有可用 data-mux，访客连接按 `data_channel_wait_timeout` 等待，否则快速拒绝。

### 负载分配

- 选择策略：`least_streams` 或 `round_robin`，默认 `least_streams`。
- 如果目标 data-mux 失败，尝试下一条；若全失败，拒绝访客。
- `least_streams` 统计需在线程安全结构上更新，并确保 stream 关闭时及时回收，否则会出现负载倾斜。

### 失败恢复

- data-mux 断线时：
  - 标记为不可用并从池中移除。
  - 立即请求 `CreateDataMux` 以补齐池大小。
- 建议对频繁失败进行指数退避，避免抖动风暴（服务端与客户端均需实现）。

### Mux 池补齐策略

- 断线即触发：data-mux 断线瞬间立刻请求补齐。
- 惰性补齐：若断线后短时间无法重连，在新访客到来时再次触发补齐（避免无流量场景无意义重连）。
- 退避实现建议统一为 **backon**（维护更活跃），支持 jitter 与最大退避。
- 客户端侧也需要退避：控制通道反复下发 `CreateDataMux` 时，客户端应按 backon 节流，避免重连风暴。

### 安全与 session_key 复用

- V3 data-mux 仍复用 `session_key`，等价于“同一服务的认证令牌”。
- 风险：若 session_key 泄露，攻击者可尝试建立多条 mux。
- 对策：服务端增加 `mux_pool_size` 上限与握手速率限制；客户端设置 `mux_max_pool` 兜底。

### 流量隔离与公平性

- 多 stream 共享同一 TCP 连接，默认由 yamux 的窗口控制流量。
- 建议：
  - 保留 `mux_max_streams` 限制并发。
  - 对单 stream 增加空闲超时（见下节），防止慢消费者长期占用窗口。
  - 需要更强隔离时，可引入 per-stream 速率限制或背压策略。

### 空闲超时

- stream 空闲超时：默认沿用 `TCP_IDLE_TIMEOUT`（例如 360s），超时则关闭该 stream。
- mux 连接空闲超时：当 mux 下无活跃 stream 且超过 `mux_idle_timeout` 时关闭连接并允许重建。
- yamux keepalive 建议开启，避免中间设备切断空闲连接。

### 异常关闭顺序与 EOF 语义

- 服务端主动关闭 stream 时，应 **先 `shutdown(Write)` 再关闭读半边**，保证对端能尽快读到 EOF。
- 访客连接断开时，应及时关闭对应 stream，避免客户端侧读写任务悬挂。
- 若使用 `copy_bidirectional`，应确保在任一方向返回错误/EOF 时主动关闭另一侧写端。

### 传输层差异与超时

- 在 TCP/Noise 传输下，多路复用会放大握手与重连的成本，应区分统计“握手失败”与“stream 级失败”。
- 若未来启用 TLS/WebSocket，应单独评估握手与重连成本。
- 建议对 data-mux 连接建立设置超时，并在日志中区分：连接失败 / 握手失败 / yamux 建立失败。

### 控制通道与 mux 生命周期绑定

- 控制通道断开时应主动关闭该服务所有 mux 连接，避免“无控制通道但数据仍在跑”的漂移状态。
- 建议由控制通道的 shutdown 信号统一触发 mux 池清理。

### 接入背压与超时拒绝

- 访客接入速度远高于 mux 可用 stream 时，应转化为“等待 stream / 超时拒绝”的策略。
- 超时拒绝必须打点与日志区分（等待超时 vs 无 mux vs 限流拒绝），便于排障与调参。

### 连接上限

- stream 级限流：以“服务总流数”为上限，而非“data-mux 数”。
- 建议复用现有 limiter 机制，将 `DataChannelLimiter` 语义迁移为 `StreamLimiter`。

## 资源与 FD 估算

- 传统模型：每个 TCP 转发需要 1 条 data channel + 1 本地连接。
- Mux 模型：
  - 每服务固定 `mux_pool_size` 条 data-mux 连接。
  - 每个转发只需要 1 本地连接。

建议调整 FD 成本模型：

```text
fd_cost_per_tcp_flow = 1
fd_cost_per_service = mux_pool_size
```

### UDP 配额的顺带影响

- TCP 成本下降会释放 FD 预算，可能导致 UDP 预算上升。
- 这是正向改进，但建议 **额外保留部分 FD 给控制面**（连接、监控、健康探测），避免 UDP 反向挤占。

## 配置建议

### 服务端（建议新增）

- `server.services.<name>.mux_pool_size`：每服务 data-mux 数量（默认 4）
- `server.services.<name>.mux_max_streams`：每个 data-mux 的最大流数（默认 256）
- `server.services.<name>.mux_select`：`least_streams` 或 `round_robin`
- `server.services.<name>.mux_idle_timeout`：mux 空闲超时（默认 300s）
- `server.services.<name>.stream_idle_timeout`：单 stream 空闲超时（默认 360s）
- `server.services.<name>.yamux`：yamux 配置（详见下节）

### 客户端（建议新增）

- `client.services.<name>.mux_max_pool`：客户端侧最大并发 data-mux 数（防止被误配置拉爆）

### Yamux 配置与推荐默认值

```text
max_connection_receive_window = 1GiB (默认)
max_num_streams = 512 (默认，服务端会按 mux_max_streams 覆盖)
read_after_close = true
split_send_size = 16KiB
```

说明：
- 当前实现仅显式设置 `max_num_streams`，其余沿用 yamux 默认值。
- 若需收敛内存占用，可考虑暴露 `max_connection_receive_window` 为配置项。
- keepalive 依赖传输层 TCP keepalive 或应用层心跳，不由 yamux 直接提供。

## 关键实现点（概要）

- 服务端：
- V3 控制通道发送 `CreateDataMux`；V2 仅发送 `CreateDataChannel`。
  - data-mux 握手后建立 yamux session，并注册到服务的 mux 池。
  - 访客连接到来时打开新 stream 并写 `StartForwardTcp`。
  - stream 空闲超时与 mux 空闲超时均需落地。
  - data-mux 握手并发需受控（可复用 `max_inflight_handshakes` 或单独限额）。
  - 退避策略统一使用 backon（替代 backoff），并在日志中打印退避阶段。
- 客户端：
  - 收到 `CreateDataMux` 时创建 data-mux 连接并启动 yamux 监听。
  - 每个新 stream 读取 `DataChannelCmd`，转发到本地服务。
  - 支持 `DataChannelMode::Plain` 以兼容回滚路径。
  - 控制通道触发频繁时按 backon 退避，避免连接风暴。

## 稳定性与风险

- 风险：单 data-mux 仍存在 TCP HOL。
- 对策：固定小池（默认 4），并允许按服务调大。
- 风险：data-mux 断线导致短时不可用。
- 对策：快速补齐 + 保留至少 1 条可用连接。
- 风险：stream 级异常难定位。
- 对策：区分日志/指标，区分 mux 连接级与 stream 级错误。

## 测试与验证

### 功能测试

- V3 正常：单服务 TCP 转发、并发连接、长连接。
- 兼容性：V2 客户端连接新服务端仍可用。
- UDP：保持原行为不变。

### 稳定性测试

- data-mux 断线恢复：池能自动补齐。
- 压力测试：FD 下降与吞吐稳定性对比。

### 观测指标

- `mux_pool_size` / `mux_available` / `mux_streams_total`
- `mux_reconnect_count` / `mux_open_stream_fail`
- `tcp_forward_success` / `tcp_forward_reject`
- `mux_goaway_count` / `stream_reset_count`

### 观测落地方式

- 默认使用 `tracing` 结构化日志输出关键字段（service、session_key、mux_id、stream_id）。
- 若接入 metrics，可将上述指标映射到 `counter`/`gauge`。

## 回滚与灰度

- V3 客户端默认启用 mux，不提供回滚开关。

## 推进计划（建议）

1. 协议层 V3 + data-mux 握手
2. 服务端 mux 池与 stream 转发
3. 客户端 mux 监听与 stream 转发
4. route_proxy 配额模型调整与限流迁移
5. 压测与回归
