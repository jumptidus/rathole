# TCP Mux 行动计划（基于方案 A + 固定小池）

本文档将设计方案拆解为可执行步骤，列出每一步的实现范围、关键关注点与验证方式。默认实现不覆盖配置热更新。

## 总体约束

- 保持 V2 兼容：新服务端必须接收老客户端，显式拒绝 V1。
- V3 才启用 Mux，且保持 Hello 长度不变。
- DataChannelMode 使用 1 字节 opcode（非 bincode）。
- UDP 仍沿用原有模型，不走 Mux。
- 退避统一使用 backon（服务端 + 客户端）。
- 客户端仅使用 MQTT 下发配置，默认开启并受控，不考虑手工配置路径。
- 当前线上仅使用 TCP + Noise + UDP 传输通道，其他 transport 暂不纳入范围，避免引入额外复杂性。

## Step 0：依赖与基础设施

**实现范围**
- 增加依赖：`rust-yamux`、`backon`（rathole + rathole_client）。
- 统一错误类型与日志字段，预留 `mux_id` / `stream_id` / `service`。

**关注点**
- 版本锁定与 feature 选择，避免与现有 tokio/bytes 冲突。
- backon 与现有 backoff 的替换范围，避免混用。

**验证**
- 能成功编译；基础连接不回归。

## Step 1：协议层 V3 扩展

**实现范围**
- 协议常量：新增 `PROTO_V3`。
- 新增 `ControlChannelCmd::CreateDataMux`（仅 V3 发送）。
- 新增 `DataChannelMode`（Plain/Mux）。
- V3 DataChannelHello 后读取 1 字节 mode；V1/V2 不读。

**关注点**
- Hello 长度保持不变，不能把 mode 合并进 Hello。
- 旧客户端永远不应收到 CreateDataMux。
- 新 opcode 追加在枚举末尾，或使用 V3 专用枚举。

**验证**
- V2 client <-> 新 server 仍可握手。
- V3 client <-> 新 server 走 Mux。

## Step 2：服务端 MuxPool 与控制通道扩展

**实现范围**
- 每服务维护 `MuxPool`：`mux_pool_size`（默认 4）、`mux_max_streams`、`mux_select`。
- 控制通道：V3 发送 `CreateDataMux`；V2 仅发送 `CreateDataChannel`。
- data-mux 握手成功后创建 yamux session，注册到 MuxPool。
- 控制通道关闭时清理该服务所有 mux。

**关注点**
- MuxPool 状态一致性（并发下的 stream 计数回收）。
- data-mux 握手并发限制（复用 `max_inflight_handshakes` 或独立 semaphore）。
- backon 退避策略与日志标记阶段（初次/重试/上限）。
- 资源回收需有超时兜底，避免 mux/stream 悬挂导致泄漏。

**验证**
- mux 断线后池补齐；断线时 stream 被正确回收。

## Step 3：客户端 Mux 接入与监听

**实现范围**
- 处理 `CreateDataMux`：建立 data-mux 连接，发送 DataChannelMode::Mux。
- 启动 yamux 接收循环，对每个新 stream 读取 `DataChannelCmd`。
- 按 backon 对重试进行节流。

**关注点**
- 反复 CreateDataMux 的重连风暴控制（backon + 上限）。
- stream 关闭顺序与 EOF 语义（先 shutdown write）。
- 明确不支持 UDP 走 mux，收到 UDP cmd 需报错并关闭 stream。

**验证**
- mux 建立后可以稳定接收多 stream。
- 服务端断线时客户端可恢复。

## Step 4：TCP 转发流程改造（服务端）

**实现范围**
- `run_tcp_connection_pool` 改为从 MuxPool `open_stream()`。
- 等待策略从“等 data channel”改为“等 stream”，超时拒绝。
- stream 创建后写 `StartForwardTcp` 并 `copy_bidirectional`。

**关注点**
- 等待超时分类：无 mux / 限流拒绝 / open_stream 失败。
- backpressure 与超时协同：读写活动更新避免误判空闲。
- stream 关闭与资源回收需设置超时兜底，确保异常路径可释放。

**验证**
- 大量并发下 stream 分配均衡。
- 超时拒绝有明确日志/指标。

## Step 5：限流与 FD 配额调整

**实现范围**
- 将 `DataChannelLimiter` 语义迁移为 `StreamLimiter`。
- route_proxy 中 TCP FD 成本从 2 调整为 1。
- 控制面 FD 预留增加，避免 UDP 预算过度膨胀。

**关注点**
- StreamLimiter 计数准确，关闭时回收。
- 控制面预留比例需可配置或固定策略明确。

**验证**
- 限流触发位置符合预期（stream 级）。
- FD 压力测试吞吐明显提升。

## Step 6：配置与默认值

**实现范围**
- 服务端：`mux_pool_size`、`mux_max_streams`、`mux_select`、`mux_idle_timeout`、`stream_idle_timeout`、`yamux`。
- 客户端：`mux_max_pool`。
- 配置校验与默认值落地（不做热更新）。

**关注点**
- 含 `enable_mux` 的旧配置需要删除该字段。

**验证**
- 删除 `enable_mux` 后可正常启动。

## Step 7：观测与诊断

**实现范围**
- 结构化日志：mux_id、stream_id、service、原因码。
- 关键指标：mux_pool_size、mux_available、mux_reconnect、stream_reset、open_stream_fail。

**关注点**
- 避免高基数（session_key 脱敏/不打指标）。
- 错误分类：连接失败 / 握手失败 / yamux 失败 / stream 失败。

**验证**
- 日志可用于定位 mux 级 vs stream 级故障。

## Step 8：测试与回归

**实现范围**
- 协议测试：V2/V3 互通、DataChannelMode 解析。
- 功能测试：单服务、多并发、长连接。
- 失效测试：mux 断线、重连、超时拒绝。
- 传输矩阵测试：仅覆盖当前开启的 transport feature（TCP/TLS/Noise/WebSocket 的子集）。
  - 当前仅验证 TCP + Noise（UDP 维持原回归用例）。
- 详细测试矩阵与步骤见 `docs/tcp_mux_test_plan.md`。
- 推荐使用 `tools/loadgen` 的 `--scenario` 场景进行集成测试跑批。

**关注点**
- 与现有 TCP/UDP 回归测试共存。
- 压测场景关注 FD 与吞吐变化。
- 传输矩阵测试优先级：TCP > Noise。

**验证**
- 保证 V2 客户端可用；V3 Mux 性能改善。

## Step 9：灰度与回滚

**实现范围**
- 不提供 enable_mux 回滚；灰度通过新旧服务端隔离完成。

## 交付物清单

- 代码：rathole、rathole_client、route_proxy（配额调整）
- 文档：本行动计划、协议/配置说明更新
- 测试：协议与稳定性用例
