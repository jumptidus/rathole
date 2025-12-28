# V2 协议兼容说明（服务端）

## 范围
- V2 客户端：`../route_proxy` 的 fd 分支，依赖 `../rathole_client` 的 `run_from_str` 分支（`CURRENT_PROTO_VERSION=PROTO_V2`）。
- 服务端基线：`timeout` 分支。
- 当前分支目标：兼容 V2，显式拒绝 V1，同时保留 V3/mux。

## timeout 分支 V2 服务端基线行为
1) 握手与版本
   - `read_hello` 接受 V2（同时也接受 V1）。
   - 控制通道握手在 V2 下读取 `u64` 小端 `timestamp`。
2) 服务端 Hello 回包
   - 当 `timestamp != 0` 时，回 `ControlChannelHello(PROTO_V2, nonce)`。
3) 控制通道命令
   - 仅 `CreateDataChannel` / `HeartBeat` 两个变体。
   - bincode 编码顺序为 `CreateDataChannel=0`、`HeartBeat=1`。
4) 数据通道
   - `DataChannelHello` 后无额外字节。
   - 使用 `StartForwardTcp` / `StartForwardUdp`。
5) 时间戳冲突处理
   - 若旧连接 `timestamp > 新连接 timestamp`，返回 `Ack::AuthFailed` 并拒绝新连接。

## 当前分支 V2 兼容实现（对照结果）
1) 版本准入
   - 仅接受 V2/V3 hello，V1 明确拒绝。
2) V2 控制通道握手
   - 读取 `u64` 小端 `timestamp`（带超时）。
   - 回包 `ControlChannelHello(PROTO_V2, nonce)`。
3) 控制通道命令编码
   - V2 使用 `ControlChannelCmdV2` 序列化（保持 `0/1` 编码）。
   - V3 使用 `ControlChannelCmd`（包含 `CreateDataMux`）。
4) V2 数据通道
   - 不读取 `DataChannelMode`，默认 `Plain`。
5) 时间戳冲突处理
   - 与 timeout 分支一致，返回 `Ack::AuthFailed`。
6) 额外安全性
   - V3 的 `DataChannelMode` 读取增加超时，避免握手挂起占用并发额度。

## 复核清单
- [x] 仅允许 V2/V3 hello，V1 明确拒绝。
- [x] V2 控制通道命令编码保持 `0/1` 序号。
- [x] V2 数据通道不要求 `DataChannelMode`。
- [x] 时间戳冲突处理与 timeout 分支一致。
- [x] V2 编码测试已覆盖（`test_control_cmd_v2_*`）。
- [x] mux 等待唤醒测试已覆盖（`test_open_stream_waits_until_stream_released`）。

## 注意事项
- V2 客户端必须发送 `timestamp`，否则握手会超时失败。
- V2 下不使用 `Ack::RejectedDueToTimestamp`，保持与 timeout 分支一致。
