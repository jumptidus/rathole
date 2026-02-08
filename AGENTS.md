# AGENTS.md

## 项目概述

`rathole_client` 是 `route_proxy` 使用的客户端库，负责与 `rathole` 服务端建立和维护转发通道。

## 项目依赖关系（跨仓库）

### 当前项目定位

- 位于 `route` 体系内，为 `route_proxy` 提供可复用的连接与会话能力。
- 不承载 UI 或平台壳层逻辑。

### 直接协作关系

- 上游协作：`rathole`（服务端协议与配置语义）。
- 下游依赖：`route_proxy`（业务编排与策略控制）。
- 与 `mobile`、`tis_windows`、`jy_core` 无源码直接依赖。

### 维护规则

- 协议字段与握手流程变更需与 `rathole`、`route_proxy` 同步评估。
- 公共接口尽量保持向后兼容，降低 `route_proxy` 升级风险。
