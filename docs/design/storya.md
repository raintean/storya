# Storya 总体设计

Storya 是一个 Rust 和 TypeScript 混合的视频解决方案 monorepo。仓库统一管理工具链、依赖、跨语言协议和可复用组件，但每个应用、示例、服务和服务端 Cloudflare Worker 保持独立的运行与部署边界。

本文描述当前已经采用的仓库级设计。尚未实现的业务能力会明确标注，不把目录骨架写成已经完成的产品功能。

## 系统边界

Storya 当前包含以下运行单元与共享能力:

```text
用户界面      storya-web
管理界面      storya-admin
交互示例      storya-hls-loader-react

中心服务      storya-api
Cloudflare Worker  storya-websocket-transport

共享能力      storya-player      storya-protocol      storya-hls-loader      storya-transport
              播放器组件          跨语言协议            HLS 并行加载           HTTP Transport
```

- `storya-web` 面向最终用户，负责视频浏览、播放和用户交互。当前仅包含基础页面骨架，并已经使用 `storya-player`。
- `storya-admin` 面向运营和管理人员。当前仅包含基础页面骨架。
- `storya-hls-loader-react` 是 HLS 并行加载器的 React 交互示例，保留在 `examples/`，通过 Cloudflare Workers Static Assets 独立部署。页面使用版本化 localStorage 自动保存 HLS 地址、Loader/Transport 模式、Worker 地址和并行加载参数；首次访问不内置播放地址，播放进度、清晰度、日志和诊断等会话状态不持久化。
- `storya-api` 是中心 API 服务，负责未来的核心业务接口。当前只实现 `/health`。
- `storya-websocket-transport` 是部署在 Cloudflare Workers 上的 WebSocket Transport 单元。当前实现 `/health` 和 `/transport`，通过流式 HTTP-over-WebSocket 协议透明转发 GET/HEAD。
- `storya-player` 是框架无关的 Web Component。当前封装原生 `<video>` 元素及基础属性同步，尚未实现自适应码流、DRM、字幕或播放遥测。
- `storya-protocol` 是 Rust 和 TypeScript 共用的协议包。当前包含健康检查和 HTTP relay 控制消息的 Protobuf，以及 relay frame header codec；健康检查类型尚未接入 `storya-api` 的 HTTP 路由。
- `storya-hls-loader` 是基于 hls.js 自定义 StreamController 和 fLoader 的并行加载包。`ParallelStreamController` 规划有序 Segment 窗口, `ParallelSegmentLoader` 持有多 VirtualStream 状态, 通过内部 Worker 和 `storya-transport` 完成 Segment 内 Range Chunk 与跨 Segment 并发, 并让预加载与 hls.js 正式读取共享数据; 默认网络实现为 Fetch。
- `storya-transport` 提供通用 HTTP Transport 接口、原生 Fetch 和基于连接池的串行复用 WebSocket 实现。

目标业务关系是:

```text
storya-web ---------> storya-api
      |
      +-------------> storya-player
                           |
                           +----> storya-transport
                                      |
                                      +----> HTTP 源站
                                      |
                                      +----> storya-websocket-transport ----> HTTP 源站

storya-admin -------> storya-api

storya-protocol ----> 为需要跨进程或跨语言通信的边界提供 Schema
```

这张图表达已采用的职责方向，不表示所有调用链都已由产品界面启用。Transport、WebSocket Transport Worker 和源站转发已经实现，Web 前台尚未接入该播放链路。

## 仓库结构

```text
storya/
├── apps/
│   ├── storya-web/
│   └── storya-admin/
├── examples/
│   └── storya-hls-loader-react/
├── services/
│   └── storya-api/
├── workers/
│   └── storya-websocket-transport/
├── packages/
│   ├── storya-player/
│   ├── storya-hls-loader/
│   ├── storya-protocol/
│   └── storya-transport/
└── docs/
    ├── design/
    └── memory/
```

五类工程目录的边界如下:

| 目录       | 责任                                            | 运行方式                   |
| ---------- | ----------------------------------------------- | -------------------------- |
| `apps`     | 面向用户或管理人员的完整应用                    | 由人直接访问或运行         |
| `examples` | 演示和验证共享能力的交互示例                    | 本地运行或作为静态站点部署 |
| `services` | 独立部署并持续运行的通用服务                    | 由服务端运行环境运行       |
| `workers`  | 以服务端 Cloudflare Worker 能力为主要职责的单元 | 由 Cloudflare Workers 运行 |
| `packages` | 编译期复用的组件和协议                          | 不单独作为产品部署         |

所有叶子项目都使用 `storya-` 前缀、小写字母和 kebab-case。npm 包不使用 scope，也不发布到公共 registry。

顶层目录按项目主要职责划分，不按部署平台划分。应用和示例即使通过 Workers Static Assets 部署，也继续保留在 `apps/` 或 `examples/`; `workers/` 只容纳本身以服务端 Worker 能力为主要职责的项目。

## 依赖方向

依赖只能从具体运行单元指向共享包:

```text
apps -----+
examples -+
services -+----> packages
workers --+
```

- `packages` 不依赖具体 `apps`、`examples`、`services` 或 `workers`。
- 一个实现只有在出现真实消费者和稳定边界后才移动到 `packages`。
- 不因为多个项目暂时存在相似代码就提前建立 `storya-ui`、`storya-sdk` 或其他公共包。
- `storya-player` 保持框架无关，具体页面状态和产品交互留在对应应用。
- `storya-protocol` 只描述跨边界数据，不承载业务流程或服务实现。
- `storya-hls-loader` 依赖 `storya-transport`, Transport 不反向依赖 HLS 或媒体类型。

当前已经存在 `storya-web -> storya-player`、`storya-hls-loader-react -> storya-hls-loader -> storya-transport -> storya-protocol` 和 `storya-websocket-transport -> storya-protocol`。其他图中关系是已经采用但尚待实现的方向。

## Protocol

跨 Rust/TypeScript 的传输数据使用 Protobuf 描述，由 Buf 管理格式、lint、breaking check 和代码生成。仅 TypeScript 消费且性能敏感的协议可以在设计文档明确记录后使用手写二进制 codec。HTTP-over-WebSocket relay 的控制消息使用 Protobuf；媒体 body 不进入 Protobuf，只使用固定 frame header 加原始 payload。

```text
packages/storya-protocol/
├── proto/
│   ├── service/
│   │   └── health.proto
│   └── transport/
│       └── http.proto
├── typescript/
│   └── transport-frame.ts
├── generated/rust/
└── typescript/generated/
```

协议约定:

- Proto 文件按领域直接放在 `proto/<domain>/`。
- 文件系统不增加重复的 `storya/` 目录层。
- Protobuf package 使用 `storya.<domain>`，例如 `storya.service`。
- 不建立预防性的 `v1`、`v2` 目录或 namespace。真正发生不兼容迁移时，根据实际兼容策略处理。
- Protocol 和 Worker 生成代码由 `make generate` 统一更新，不手工编辑，也不纳入版本管理。
- 手写 frame codec 必须集中在 Protocol 包中，由通信双方复用同一实现，并在对应设计文档中记录 wire format 和边界。
- 只在进程内部使用的 TypeScript 或 Rust 类型留在所属项目，不进入 Protocol。

Buf 当前禁用 `PACKAGE_DIRECTORY_MATCH` 和 `PACKAGE_VERSION_SUFFIX`，分别用于支持精简目录层级和不使用版本后缀的约定。

## Workspace 与工具链

- Rust 使用根 Cargo workspace，所有第三方依赖版本集中在根 `Cargo.toml`。
- TypeScript 使用 pnpm workspace，公共依赖版本集中在 `pnpm-workspace.yaml` catalog。
- Rust 工具链和 rustfmt 配置与 Pantheon 保持一致。
- Oxfmt 和 Oxlint 只安装在根 Node 包，覆盖整个 JavaScript/TypeScript 工作区。
- Makefile 是跨语言统一入口；`package.json` 只处理 Node workspace 内部任务。

主要任务:

| 命令                | 用途                                     |
| ------------------- | ---------------------------------------- |
| `make pnpm-install` | 安装 pnpm workspace 依赖                 |
| `make build`        | 构建 Rust release 和所有 TypeScript 项目 |
| `make build-linux`  | 使用 cross 构建 Linux musl Rust release  |
| `make check`        | 执行 Cargo check 和 TypeScript typecheck |
| `make format`       | 运行 Rustfmt、Oxfmt 和 Buf format        |
| `make generate`     | 生成 Protocol 代码和 Worker 类型         |
| `make lint`         | 运行 Clippy、Oxlint 和 Buf lint          |
| `make test`         | 运行 Rust 和 TypeScript 测试             |
| `make clean`        | 清理可重新生成的构建产物                 |

## 部署边界

- `storya-web` 和 `storya-admin` 生成独立静态前端产物。
- `storya-hls-loader-react` 生成 Vite 静态产物，并通过独立的 Cloudflare Workers Static Assets Worker 部署。
- `storya-api` 生成独立 Rust release 二进制。
- `storya-websocket-transport` 作为 Cloudflare Worker 独立部署，不与中心 API 合并进程。
- `packages` 不独立部署，其代码进入消费者构建产物。

服务之间通过公开协议通信，不通过共享数据库或读取其他服务内部状态形成隐式耦合。具体鉴权、存储、媒体源和部署拓扑尚未确定，在实现相应业务前不提前固化。

## 当前实现范围

当前仓库已经完成:

- Rust 和 pnpm workspace 骨架。
- Web、Admin、中心 API 和 WebSocket Transport Worker 的最小可运行项目。
- HLS 并行加载器 React 交互示例及其 Workers Static Assets 部署入口。
- 框架无关播放器 Web Component。
- HLS 自定义主 StreamController、VirtualStream 有序窗口、共享 fLoader、统一 HTTP Transport、Range Chunk 并行、正式读取抢占、Segment 驱离和诊断快照。
- 通用 Fetch/WebSocket HTTP Transport、串行复用连接池和流式 WebSocket Transport Worker relay。
- Protobuf/Buf 跨语言生成链路。
- 统一工具链、Makefile、格式化和 lint 配置。

当前尚未实现:

- 用户、权限和管理业务。
- 视频上传、转码、媒资管理和存储。
- 播放鉴权和 WebSocket Transport Worker 的公开代理安全策略。
- 播放器自适应码流、DRM、字幕、遥测等生产能力。
- API 与 Protocol 生成类型的实际集成。

## 修改历史

- 2026-08-10: React 交互示例使用版本化 localStorage 自动保存用户配置，首次访问不再内置播放地址；Transport 切换只控制配置显隐，不清空 Worker 地址。
- 2026-08-10: 为 `examples/storya-hls-loader-react` 增加 Workers Static Assets 部署入口，并明确顶层目录按项目职责而非部署平台划分。
- 2026-08-10: 新增服务端 Worker 能力的顶层 `workers/` 分类，并将 `services/storya-edge-worker` 迁移为 `workers/storya-websocket-transport`，叶子项目名只表达具体职责。
- 2026-08-10: 删除没有产品消费者且与 Fetch/WebSocket relay 能力重叠的 HTTP Proxy Transport 和独立 Rust proxy 服务，网络实现收敛为 Fetch 与 HTTP-over-WebSocket。
- 2026-08-08: 完成新 HLS 并行加载模型, 由 Controller 维护有序窗口, Loader 统一持有多 VirtualStream、Chunk Worker、fLoader、Transport、驱离和诊断。
- 2026-08-07: 删除 HLS VirtualStream 与 StreamFiller 架构, 将并行加载器重建为 `ParallelStreamController` 和提供 fLoader 兼容的 `ParallelSegmentLoader`。
- 2026-08-07: WebSocket relay 恢复 Protobuf 控制帧、128 KiB 流式 body 和 CANCEL，同时保留单连接串行 Keep-Alive 及现有连接池策略。
- 2026-08-07: WebSocket relay 改为单 Request/Response 整包协议，删除流式分帧、取消、心跳和最大连接寿命；增加 TypeScript 手写二进制 codec 的 Protocol 例外。
- 2026-08-06: 增加基于 cross 的 `x86_64-unknown-linux-musl` Rust workspace release 构建入口。
- 2026-08-06: 增加 `storya-http-proxy` 和多 Origin HTTP Proxy Transport，Cloudflare 只作为其前置 CDN。
- 2026-08-05: 增加 WebSocket Transport Worker、通用 HTTP Transport、WebSocket 连接池和透明 relay。
- 2026-08-05: 增加 HLS 并行加载器边界，并完成虚拟流、跨 Segment 预填充和全局请求调度。
- 2026-08-04: 建立 Storya 总体设计，明确 apps/services/packages 初始边界、Protocol 机制、依赖方向和当前实现范围。
