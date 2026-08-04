# Storya 总体设计

Storya 是一个 Rust 和 TypeScript 混合的视频解决方案 monorepo。仓库统一管理工具链、依赖、跨语言协议和可复用组件，但每个应用和服务保持独立的运行与部署边界。

本文描述当前已经采用的仓库级设计。尚未实现的业务能力会明确标注，不把目录骨架写成已经完成的产品功能。

## 系统边界

Storya 当前包含三类运行端:

```text
用户界面      storya-web
管理界面      storya-admin

中心服务      storya-api
边缘服务      storya-playback-relay

共享能力      storya-player      storya-protocol
              播放器组件          跨语言协议
```

- `storya-web` 面向最终用户，负责视频浏览、播放和用户交互。当前仅包含基础页面骨架，并已经使用 `storya-player`。
- `storya-admin` 面向运营和管理人员。当前仅包含基础页面骨架。
- `storya-api` 是中心 API 服务，负责未来的核心业务接口。当前只实现 `/health`。
- `storya-playback-relay` 是部署在 Cloudflare Workers 的边缘播放中继。当前实现 `/health`，其他路径返回 `501`，尚未实现回源、鉴权、缓存或流量调度。
- `storya-player` 是框架无关的 Web Component。当前封装原生 `<video>` 元素及基础属性同步，尚未实现自适应码流、DRM、字幕或播放遥测。
- `storya-protocol` 是 Rust 和 TypeScript 共用的协议包。当前包含健康检查 Schema 和生成类型，但尚未接入 `storya-api` 的 HTTP 路由。

目标业务关系是:

```text
storya-web ---------> storya-api
      |
      +-------------> storya-player
                           |
                           +----> storya-playback-relay ----> 视频源站

storya-admin -------> storya-api

storya-protocol ----> 为需要跨进程或跨语言通信的边界提供 Schema
```

这张图表达已采用的职责方向，不表示所有调用链都已实现。特别是播放中继与源站之间的转发逻辑仍为空。

## 仓库结构

```text
storya/
├── apps/
│   ├── storya-web/
│   └── storya-admin/
├── services/
│   ├── storya-api/
│   └── storya-playback-relay/
├── packages/
│   ├── storya-player/
│   └── storya-protocol/
└── docs/
    ├── design/
    └── memory/
```

三类顶层目录的边界如下:

| 目录       | 责任                               | 运行方式           |
| ---------- | ---------------------------------- | ------------------ |
| `apps`     | 面向用户或管理人员的完整应用       | 由人直接访问或运行 |
| `services` | 独立部署并持续运行的后端或边缘服务 | 由部署环境运行     |
| `packages` | 编译期复用的组件和协议             | 不单独作为产品部署 |

所有叶子项目都使用 `storya-` 前缀、小写字母和 kebab-case。npm 包不使用 scope，也不发布到公共 registry。

## 依赖方向

依赖只能从具体运行单元指向共享包:

```text
apps --------+
             +----> packages
services ----+
```

- `packages` 不依赖具体 `apps` 或 `services`。
- 一个实现只有在出现真实消费者和稳定边界后才移动到 `packages`。
- 不因为多个项目暂时存在相似代码就提前建立 `storya-ui`、`storya-sdk` 或其他公共包。
- `storya-player` 保持框架无关，具体页面状态和产品交互留在对应应用。
- `storya-protocol` 只描述跨边界数据，不承载业务流程或服务实现。

当前实际依赖只有 `storya-web -> storya-player`。其他图中关系是已经采用但尚待实现的方向。

## Protocol

跨 Rust/TypeScript 或跨 TypeScript 项目的传输数据使用 Protobuf 描述，由 Buf 管理格式、lint、breaking check 和代码生成。

```text
packages/storya-protocol/
├── proto/
│   └── service/
│       └── health.proto
├── generated/rust/
└── typescript/generated/
```

协议约定:

- Proto 文件按领域直接放在 `proto/<domain>/`。
- 文件系统不增加重复的 `storya/` 目录层。
- Protobuf package 使用 `storya.<domain>`，例如 `storya.service`。
- 不建立预防性的 `v1`、`v2` 目录或 namespace。真正发生不兼容迁移时，根据实际兼容策略处理。
- Protocol 和 Worker 生成代码由 `make generate` 统一更新，不手工编辑，也不纳入版本管理。
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
| `make check`        | 执行 Cargo check 和 TypeScript typecheck |
| `make format`       | 运行 Rustfmt、Oxfmt 和 Buf format        |
| `make generate`     | 生成 Protocol 代码和 Worker 类型         |
| `make lint`         | 运行 Clippy、Oxlint 和 Buf lint          |
| `make test`         | 运行 Rust 和 TypeScript 测试             |
| `make clean`        | 清理可重新生成的构建产物                 |

## 部署边界

- `storya-web` 和 `storya-admin` 生成独立静态前端产物。
- `storya-api` 生成独立 Rust release 二进制。
- `storya-playback-relay` 作为 Cloudflare Worker 独立部署，不与中心 API 合并进程。
- `packages` 不独立部署，其代码进入消费者构建产物。

服务之间通过公开协议通信，不通过共享数据库或读取其他服务内部状态形成隐式耦合。具体鉴权、存储、媒体源和部署拓扑尚未确定，在实现相应业务前不提前固化。

## 当前实现范围

当前仓库已经完成:

- Rust 和 pnpm workspace 骨架。
- Web、Admin、中心 API 和播放中继的最小可运行项目。
- 框架无关播放器 Web Component。
- Protobuf/Buf 跨语言生成链路。
- 统一工具链、Makefile、格式化和 lint 配置。

当前尚未实现:

- 用户、权限和管理业务。
- 视频上传、转码、媒资管理和存储。
- 播放鉴权、回源、缓存与加速策略。
- 播放器自适应码流、DRM、字幕、遥测等生产能力。
- API 与 Protocol 生成类型的实际集成。

## 修改历史

- 2026-08-04: 建立 Storya 总体设计，明确 apps/services/packages 边界、Protocol 机制、依赖方向和当前实现范围。
