# Storya

本文件是 Storya 仓库的统一开发说明，也是项目规则、Memory 和设计文档的入口。内容不依赖具体的 AI 编程工具；`CLAUDE.md` 只负责指向本文件，不维护另一份规则。

## 项目概要

Storya 是一个由 Rust 和 TypeScript 构建的视频解决方案 monorepo。仓库包含面向用户和管理员的应用、独立部署的服务，以及跨应用复用的播放器和协议包。

- `apps/storya-web`: 面向用户的 Web 前台。
- `apps/storya-admin`: 面向管理人员的 Web 前台。
- `services/storya-api`: Rust 中心 API 服务。
- `services/storya-http-proxy`: Rust 无状态 HTTP proxy，通过 Cloudflare CDN 提供多域名 GET/HEAD 直通。
- `services/storya-edge-worker`: 基于 Cloudflare Workers 的边缘能力部署单元，当前提供通用 HTTP relay。
- `packages/storya-player`: 框架无关的 Web Component 播放器。
- `packages/storya-hls-loader`: 基于虚拟流和 hls.js fLoader 的 HLS 并行加载器。
- `packages/storya-transport`: 提供 Fetch、HTTP Proxy 和 HTTP-over-WebSocket Transport。
- `packages/storya-protocol`: 使用 Protobuf 和 Buf 维护的 Rust/TypeScript 共用协议。

## 工作方式

- 开始工作前先了解用户目标、相关代码、已有设计和最近改动，不凭空设计。
- 简单且目标明确的任务直接调查、修改并验证，不为流程创建临时设计文档。
- 会改变架构、公共接口、协议或多个组件边界的任务，先讨论关键取舍再实现。
- 只有需要长期保存的系统设计才写入 `docs/design`；特定场景的经验和约定写入 `docs/memory`。
- 修改代码后运行与范围相称的格式检查、lint、静态检查和测试。不能运行的验证必须说明原因。
- 不在当前任务中顺手修改无关问题。
- 没有用户明确授权，不执行 `git commit`、`git push`、合并、rebase、删除分支或丢弃改动。

## 语言与沟通

- 与用户交流、项目文档和代码注释使用简体中文。
- 先说结论，再说明动作、原因和验证结果。
- 中文使用半角标点。代码标识符和业内固定名称保留英文。
- 单行代码注释结尾不加句号。

## 全局工程规则

### Workspace 与任务入口

- Rust 项目统一加入根 Cargo workspace。第三方 Rust 依赖在根 `Cargo.toml` 的 `[workspace.dependencies]` 声明，成员使用 `workspace = true`。
- TypeScript 项目统一加入根 pnpm workspace，依赖版本优先由 `pnpm-workspace.yaml` 的 catalog 精确锁定。
- `Makefile` 是跨 Rust、TypeScript 和 Buf 的仓库级任务入口。`package.json` 不编排 Rust 命令。
- 根命令只负责全仓任务，子项目命令只负责该项目自身生命周期。不要为同一操作同时建立根别名和子项目脚本。
- 前端和 JavaScript 依赖只使用 pnpm，不使用 npm 或 yarn。
- 修改依赖时同步更新 `Cargo.lock` 或 `pnpm-lock.yaml`。

常用仓库命令:

```bash
make pnpm-install
make build
make build-linux
make check
make format
make format-check
make generate
make lint
make test
make clean
```

`make build-linux` 使用 cross 为 `x86_64-unknown-linux-musl` 编译 Rust workspace release 产物，不重复构建平台无关的 TypeScript 项目。可以通过 `LINUX_TARGET` 覆盖目标平台。

### 文件组织与命名

- `apps/` 放由用户直接运行或访问的应用。
- `services/` 放独立部署、持续运行的服务。
- `packages/` 放被应用和服务复用的组件或协议。
- 叶子项目的目录名和 Rust/npm 包名统一使用 `storya-` 前缀、小写字母和 kebab-case。
- npm 包不使用 scope，全部保持 `private: true`。
- `apps`、`services` 和 `packages` 只负责分组，本身不是包。
- 共享包不能反向依赖具体应用或服务。只有存在真实消费者和稳定边界时才新增共享包。

### 格式化与 lint

- Rust 工具链和 `rustfmt.toml` 与 Pantheon 保持一致，不单独修改其中一份。
- Rust 使用 workspace 根 `rustfmt.toml` 和 Clippy。
- Web/JavaScript 生态使用根 `.oxfmtrc.json` 和 `.oxlintrc.json`，formatter 和 linter 只安装在根包。
- Protobuf 使用 Buf format 和 lint。
- 不在子项目重复增加 `format` 或 `lint` 脚本。
- 提交前至少运行 `make format-check`、`make lint` 和与改动相关的检查或测试。

### Protocol

- 跨 Rust/TypeScript 或跨 TypeScript 项目的传输协议统一放在 `packages/storya-protocol`。
- 跨 Rust/TypeScript 的 Schema 使用 Protobuf，格式化、lint、breaking check 和生成由 Buf 管理。仅 TypeScript 消费且性能敏感的协议可以使用经过设计文档确认的手写二进制 codec；当前只有 HTTP-over-WebSocket relay 使用该例外。
- Proto 文件按领域直接放在 `proto/<domain>/`，不增加重复的 `storya/` 目录层。
- Protobuf package 使用 `storya.<domain>`；不建立没有实际兼容需求的 `v1`、`v2` 目录或命名空间。
- Rust 生成代码位于 `generated/rust`，TypeScript 生成代码位于 `typescript/generated`，不得手工修改或纳入版本管理。
- `make generate` 统一生成 Protocol 和 Worker 类型；`build`、`check`、`lint` 和 `test` 会先执行该目标。
- 纯进程内类型留在所属项目中，只有跨边界传输的数据才进入 Protocol。

### Rust

- workspace 禁止 `unsafe`。
- 不使用无说明的 `unwrap()`。正常失败通过错误类型处理；启动期不可恢复错误使用 `expect` 并说明原因。
- 依赖在文件顶部导入。crate 内模块引用使用 `crate::` 路径。
- 不为单一场景提前增加 trait、泛型、配置项或扩展点。
- 只有存在真实复用、独立不变量、资源生命周期或业务边界时才抽取函数、模块或 crate。

### TypeScript

- 共享包直接暴露 TypeScript 源码供 workspace 消费，不为发布场景增加额外兼容层。
- `storya-player` 保持框架无关，不反向依赖具体 Web 应用。
- `services/storya-edge-worker/worker-configuration.d.ts` 是忽略的生成物，不手工修改或纳入版本管理。
- 不在多个子项目重复安装 TypeScript、formatter 或 linter；公共版本由根 catalog 管理。

### Git 与提交

- 任何 `git commit` 或 `git push` 前必须取得用户明确授权。
- 提交信息使用 `feat:`、`fix:`、`docs:` 等类型前缀，不添加模块 scope。
- 不添加 `Co-authored-by` 或其他 AI 署名。
- 详细规范见 Memory 中的“提交规范”。

## Memory

Memory 记录特定场景下需要长期保留的经验。遇到描述匹配的任务时，先读取对应全文。

- **提交规范** (`docs/memory/commit-conventions.md`) - 准备提交、组织 commit 或推送时阅读。

## Design

设计文档描述当前采用的结构、职责、接口和关键取舍。修改对应范围前先阅读相关设计。

- **Storya 总体设计** (`docs/design/storya.md`) - 仓库结构、组件边界、协议机制、依赖方向和当前实现范围。
- **HLS 并行加载器设计** (`docs/design/hls-parallel-loader.md`) - 虚拟流、跨 Segment 预填充、全局请求调度和 fLoader 边界。
- **HTTP Transport 设计** (`docs/design/http-transport.md`) - 通用 HTTP Transport、WebSocket 连接池、线协议和 Edge Worker relay。

## 文档维护

- 每次都必须遵守的内容写入本文件。
- 只在特定任务中需要的经验写入 `docs/memory`，并同步更新本文件的 Memory 索引。
- 系统结构、组件职责、接口和关键取舍写入 `docs/design`，并同步更新本文件的 Design 索引。
- 设计文档描述当前事实和已采用的方向，明确区分“已经实现”与“尚未实现”。
- 只有设计文档维护“修改历史”，按日期倒序记录影响设计含义的变化，不使用版本号。
