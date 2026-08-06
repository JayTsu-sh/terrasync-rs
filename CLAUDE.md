# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Before You Write Any Code

Every time. No exceptions.

1. **Grep first.** Search for existing patterns before creating anything. If a convention exists, follow it.
2. **Blast radius.** What depends on what you're changing? Check imports, tests, consumers. Unknown blast radius = not ready to code.
3. **Ask, don't assume.** Ambiguous request? Ask ONE clarifying question. Don't guess, don't ask five questions. One, then move.
4. **Smallest change.** Solve what was asked. No bonus refactors. No unrequested features. Scope creep is a bug.
5. **Verification plan.** How will you prove this works? Answer this before writing code.

## 按场景加载文档

| 场景 | 优先读 |
|------|-------|
| 首次进入 / 理解代码结构 | `.claude/docs/codebase.md` |
| 改 scan / sync 业务逻辑 | `.claude/docs/architecture.md` |
| 跑 / 改 E2E 测试 | `.claude/docs/e2e-testing.md`、`tests/lab/README.md` |
| 改存储驱动 / 配置测试环境 | `.claude/docs/services-and-storage.md` |
| 提 commit / PR | `.claude/docs/conventions.md` |
| 改 Web UI | `.claude/docs/codebase.md`（前端章节）|
| Claude 协作机制 / 新建 skill | `.claude/docs/claude-onboarding.md` |

知识库索引：`.claude/docs/README.md`

## Build & Development Commands

```bash
# Standard build
cargo build                          # debug
cargo build --release                # release
cargo build --all-features           # all features

# Cross-compile (cargo-zigbuild)
make release                         # x86_64-unknown-linux-musl release
make debug                           # x86_64-unknown-linux-musl debug

# Run
cargo run -- scan --id my_scan /path/to/dir

# Tests
cargo test --workspace --no-fail-fast
cargo test -p app -- test_name --nocapture

# Quality
cargo fmt && cargo check
```

## Workspace Architecture

| Crate | Role |
|-------|------|
| `src/` (root) | Entry point — calls `cli::cli_match()` |
| `cli/` | Argument parsing (clap), routes to `app` |
| `app/` | Core business logic: scan, sync, dir_walker, consumers, ACE |
| `data-mover` | Storage abstraction: NASEntry, S3Entry, StorageEnum, filter, qos (external git dep) |
| `db/` | Database layer: ClickHouse |
| `transport/` | 传输层：InProcess / QUIC，Sender↔Receiver 消息协议 |
| `sync-delta/` | chunk 级增量算法（rsync 风格）|
| `licensing/` | 离线 license 验证（Ed25519 + 机器指纹）|
| `kafka/` | Kafka producer/consumer |
| `utils/` | AppConfig, logger, crypto, types |
| `web/` | Web API (axum + SQLite), DDD 四层 |

**Data flow:** `cli → app → dir_walker (StorageEnum) → transport → consumer → db`

**Key types:** `NASEntry`, `S3Entry`, `StorageEnum`, `SenderMsg/ReceiverMsg`, `DeltaToken`, `db::Database`

**Incremental:** 自动检测 `jobs/<job_id>/` 目录存在时切换为增量模式；删除该目录重置为全量。

## Storage URL Formats

```
Local:     /path/to/dir  or  C:\path\to\dir
NFS v3:    nfs://server:port/export/path:/prefix?uid=1000&gid=1000
S3:        s3://access_key:secret_key@bucket.host:port/prefix
           s3+https://access_key:secret_key@bucket.host/prefix
SMB/CIFS:  smb://user:password@host[:port]/share[/sub/path][?smb2_only=false]  (\ → %5C)
           # smb2_only 默认 true（跳过 SMB1 探测帧，直接 SMB2 协商）
           # smb2_only=false：启用多协议协商，兼容不接受直接 SMB2 的老设备
```

## Core Code Rules

> 完整规则见 `.claude/rules/rust-patterns.md`（始终自动加载）

- **use 语句**：全部集中在文件顶部；函数体内路径最多两层，超过必须顶部 import
- **禁止** `.unwrap()` / `.expect()`（编译期 deny）；测试代码用 `#[allow(clippy::unwrap_used)]`
- **每个 crate** 必须有 `src/error.rs`，用 `thiserror` 定义专属 Error 枚举，不得用 `Box<dyn Error>` 或 `String`
- **重构**只改结构不改语义；前后可观测行为完全一致，所有测试必须 pass

## Approach

1. 行动前先思考，写代码前先阅读现有文件
2. 输出简洁，推理彻底
3. 优先编辑而不是重写整个文件
4. 不要重复阅读已经读过的文件
5. 在宣布完成前测试你的代码
6. 不要有奉承的开场白或结束语
7. 保持解决方案简单直接
8. 用户指令始终覆盖此文件
9. 注释用中文，技术标识符保持英文
