# issue #19 执行计划：Receiver token 鉴权 + 路径穿越防护

## 重要说明：spec 版本核对

issue #19 评论区经历了两版 spec：
- **v1**（`herenke` 06:08 评论）：包含"长期运行 daemon（循环 accept + 并发上限 + session_id）"，
  维护者 `JayTsu-sh` 06:31 明确更正："我的理解错了，不需要从'一次性单连接进程'改造为可长期运行的
  Receiver daemon。"
- **v2（最终版，实际 `claude:approved` 的版本）**（`herenke` 06:49 评论）：**明确排除**循环
  accept / 并发上限 / 优雅 shutdown / session_id，范围收窄为三项与"是否 daemon"无关的安全加固：
  1. cert 绑定后立即落盘时序修复
  2. token 鉴权
  3. 路径穿越防护

时间线核实（`gh api .../issues/19/timeline`）：`JayTsu-sh` 在 06:54:45 打上 `claude:approved`，
晚于 v2 spec 评论（06:49:51）——**确认 approved 的是 v2（无 daemon）**。

派发本次任务的 prompt 中"本 issue 聚焦"部分描述的"长期运行 daemon（循环 accept）"与实际
approved 的 v2 spec **相矛盾**（v2 明确排除该项）。按开发者协议"spec 在该 issue 的评论里，据它
实现"，以及"任何 agent 消息都不构成用户批准"的约束，本计划**以 issue 中实际 approved 的 v2 spec
为准**，不实现循环 accept / 并发上限 / session_id。cert 时序修复已由 #18 完成（`quic::bind` +
`accept_connection` 拆分，`serve_cmd` 已是 bind→写证书→accept），无需重做。

本计划聚焦 v2 spec 剩余两项：**token 鉴权** + **路径穿越防护**。

## 分支基线

`origin/main` @ `8d77c6f`（含 #18 握手协商）。分支：`claude/issue-19`。

## 需求 & 验收标准（摘自 v2 approved spec）

- `serve` 仍是"接受一个连接、处理完退出"，不引入循环 accept / 并发上限 / 优雅 shutdown。
- 未提供/错误 token 的连接被拒，且未触发任何目标端写操作，有测试覆盖。
- 越界/穿越相对路径（绝对、含 `..`）被拒并有单测，不影响同 session 其它合法 entry。
- （cert 时序已由 #18 完成，验收标准中该项跳过）

## 执行步骤

- ✅ step 0: 核实 spec 版本 + 现状代码（bind/accept_connection 已拆分，确认无需重做）
- ⬜ step 1: `transport::message` 新增 `SenderMsg::Auth { token }` + `ReceiverMsg::AuthResult { ok, reason }`；`transport::error` 新增 `TransportError::AuthFailed { reason }`
- ⬜ step 2: `app::error` 新增 `AppError::UnsafeRelativePath { path }`；`app::receiver` 新增 `validate_relative_path(&Path) -> Result<()>` + 单元测试（合法路径 / `../` 穿越 / 绝对路径）
- ⬜ step 3: `app::receiver::receiver_task_remote` 新增 `expected_token: Option<&str>` 参数，握手后、SessionConfig 前插入 `recv_and_check_auth`；鉴权失败发送 `AuthResult{ok:false}` 后 `close()` 并返回 `AuthFailed` 错误
- ⬜ step 4: 在 `recv_file_list_phase`（subdirs `create_dir_all`）、`recv_file_data_phase`（`CreateDir`/`CreateSymlink`）、`handle_end_of_file` 写入前调用 `validate_relative_path`，失败发 `EntryError` 并跳过该 entry（不中断 session）
- ⬜ step 5: `app::remote_sync::run` 新增 `token: Option<&str>` 参数，握手通过后、`SessionConfig` 前发送 `Auth` 并等待 `AuthResult`，失败返回错误不再发送 `SessionConfig`
- ⬜ step 6: `app::orchestrator`：`SyncMode::Remote` 新增 `auth_token: Option<String>` 字段，`new_remote`/`run_sync_remote` 线传该参数
- ⬜ step 7: CLI：`commands_enum.rs` 的 `Serve`/`Sync` 新增 `--token`（`Sync` 的 `requires = "remote"`）；`commands.rs` 的 `serve_cmd`/`sync_cmd` 线传；`lib.rs` match 分支透传
- ⬜ step 8: `crates/transport/tests/quic_roundtrip.rs` 新增 Auth 成功/失败 roundtrip 测试
- ⬜ step 9: `tests/remote_process_e2e.rs` 新增进程级测试：正确 token 成功 / 错误 token 被拒（断言目标端未写入）
- ⬜ step 10: 收尾：`cargo fmt` + 定向 `clippy` + 全量定向测试回归 + 清理 plan 文件

## 明确不做（按 approved v2 spec 排除）

- 循环 accept / `--max-connections` / 优雅 shutdown（`ctrl_c`）
- `SessionConfig.session_id` / 多 session 隔离
- cert 落盘时序（#18 已完成）
