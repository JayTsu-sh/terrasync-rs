# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

## Direct 取消回归

`verify-direct-cancel.py` 在真实远端 Local 目标和 QUIC 链路上验证 Direct 取消。
先构建 `cargo build --release --bin terrasync` 和
`cargo test -p app --test direct_cancel_real --no-run`；后者输出独立测试二进制路径。

```sh
timeout 1200 python3 tests/lab/verify-direct-cancel.py \
  --binary /path/to/release/terrasync \
  --sender-test-binary /path/to/direct_cancel_real-HASH \
  --work-dir /tmp/terrasync-direct-cancel-RUN \
  --run-id nightly-direct-cancel-YYYYMMDD-HHMM \
  --ssh-key /path/to/terrasync_lab
```

默认在目标 VM `10.131.9.13` 启动正式 Receiver，业务连接走 `10.10.1.13`；
runner 调用 `prepare-run.sh`／`cleanup-run.sh`，只写本次 `/var/lib/terrasync-ci/<run>/`。
测试 Sender 使用生产 QUIC transport，在远端确认私有补丁已接收或原生文件校验开始后
主动关闭连接。阶段确认使用 Linux `/proc/<pid>/fdinfo`：basis 读取为 positioned read，
校验使用顺序读取；测试不依赖固定睡眠来猜测阶段，也不向生产代码加入暂停入口。

默认每项使用 2 GiB 文件和 16 MiB 差异。分别检查接收中断不改目标、校验中断保留已写内容
但不发布元数据、不报告业务成功、三秒内取消退出、进程及临时文件收尾；每次中断后用正式
CLI 对同一目标重新执行，独立核验 SHA-256、大小、inode 和 mode。报告同时记录两次正常重试
及清理结果。此测试覆盖 Direct 的真实 Local 目标，不代表其他 backend 或其他写入策略的
故障矩阵已通过。

| Role | Management | Data | Services |
|---|---|---|---|
| Controller | 10.131.9.11 | 10.10.1.11 | GitHub Actions Runner, ClickHouse |
| Source | 10.131.9.12 | 10.10.1.12 | NFSv3, NFSv4.1, RustFS |
| Destination | 10.131.9.13 | 10.10.1.13 | NFSv3, NFSv4.1, RustFS |
| Worker | 10.131.9.14 | 10.10.1.14 | RustFS, fault injection |
| HDFS NameNodes | 10.131.9.30, 10.131.9.33 | - | HDFS HA, Kerberos |
| HDFS DataNodes | 10.131.9.31, 10.131.9.32 | - | HDFS data plane |

Every run must call `prepare-run.sh` with a unique `nightly-*` or `release-*`
identifier and call `cleanup-run.sh` from an `always()` step.

Management traffic uses `10.131.9.0/20`. Test data uses `10.10.1.0/24`.
Credentials are provisioned on the self-hosted runner and must not be committed.
NFS fixtures use UID/GID 1000 to match the non-root GitHub runner, allowing
metadata preservation to be verified without privileged local writes.

The self-hosted runner uses the same environment as `data-mover-rs`. It must
provide `LAB_S3_ACCESS_KEY` and `LAB_S3_SECRET_KEY`, Python 3 with `boto3`, and
the `terrasync-lab` SSH key. Direct data-network traffic must bypass any system
HTTP proxy. Rust 1.95.0 with Clippy and rustfmt must be preinstalled; nightly
sets `RUSTUP_TOOLCHAIN=1.95.0` and verifies it instead of downloading tools at
runtime.

The `TS-SINGLE` matrix additionally requires CI-only `LAB_CIFS_USERNAME`,
`LAB_CIFS_PASSWORD`, `LAB_DXN_S3_ACCESS_KEY`, and `LAB_DXN_S3_SECRET_KEY`.
The FAS2750 share defaults to `ontap_lisaauto_cifs`; all values can be
overridden by runner-only `LAB_*` settings and must never enter logs or reports.
Its writable lab root is `ci/terrasync-data-mover` by default
(`LAB_CIFS_WRITABLE_ROOT`), rather than the share root. This is required because
the FAS volume uses UNIX security style and the SMB user receives ordinary
`Change` permissions, not ownership or ACL-administration rights.

`run-e2e.sh` retains the directed 4-by-4 compatibility suite across Local,
NFSv3, NFSv4.1, and standard S3, verifies every destination by
SHA-256, and independently runs quick and full integrity checks. It also covers
same-backend incremental synchronization and the local filter contract. The
nightly workflow separately runs the real two-process QUIC tests. Each run uses
an isolated ClickHouse database that is created and dropped for every run.

`run-hdfs-e2e.sh` certifies the pinned `data-mover` revision against the HA,
Kerberos-protected HDFS cluster. The runner provides `LAB_HDFS_LOCATION`,
`LAB_HDFS_ADMIN_USER`, `LAB_HDFS_CONFIG_DIR`, and `LAB_HDFS_KEYTAB`. It covers
HDFS-to-HDFS copy and full integrity in both single-process and separate QUIC
sender/receiver modes. The sender receives source credentials only and the
receiver destination credentials only. Missing source configuration and an
invalid keytab are negative gates, including a secret-leak assertion. All HDFS
paths, ClickHouse databases, copied keytabs, credential caches, and generated
0600 configs are scoped to the run id and removed by the exit trap. The JSON
artifact follows `tests/lab/evidence-matrix.json` and records both exact commit
identities.

`run-single-process-matrix.sh` is the authoritative `TS-SINGLE` evidence gate.
It emits 64 independent cell records for Local, NFSv3, NFSv4.0, NFSv4.1,
FAS2750 CIFS, standard S3, DXN S3, and HDFS. Every cell preloads its source,
runs the protocol-neutral `Storage + run_local_transfer` single-process path,
verifies every stream with copy-time BLAKE3, reads the destination back, and
records the exact terrasync and data-mover commits. This runner intentionally
does not route through the legacy URL-dispatching CLI. FAS2750 and DXN
credentials are CI secrets; reports contain only profile-level environment
fingerprints.

While FAS2750 CIFS root provisioning is explicitly deferred, nightly sets
`TS_SINGLE_DEFER_CIFS=true`. This emits and validates the 49-cell product of
the remaining seven profiles, with `cifs_fas2750` recorded in
`deferred_profiles`; it is not a substitute for the 64-cell acceptance gate.
Release validation remains on the full matrix.
