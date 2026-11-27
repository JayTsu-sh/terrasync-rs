# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

Evidence availability, November 16: the older `/tmp` artifacts cited below did
not survive the environment refresh. Their dated execution figures are
historical summaries, not inspectable evidence for the current working tree.
The current independent root-metadata revalidation subsection records durable
results and explicitly separates software, performance and field acceptance.

`python3 tests/lab/run-notification-archive-local.py --mutable-catalogue` runs
24 real migration rounds across Single and two-replica ClickHouse, ordinary and
suspended-version S3 sources, and latest-only and versioned destinations. An owned
read proxy fixes LastModified while retaining real payloads, ETags, tags and version
IDs. Equal-sized overwrites with the same version slot must copy once; an unchanged
third round copies no data. Independent S3 reads check final bytes, tags, retained
source and exactly two ordered destination versions where history is enabled.
Source request counters require two data GETs and three scan-time tag GETs per
case, so the identity comparison adds no source reads. The test does not establish
content equality when the backend's observed identity also stays unchanged, source
purge safety, or whole-job throughput/RSS. Containers and their private data are
removed on exit.

`python3 tests/lab/run-notification-archive-local.py --version-metadata` runs
56 real migration rounds across Single/two-replica ClickHouse, latest/history
S3 destinations and quick/full verification. Each case denies source tag reads
twice, denies destination tag writes twice, recovers, checks an unchanged round,
and changes only an old source version's tags. It checks data-only versus whole
readiness, raw source facts, exact destination version bindings and archived
reports. An independent S3 client checks every body/tag/version, latest head and
retained source; proxy counters require zero data replay in all six incremental
rounds. HTTP 204 callback checks cumulative entry counts, each version failure,
source observation diagnostics and conservative retained ranges separately.
This does not establish recovery for ambiguous historical data publication,
source purge readiness, whole-job network budgets or throughput/RSS.

## 扫描代次提交结果

```sh
python3 tests/lab/run-scan-generation-result.py
```

使用 Linux、Python 3.11+、Cargo、Docker 和 `nsenter`。构建以调用者身份运行，
隔离实验使用 root 或 `sudo -n`。入口从 Cargo 输出取得启用 `lab-fault-injection`
的 CLI 和回调驱动，在启动 ClickHouse 前核验两者并记录 SHA-256。构建缓存和本轮
可执行文件位于 Cargo 的目标目录。默认在临时目录保留构建日志、回调测试列表、
运行日志和场景报告，也可以用 `--output-dir` 指定一个新目录。

测试启动 ClickHouse 单机和两副本集群，核对 CLI、真实 HTTP 204 回调和归档报告。
覆盖全量、增量、续扫、提交前失败保留基线、提交后清理失败、提交响应中断、
回查断连和超时、重启后查证和补提。实验在回滚期间和补提之前重放旧提交，
并在发布前、发布应答丢失和发布后清理三个阶段取消任务。还验证同作业复制接管和纯流式省略字段。
实验室只使用本轮容器和数据库，保留每轮 CLI 日志及 JSON 证据。

## 迁移代次提交结果

构建 `cargo build --locked --bin terrasync` 后运行：

```sh
LAB_TERRASYNC_BIN=/path/to/terrasync \
LAB_COPY_RESULT_LOG_DIR=/path/to/new-results \
python3 tests/lab/run-notification-archive-local.py --copy-generation-result
```

单机和集群均以同一作业 ID 运行全量、增量、中途故障、重启和后续两轮增量，
逐轮核对终态、目录状态、数据库变更明细、文件内容及权限。故障覆盖提交前拒绝、
提交应答丢失后的已确认和待确认、静默应答超时、删源后目录事实清理失败。
待确认用例扣留 ClickHouse 回查响应，验证恢复预算耗尽后按执行身份取消仍在运行的查询、
关闭客户端连接及归档终态。
还检查删源后的全量重建、范围外目标保留及无数据库任务。支持 `--multi-shard`。

用 `cargo test --locked -p app --test copy_generation_result --no-run` 构建真实回调驱动，
以 `LAB_COPY_RESULT_CALLBACK_DRIVER` 指定测试二进制。驱动调用与 CLI 共用的迁移入口，
对照实际 HTTP 回调与 ClickHouse 归档。另行运行不设置该变量的 CLI 矩阵。

在获准访问现有 VM 104 的主机上，把 runner 换成
`python3 tests/lab/copy_generation_nfs.py`，可在独占 NFS 4.1 前缀复测待确认与后续多轮恢复。
它用稳定文件身份验证真实重命名及零重传，并撤销本次挂载和前缀。

同一 runner 的 `--copy-independent-failures --multi-shard` 覆盖增量比较阶段并发失败。
代理等两个 worker 都发出比较请求，再让真实 ClickHouse 返回不同的查询错误。
验证两个根因各计一次、目标保留、基线未发布；设置回调驱动时再验证 HTTP 重放沿用同一事件身份及累计值。

## Direct 取消回归

`verify-direct-cancel.py` 在真实远端 Local 目标和 QUIC 链路上验证 Direct 取消。
先构建 `cargo build --release --bin terrasync` 和
`cargo test -p app --features remote --test direct_cancel_real --no-run`；后者输出独立测试二进制路径。

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

`python3 tests/lab/run-version-entry-count-local.py` provides an isolated real S3 check for
versioned-source full-copy accounting. It requires local `docker`, `nsenter`, `boto3`, cached
Rust dependencies, and a cached RustFS image (`MIGRATION_S3_IMAGE` overrides the image).
It seeds 20 versions, runs the app copy pipeline into latest-only and versioned targets, checks
one completed entry in the statistics/progress report, and independently verifies one versus
20 destination versions and the retained source. A proxy then denies source tag reads: all
20 data versions must remain published, failure accounting must be one entry, and a real HTTP
callback must receive and acknowledge all 20 version-specific reasons. Denied source data reads
also check that later versions and history segments stop publishing, with one failed logical
entry across segment sizes 20 and 10. Success and source-metadata failure additionally run
with segment sizes 1 and 7: each target has all 20 versions, completion or failure counts
remain one, and the captured metadata failure causes no repeated tag requests. Its container and volumes are removed on exit. This check does
not cover ClickHouse acknowledgement, callback outages, or the shared network recovery window.

The runner also seeds a nonempty `a/`, `a/file`, and an empty marker. It verifies one
`copy_skipped` callback event and cumulative `skipped_count=1` with zero errors to Local;
object destinations preserve the complete key, bytes, child, marker, and the history actually
exposed by the source provider. The cached RustFS image stores slash keys as one `null` version
even in a versioned bucket: the runner records the pre-copy version list and requires the
source to remain identical afterwards; it does not claim a real 20-version slash-key history.
The 20-version skip-count and remote classification cases use deterministic observations.
Ordinary `payload` version history is independently checked against 20 real versions above.

An additional target proxy refuses every tag PUT with HTTP 403 while forwarding data
operations to the real S3 service. Whole-entry quick/full copies and history segments
of size 1/7 must each retain all 20 data versions and count one failed entry. The two
whole-entry runs deliver 40 concrete version errors to an HTTP 204 callback, each
reporting 140 copied bytes. Independent inspection reads every retained target
version and checks the original 20 source versions. Proxy counters require 80 data
PUTs, 80 tag refusals and exactly 20 full-verification data GETs: metadata failure
must not skip selected data verification. These checks cover the app's captured
metadata-plan integration and data-only failure evidence; they do not prove source
purge readiness or metadata repair without retransmission in the next generation.

It also counts source tag requests through a deny-on-unneeded-read proxy. An explicit metadata
scope for filesystem output reads only the child and empty marker tags (two requests), while
the nonempty slash object's tag endpoint is denied and must receive no request. Latest-only
output keeps all 20 source history records available for ordering and cleanup decisions but
requests tags only for the latest version (one request); all old-version tag endpoints are
denied. These are source scanning checks, not evidence of remote handshake scope propagation
or whole-job throughput/RSS equivalence.

The same runner also disconnects a required event consumer while another consumer's queue is
full. Against real versioned S3 it checks that the sender returns a typed failure after the
first published version, starts no later history version or delivered entry, releases its
active-entry counter, and retains all 20 source versions. Required-consumer failures latch
across broadcaster clones; blocked sends to other queues are cancelled. This is a prerequisite
for stopping safely when diagnostic persistence fails, not validation of diagnostic spill or
replay itself.

`python3 tests/lab/run-notification-archive-local.py --catalogue-recovery` checks
catalogue batch replay within the shared database execution scope. Its 32 cases
cover Single/one-shard-two-replica Cluster, latest/history target slots, normal,
flash disconnect, committed write with a lost acknowledgement, recovered timeout,
persistent disconnect/stall, permission rejection and cancellation. The proxy
checks one deduplication token across retries. Merge thresholds are reduced only
on owned test tables; raw reads without FINAL must contain exactly one batch,
with the same sequence, execution, working state, target-version mapping,
readiness and native metadata snapshots. Successful probes do not reset the failed
write's window: sustained loss exits around 3.05 seconds, silent loss around 4.2
seconds with the 1-second detection budget, and cancellation around 0.5 seconds.
HTTP 403 is attempted once. This validates the batch mechanism; copy-worker shared
scope integration, early source removal, directory finalization and throughput/RSS
acceptance remain separate work.

`python3 tests/lab/run-notification-archive-local.py --copy-generations` reuses the
isolated ClickHouse pair for migration-generation tests. Against both Single and the
one-shard/two-replica Cluster it checks previous source-retaining/deleting policies,
completed/incomplete generations, present/missing copy rows, policy persistence on reopen
and commit, and clearing the base after a source-deleting generation for either current
source policy. A controlled
proxy disconnects and silently stalls policy reads: the typed failure must propagate,
the policy must remain unchanged, and each request returns within its bounded timeout.
A real Local copy then verifies that switching to source deletion classifies an unchanged
entry without copy work, deletes its source, and leaves the target bytes, inode and ctime
unchanged. After a real Local copy and protected source purge, both a completed and an
incomplete source-deleting generation must switch to full when retaining the source again,
with no reverse-delete candidates and the existing target bytes, inode and ctime unchanged.
The same suite also leaves three compared generations uncommitted, commits a
selected subset, and deliberately makes an unselected old row's binary state
collide with the next working state. Only the currently selected unchanged file
is source-purged; outside files/directories remain, the unselected target directory
mtime is unchanged, and no file retransmission is requested. Preparation rebases
old rows to C; execution identity still limits cleanup to the current selection.
This component suite uses catalogue selection batches and Local IO. Actual window
planning is exercised separately by `--request-policy`.
These are catalogue/cleanup contract checks; they do not validate the pending
change to delete before asynchronous database confirmation, the whole-job failure callback,
version-history purge, six-replica topology or throughput/RSS targets.

`python3 tests/lab/run-notification-archive-local.py --copy-contracts` runs nine
additional catalogue contracts against an owned real Single deployment. It checks
explicit execution scopes, accepted empty generations, binding rejection, pending
rename acknowledgement after restart, ordered unconfirmed history versions,
nullable ownership, duplicate observations and directory-marker intents. Fixtures
establish a preceding generation explicitly; first Full preparation must not
silently inherit rows written before any accepted copy request. This runner skips
the separate six-replica topology tests and does not certify history publication
replay in app, which remains guarded.

`python3 tests/lab/run-notification-archive-local.py --version-catalogue` adds an
owned RustFS service to the isolated ClickHouse network. It executes eighteen actual
migrations across Single and the two-replica Cluster: four distinct source versions
to latest-only and history targets, two new source versions in one incremental entry,
unchanged incremental runs, a failed historical incremental run, and latest-only
runs after source delete markers are added. Sixteen runs succeed; two fail as injected.
It checks archived plans, selected bytes and logical inventory/delivery counts;
latest-only base remains one slot with raw source version identity, while the
successful history base grows from four to six versions. Independent S3 reads verify
all payloads, chronological publication order, six retained source versions and no
extra versions on unchanged replay. A source proxy denies the first new version's
data GET: each failed run counts one entry, reports that version's actual error and
the later version's blocked reason, starts no later data GET and leaves the target's
four successful historical versions intact. All four version errors reach HTTP 204;
accepted details clear, cumulative errors remain, and final Failed reports are archived.
Latest delete markers retain the existing latest-only target payload and report one
structured preservation event per deployment without task errors. `--copy-generations`
separately tests same-size/same-mtime version changes, raw tag recovery, unacknowledged
writes and source version reversal across three further uncommitted generations.
Earlier confirmed physical rows are deliberately retained: unconfirmed tombstones
must prevent their resurrection. These runs do not certify version-to-target metadata
retry mapping, unknown publication recovery, permanent history purge, or throughput/RSS.

`python3 tests/lab/run-notification-archive-local.py --scan-wire` runs three shared
scan-storage regressions on the owned two-replica topology: schema availability,
key staging with event replay, and native observation roundtrip/merge/deletions.
It checks the shared row/schema change without claiming six-replica topology or
whole-job fault/performance coverage.

`python3 tests/lab/run-notification-archive-local.py --retention-events` checks the
existing source-deleting migration's target-preservation branch against real Single and
two-replica ClickHouse, real Local target contents, and an HTTP 204 callback. Twenty
deterministic S3 history observations plus one directory produce two entry-level retention
events, no delete/rename/error counts, and 21 preserved base rows. The HTTP receiver checks
structured unknown-presence details; sender checks details cleared after receipt, cumulative
count retained, and final reports still queryable in ClickHouse. S3 source history is a
deterministic fixture, not a real S3 protocol run. This component suite does not validate source-presence probing or whole-job
throughput/RSS; actual window plans have separate request-policy coverage.

`python3 tests/lab/run-notification-archive-local.py --request-policy` checks the
task-owned SourceCleanupPolicy against owned Single and two-replica ClickHouse.
Three uncommitted attempts deliberately use a global source-retention setting
opposite to the request, then independently read the persisted request policy.
Remote move requests are rejected before transport/storage access; retained-source
requests with an invalid remote address report that address error instead of
inheriting the global deletion policy. Both errors reach the real HTTP 204 sink,
clear their delivered details, retain cumulative task errors and final reports in
ClickHouse, redact storage credentials and a whitespace-containing auth token,
and create no copy base. The process-level wrong-token QUIC case additionally
requires a final task-error count and no plaintext token in stdout/stderr. This is
preflight and notification evidence; it does not certify remote source deletion
or the whole-job recovery budget.
The same suite now checks Local packaged-mode rejection, invalid filters, missing
file lists and changed bindings before consumer startup. Rejections retain source
bytes; changed bindings also retain the original target and base. Invalid database
configuration still reaches the callback through the notification-only fallback,
returns the original typed error and starts no copy; that invalid database cannot
archive its report. A controlled proxy additionally disconnects/stalls startup
ClickHouse probes, then restores access when notification archival begins. Both
Single and Cluster report one task error, receive HTTP 204, preserve source bytes
and archive the final report. Observed total times including final notification
were 83/310 ms for disconnect and 1089/1306 ms for stall with one-second read
budgets; these are startup cases, not whole-job recovery/cancellation evidence.
There are 18 independently inspected rejected requests across both deployments,
including 14 Local requests. Successful Local full and subsequent incremental
runs verify actual target bytes, distinct execution identities, correct full/delta
report kinds, and two archived final reports per deployment without a callback.

The request suite also executes 48 successful Local migrations with include,
exclude and combined relative-time predicates across both database deployments.
Windows move out and back; only newly selected data transfers, outside facts and
targets survive, unchanged selected sources can be deleted, and a previous
source-deleting run forces the next run to full. Each actual mode, fixed filter
time, preservation policy and transferred byte count is checked against both
the HTTP callback and the separately queried final archive. Six changed-binding
requests leave the accepted descriptor intact. Ten additional successful runs
cover recovery after an accepted request's preparation is rejected with a real
HTTP 403, loss of base without loss of binding, and source-deleting rename copying
the new path while preserving the old target. Six rejected/failed lifecycle runs
check preparation readiness, mirror conflicts and changed bindings after base
removal. This is not a whole-job network/cancellation budget, asynchronous source
purge, full version-history, distributed fencing, or throughput/RSS certification.

`python3 tests/lab/run-notification-archive-local.py --source-absence` exercises
incremental target reconciliation against owned Single and two-replica ClickHouse
and actual Local source/target paths. Cases distinguish an unobserved but present
file/directory from a truly absent source, retain unknown descendants without
recursive deletion, and copy a new hardlink path while retaining the old target
and both confirmed catalogue facts. The hardlink association uses deterministic
NFS-shaped identities; it is not a real NFS protocol result. Events are checked
before production DatabaseConsumer processes them; HTTP delivery uses the separate
retention suite. The pinned data-mover namespace-read runner separately injects
real S3 disconnect, timeout, permission, cancellation and recovery. These checks
do not certify target ownership guards, whole-history absence, atomic cross-store
absence/delete, remote reconciliation, or the complete network/performance matrix.

`python3 tests/lab/run-notification-archive-local.py --rename-generations` uses real
ClickHouse Single and the two-replica Cluster to compare the same file/directory,
verify its persisted state changed, inject a connection failure, and reopen with
a new path observation for that same stable identity. It covers one, two and five interrupted rounds:
committed state stays fixed, working state stays fixed, and the final rename resolves
from the confirmed old target path. Occupied/swapped paths must copy before replacing
confirmed facts; an already copied new target must not be overwritten by an older rename.
A component case runs real Local target rename for an unfiltered source-retaining
policy, keeping the source. Source-deleting plans preserve old targets instead;
stable-identity file and directory observations must still produce copy work for
the new path. The actual Local source-deleting path is covered by `--request-policy`.
Source stable identities in the component cases are deterministic
NFS-shaped observations; this does not claim a real NFS/CIFS source, whole-process
SIGKILL, whole-job callback, or throughput/RSS validation. Local execution tests
separately recover an already-moved but unrecorded target by copying current source
bytes, including explicit full verification; recovered missing-path errors stay in logs.

`python3 tests/lab/run-notification-archive-local.py` validates the database notification archive
in isolated ClickHouse single-server and one-shard/two-replica deployments, with an embedded Keeper.
It requires `docker`, `nsenter`, cached Rust dependencies, and
`clickhouse/clickhouse-server:26.3-alpine` (`NOTIFICATION_CLICKHOUSE_IMAGE` overrides the image).
The repository uses the `deduplicate_insert` setting introduced in
[ClickHouse 26.2](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelogs/v26.2.1.1139-stable.md),
so older test images cannot execute the current database client settings.
The runner checks 1024 archived errors, reopen/replay, late inserts after ACK, discovery of a late
event behind a pagination cursor, exact final-failure
payloads, and byte-bounded reads of 800 KB payloads. Its proxy injects disconnects, lost write/ACK
responses, and silent response stalls; a real readonly setting checks confirmed refusals separately.
Only its own databases, containers, volumes, and configuration directory are removed on exit.
The runner also exercises the app's final callback lifecycle through real HTTP: disconnects,
HTTP 500, redirects, silent stalls, and recovery within four attempts. HTTP 2xx alone
is receipt success, including empty 204 and arbitrary 200/202 response bodies; redirects are disabled. It checks byte-identical
retries, retained unacknowledged diagnostics, the exact final report archived after exhaustion,
and archival during an already-cancelled task scope, successful HTTP delivery, or no configured
callback. Callback delivery failure causes remain only in logs; they are excluded from the
archived envelope. Final reports remain queryable after ACK via
`ClickHouseDatabase::final_report` (round, attempt, sequence). The job-scoped
`notifications_<sanitized_job_id>` table stores kind 2 for reports and delivery_revision 0/1
for pending/ACK, with the exact report in the envelope's `payload_json` field.
The archive proxy separately checks lost initial-write/ACK replies, disconnects, and silent
stalls during that lifecycle. Normal final HTTP delivery has a shared two-second budget,
plus at most one second shared by initial archival and durable ACK; cancelled lifecycle
uses 500 ms for each. Expired archival has an unknown write outcome and does not claim delivery.
The app journal tests drive 1280 version failures through the real statistics consumer and bounded
broadcast queue. They check 64 failed entries, all 1280 unique diagnostics delivered in pages,
a memory prefix of at most 64 events during healthy archival, and no recounting during replay.
Lost archive/confirmation responses recover without duplicate accepted HTTP details. Restored
pending confirmations and stale checkpoints cannot resend already confirmed events.
A restored empty checkpoint must still discover diagnostics archived after that checkpoint. Cancellation
retains an unknown archive prefix for idempotent replay. Persistent write disconnects and stalls
stop the producer even while database reads succeed; the identified incident has one shared
three-second recovery window, with a separate one-second request detection bound. Final reports
have `diagnostics_complete=false` if collection or delivery cannot be completed in budget.
These checks cover the notification chain, not the full source/target network fault matrix,
whole-worker cancellation, or throughput/RSS parity of the complete migration.

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

Nightly reaches the five-node `hdfscluster` through `with-hdfs-tunnel.sh`:
NameNodes `10.30.3.160/162`, DataNodes `10.30.3.161/163/164`, and the
KDC on `.164`. The runner cannot route directly to this data network. The
wrapper opens loopback-only SSH forwards through `10.128.135.164`, creates
per-process Kerberos, HDFS and hostname configuration, and removes them and
the tunnel afterward. Both HDFS gates hold the shared lab lock while the
forwards are active. The gateway is a single point of failure for the test
path; NameNode HA does not make the gateway highly available.

The runner-only directory `~/.local/share/terrasync-hdfs-ci` contains
`tunnel_key`, pinned `known_hosts`, `client.keytab`, and the extracted Ubuntu
`libnss-wrapper` library under `pkg/usr/lib/x86_64-linux-gnu/`. The dedicated
`terrasync-fwd` SSH key is restricted to the six required TCP destinations;
`terrasync-lab-ci@HDFS.INTERNAL` owns only its private test directory under
`/tmp/terrasync-nightly/terrasync-lab-ci`. Protect these files as credentials.
No node service keytab or HDFS administrator keytab belongs on the runner.
The runner also installs `hdfs-runner/loopback.nft` at
`/etc/terrasync-hdfs/loopback.nft`, enables the adjacent systemd service, and
installs `hdfs-runner/sudoers-read-rules` through `visudo`. These rules reject
access to the six local forward ports from every OS UID except the CI UID
1000; the sudo rule permits only reading this nftables table. The wrapper
checks the rules before opening a tunnel. Jobs using the same `github-runner`
account share its credentials and remain in the same trust boundary.
The wrapper is Nightly-specific; callers of the underlying scripts can still
supply their own HDFS environment and `LAB_HDFS_BASE_PATH`.

`run-hdfs-e2e.sh` certifies the pinned `data-mover` revision against the HA,
Kerberos-protected HDFS cluster. The runner provides `LAB_HDFS_LOCATION`,
`LAB_HDFS_ADMIN_USER`, `LAB_HDFS_CONFIG_DIR`, and `LAB_HDFS_KEYTAB`. It covers
HDFS-to-HDFS copy and full integrity in both single-process and separate QUIC
sender/receiver modes. A separate data-only HDFS-to-local readback verifies
each destination SHA-256 without requiring HDFS names to map to local UID/GID.
The sender receives source credentials only and the receiver destination credentials only. Missing source configuration and an
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
records the exact terrasync and data-mover commits. Its aggregate also requires
successful HDFS and DXN prefix cleanup; the dedicated DXN test bucket must have
versioning disabled and its object/multipart prefix is checked afterward. This runner intentionally
does not route through the legacy URL-dispatching CLI. FAS2750 and DXN
credentials are CI secrets; reports contain only profile-level environment
fingerprints.

While FAS2750 CIFS root provisioning is explicitly deferred, nightly sets
`TS_SINGLE_DEFER_CIFS=true`. This emits and validates the 49-cell product of
the remaining seven profiles, with `cifs_fas2750` recorded in
`deferred_profiles`; it is not a substitute for the 64-cell acceptance gate.
Release validation remains on the full matrix.

Nightly sets the DXN S3 endpoint and private test bucket through
`LAB_DXN_NIGHTLY_ENDPOINT` and `LAB_DXN_NIGHTLY_BUCKET` repository variables.
The DXN test identity is supplied through `LAB_DXN_NIGHTLY_ACCESS_KEY` and
`LAB_DXN_NIGHTLY_SECRET_KEY` Actions secrets. Its RGW HTTP endpoint must be
reachable from the runner. The matrix cleans up objects and unfinished uploads
under its own DXN prefix and records the configured endpoint in the evidence.
If a transfer cell fails, the matrix records that failure and continues to
collect results. The evidence validator still requires every active cell to pass.

`run-source-metadata-guard.py` requires a unique `nightly-source-metadata-guard-*`
prefix already held by a `prepare-run.sh` / `cleanup-run.sh` lifecycle and
`LAB_SOURCE_GUARD_RUN_ID`, `LAB_SSH_KEY` plus root fixture control. It connects a
private TCP relay to the source NFS v4.1 data interface, then runs the ignored
`source_cleanup::real_tests` cases. It independently checks actual server files
and target bytes: unchanged source removal, post-copy source mode changes,
idempotence, recovered disconnect, continuous loss, silent loss and cancellation.
A counting namespace delegates to the real NFS role and requires zero purge calls
when metadata observation fails. The relay affects only its own sockets; no
server, firewall, export or SSH configuration is changed. The caller must always
execute cleanup, including after test failure. This verifies source observation
and purge admission, with the Unix Local target persistence barrier now on that path.
Real local-filesystem persistence tests separately cover stale inode/root bindings,
root rename round trips, file/directory sync failure and cancellation; app tests
check that persistence refusal keeps both copies and reports a typed cause. This is
not power-cut, network-target persistence/ownership, task-wide deadlines,
ClickHouse/callback outages or full migration performance.

Ordinary file-copy requests explicitly opt in to data-mover's owned Direct failure
cleanup. The four `file_copy::transfer_contract_tests` verify removal of created
and overwritten Local bad output, source retention, typed diagnostic preservation,
and metadata-failure data retention and byte accounting. Native coverage adds
eight backend and five engine real-file tests, including cancellation with settled
writers, replacement refusal and publication uncertainty. Only Unix Local implements
the removal role; post-publication data verification, other adapters and generated
streams remain separate work. These functional results do not establish throughput
or RSS parity.


`run-notification-archive-local.py --consumer-recovery` provisions its own Single
and two-replica Cluster ClickHouse pair, fault proxy and HTTP 204 callback. It runs
the actual full/incremental fact consumer through normal writes, recovered faults,
lost committed acknowledgements, persistent disconnect, silent timeout, permission
refusal, cancellation, idle faults and final-batch faults. Queued later batches must
stop after a final failure; the original catalogue must still archive a final
report and expose it through the notification read API. Four complete migration
runs also check that database refusal reaches the final callback instead of a
secondary broadcast-disconnected error. The runner removes its own containers on
exit. This is consumer/notification evidence, not a claim that active storage I/O,
walker initialization and all workers already share cancellation, or that the
migration meets throughput/RSS targets.


The Unix Local protected-purge regression captures source observations, copies the
data, rewrites the source with equal-length bytes and restores mtime. The new source
must remain, and the previous target bytes must remain. A second application test
keeps an unmigrated `.data-mover-other-task` subtree when directory purge is refused.
Native tests additionally cover persisted snapshots, missing change evidence,
hidden files/symlinks/subtrees, idempotence and normal child cleanup. Callback-outbox
tests check concrete ctime, nonempty-directory and injected read-only refusal causes;
the read-only callback case is diagnostic injection, not a real read-only mount.
No claim of concurrent-writer exclusion, atomic compare-and-unlink, power-cut or
network-mount certification follows from these tests.


`run-notification-archive-local.py --copy-execution` exercises the task-owned database
scope through actual incremental comparison workers and both consumer phases. Twelve
Single/Cluster attempts cover comparison disconnect, lost read response, recovered
silent timeout, persistent timeout and permission refusal. Each starts with a normal
one-file full copy, which also catches excessive nested async-poll stack usage without
raising the test stack. Fault timing starts at the first affected comparison request;
whole-run time includes preparation and reporting. The runner independently checks
24 final HTTP 204 summaries, original failure causes, recovered counts and preserved
source/previous target. This does not certify active storage-I/O cancellation or a
whole-task deadline with large files or stalled backend initialization.


`run-notification-archive-local.py --copy-data-cancel` interrupts the task-owned
ClickHouse data/probe path after real Local payload bytes are already written.
The notification archive path stays available; the data path is restored only
after the execution requests query cancellation. Single and two-replica Cluster,
full and incremental, Direct and Checkpoint each cover disconnect and silent
requests. The 256 MiB payload is source-throttled; Checkpoint injection waits
until more than the native 64 MiB automatic checkpoint boundary has been written.
The runner checks that failed runs retain the source, Direct removes its owned
partial output, and Checkpoint reruns reuse actual bytes rather than merely
leaving a partial file. It also checks HTTP 204 final summaries and error totals.
This covers cancellation propagated from the database execution to ordinary
Local file transfers. It does not certify cancellation of traversal, directory
or metadata operations, backend initialization, rename or remote transfer, nor
storage-network recovery or whole-task throughput and RSS.


The native directory iterator now shares the local copy execution cancellation
signal. Its channel owner can cancel backpressure and explicitly join the adapter,
which waits for native traversal completion. Local producer tests cover a full
128-entry buffer, parent cancellation, completed-but-undelivered buffers, normal
EOF, concurrent join waiters and a typed producer panic. The copy data-cancellation
and comparison suites exercise the actual full/incremental orchestration around
this scope. File-list point-read integration is covered below; initialization remains pending;
these checks do not certify whole-task or storage-network deadlines.


`run-notification-archive-local.py --copy-file-list-cancel` runs the same 16
Single/Cluster full/incremental Direct/Checkpoint failures through file-list
selection, with 8 actual checkpoint resumptions and 32 HTTP 204 final summaries.
File-list point reads use the existing native bounded namespace-observation API;
no data-mover public API was added by this file-list step. The reused
read_source_observation API was introduced earlier in this optimization at native
commit 123cc8f, rather than being an original main API. Their 1000-entry queue participates in the
same cancellation and producer join ownership as directory traversal.
`run-source-metadata-guard.py --file-list`, under the same required owned-prefix
prepare/cleanup lifecycle, provisions four read-only-observed NFS fixtures and
checks flash recovery, sustained disconnect, silent timeout and in-flight
cancellation. Server-side reads independently check all four retained payloads.
Observed times were about 24 ms, 3.3 s, 4.0 s and 251 ms with a 1 s detection limit
and a 3 s recovery budget. The actual migration configuration defaults to a 30 s
detection limit, so those test timings do not certify the default whole task.
These are separate real NFS and ClickHouse link tests, not a combined outage or
whole-job throughput/RSS acceptance. The lab prefix was cleaned after this run.


### Source-deleting generation base retirement

`python3 tests/lab/run-notification-archive-local.py --copy-generations` also runs
`real_source_deleted_base_retirement` on the isolated Single and two-replica
Cluster. Eighteen cases cover Completed/Failed/Cancelled retirement, both physical
replicas empty, other-job isolation, delayed old mutations preserving newer copy
and sequence-zero scan rows, disconnect, silent stalls, cancellation of the
retirement future, and lost mutation acknowledgements. Retrying the same retirement
is idempotent. Current-execution base writers are rejected after closure; generation
policy remains queryable. The existing 32 policy transitions and real Local
unchanged/selected source cleanup additionally assert empty base at completion,
before another generation begins. An intentionally unfinalized crash fixture keeps
its facts until the next full preparation.

`--request-policy` adds six whole-job retirement failures and six following full
runs across both deployments. An owned proxy affects only the final base mutation;
real Local source files have already moved. It independently verifies target bytes,
Failed generation and archived final report, explicit base-retirement cause via
HTTP 204, one task error with zero file-copy errors, and next-generation Full with
empty base and retained previous target data. This tests unconfirmed outcomes and
reporting; it does not certify a task-wide three-second recovery budget, automatic
in-process recovery of every terminal mutation, unknown older server requests,
cross-host exclusion, earlier per-file asynchronous source purge, or throughput/RSS.

### Published metadata verification cancellation

The pinned data-mover revision `378bd9e4667563ad7960ba9c91e1895fb15cd40d`
cancels in-flight read-only verification through its existing `verify_published`
entry. Its `tests/lab/run-version-metadata-local.py` runner independently checked
historical/current HEAD, tags GET and empty-plan stat stalls against an owned
versioned RustFS container: cancellation issued after 250 ms returned typed
`Cancelled` in 252–254 ms, both original versions and tags were unchanged, and
there was no payload GET or mutation. Five deterministic read phases additionally
cover independent permissions and release of pending futures. These are verification
cancellation checks, not whole-task recovery-budget or throughput/RSS acceptance.

The task-layer regression
`cargo test --locked -p app cancelled_published_verification_retains_both_copies_in_every_transfer_mode`
passed Direct/Checkpoint × quick/full with real Local payloads and a pending target
metadata role. Cancellation retained `PublishedDataMetadata` and the typed verification
cause; both source and published target content remained present.

After pinning, `--version-metadata` passed eight Single/Cluster × latest/history ×
quick/full combinations and independently checked 56 HTTP 204 callback summaries,
32 failed logical entries and 80 detailed version causes. Metadata retries made no
body replays and retained one/four destination versions respectively. These cases
retain their sources and do not establish permanent source-history deletion.

Native `make ci` (including strict all-target Clippy) and the main repository's Rust
CI command set passed; the latter includes 16 serial remote-process tests and retains
its existing nonfatal Clippy diagnostics. Local checks used rustc 1.98.1; CI's 1.95
toolchain, non-S3 protocol cancellation and formal whole-job performance/RSS A/B
were not established by this step. Architecture review kept read cancellation in
the existing backend-independent metadata verifier, generation/base lifetime in db,
and task error classification in the application. No new outward Native API was added.

## NFS 目标持久化与删源原语

`source_cleanup::real_tests::target_nfs::real_target_nfs_persistence_gates_source_removal`
是 app lib 的 ignored 真实回归，需准备独占的 NFSv4.1 目标前缀和 COMMIT 感知的私有代理。
使用 data-mover 实验室的 RPC framing／XID 代理，通过 `LAB_NFS_DIRECT_PROXY_URL`
配置目标、`LAB_SOURCE_GUARD_CONTROL` 配置该代理的 HTTP 控制地址。只影响本次测试连接。

已执行 quick／full 正常确认，以及 COMMIT 闪断、持续断连、静默超时、取消、响应丢失：
正常和恢复后确认成功才删除 Local 源文件；三个未确认场景保留源文件并返回准确的
TargetPersistenceUnconfirmed 类型。独立读取检查七个目标内容正确且无传输产物。
该测试调用生产删源原语，不运行数据库、整个 CLI 或外部 callback；不能冒充上述链路
及 pNFS DS、目录／链接持久化、吞吐／RSS 的组合验收。准备、二进制部署和精确前缀清理
由实验室执行方负责，本次前缀及私有代理均已清理。


### NFS 目录与符号链接目标

`source_cleanup::real_tests::target_nfs::real_target_nfs_nonfile_persistence_gates_source_removal`
使用同一个 app lib ignored 真实测试入口和独占目标前缀，代理选择 `--contract nonfile`
并触发实际 `lookup_path`。每种类型覆盖正常、闪断、持续断连、静默超时、取消和丢回复：
6 个正常／恢复场景确认后删除 Local 源，6 个未确认场景保留源，返回准确的
TargetPersistenceUnconfirmed 分类；服务端独立检查 12 个目标类型和断链原值。
代理确认两个实际丢回复及全程零文件 COMMIT。该测试不运行整轮 CLI、ClickHouse 或 callback。

锁定 data-mover 26063b0 和 nfs-rs 7d984b4 后，上述 12 个场景和原有普通文件 7 个场景
均已执行通过；普通文件确认了一次匹配 XID 的 COMMIT 回复丢失，独立检查 7 个目标内容。
实验独占前缀和代理已清理。目录／链接静默检测仍约 15 秒，普通文件约 18 秒；
不据此宣称统一任务网络预算、真实 pNFS DS 或整轮吞吐／RSS 已验收。

### NFS 源端受保护清理接入

`source_cleanup::real_tests::source_nfs::real_source_nfs_purge_uses_target_ready_and_original_source_facts`
是 app lib ignored 真实用例，需独立准备 `LAB_SOURCE_PURGE_NFS_URL` 和
`LAB_SOURCE_PURGE_TARGET_URL` 两个独占 NFS 子前缀。用例自行从临时 Local 源准备数据，
通过生产复制和删源原语验证 quick/full、重复清理、恢复 mtime 的等长改写、源端／目标端
改权限、非空目录拒绝及子项后删除空目录、断链的精确复制。NFSv3 和 v4.1 均已通过；
独立服务端读取核验目标内容、保留的改写源、mode 差异、相同 mtime、目录子项和链接原值，
并检查无传输产物。该用例不运行整轮 CLI、数据库或 callback。

对应 data-mover 的 REMOVE 感知私有代理分别验证 v3 五类及 v4.1 六类故障。
V4.1 闪断和丢回复恢复；v3 对已经发送的删源请求保留不确定结果，包括闪断和丢回复。
丢回复时 v3 源可已删除，不能把 Unknown 当成“源仍存在”。所有尝试均有完整的目标备份，
旧观察不会删除后续的同名替换项。静默检测仍约 v3 5 秒／v4.1 15 秒，取消约 253／252 ms。
合成 BADSESSION 不重建会话重发 REMOVE；不冒充真实服务端重启。

本轮依赖固定为 data-mover a4ecf4f、nfs-rs 1478d8b；两仓库对应 Rust 检查已完成。
原生 make ci 链接遇到磁盘空间不足后，逐组执行并补齐所有剩余测试、文档、示例及 Local E2E，
不是一次完整 make ci 命令成功。应用 workspace 检查、Clippy、测试和 16 项远程进程串行
回归通过，使用本机 rustc 1.98.1；SDK 另使用 Rust 1.95.0 完成规定 Rust 检查。
本轮实验前缀、私有服务和工作节点产物均已清理。并发源修改、统一任务故障预算、整轮
CLI／ClickHouse／callback、NFSv4.0／4.2／pNFS、掉电及吞吐／RSS 仍不在上述验收范围。
当前源清理仍位于数据库收尾后；本轮没有解除该屏障。

### NFS 元数据计划的实际请求审计

`migration_plan::nfs_real_tests::real_nfs_metadata_plan_controls_acl_reads_and_preserves_data_on_failure`
是 app lib ignored 真实用例，使用原生 `run-nfs-direct-contract.py --contract metadata-plan
--test-name <该用例名>` 及独占源前缀，通过 `LAB_METADATA_PLAN_TARGET_URL` 指定独立目标子前缀。
对象目标计划使用 S3 能力投影，实际复制目标为 NFS；该用例不宣称已运行 S3 服务。

锁定 nfs-rs fdc541c、data-mover a655be9 后，实际 GETATTR／LOOKUP／READDIR 掩码审计
记录 118 次属性请求、零隐式 ACL、三次显式 ACL 查询和两次注入的权限拒绝。
在切换 ACL 故障阶段前，代理先确认省略阶段无任何 ACL 查询；拒绝响应保留真实 RPC、
SEQUENCE 和 PUTFH 身份，且仍可在同一连接上读取后续 ACL。
源元数据失败时实际 copy_file 完成全部字节，返回 PublishedDataMetadata 和
SourceMetadataAcquisitionFailed 的 ACL／PermissionDenied 原因；目标数据、源数据保留，
源清理被拒绝。需要的 ACL 原值可通过 EntrySnapshot 往返并用于复制。
独立服务端读取核验三组源／目标内容、完成的权限模式及无传输产物。

同一 SDK 通过原有 v3 五类、v4.1 六类受保护删源网络回归，元数据夹具在故障后仍完整。
应用精确 Git 依赖再运行审计与 v3／v4.1 原有源／目标复核用例，通过对应 Rust CI 和
16 项远程进程串行回归（本机 Rust 1.98.1）；原生完整 make ci 通过。
本轮不覆盖整轮 CLI、真实 S3 目标、ClickHouse／callback、统一任务网络预算、掉电或吞吐／RSS。
本轮独占前缀、私有代理及 HTTP 服务、工作节点产物均已清理。

### S3 目标持久化与已复制项删源

`source_cleanup::real_tests::target_s3::real_target_s3_persistence_gates_unchanged_source_removal`
是 app lib ignored 真实用例，使用 data-mover 的隔离 RustFS runner：
`tests/lab/run-source-purge-local.py --namespace-persistence`，并以
`LAB_S3_PURGE_TEST_BINARY` 指定编译好的 app lib 测试程序，`LAB_S3_PURGE_TEST_NAME` 指定上述精确名称。
先用 `--list` 确认测试存在，防止过滤为空被当作通过。runner 创建独占版本化桶和前缀，
播种三份相同内容的目标版本及哨兵；Local 源文件与既有目标一致，用于模拟本轮跳过数据的项。
测试只在 Namespace::persist_observed 调用前启用代理故障，前面的目标属性校验正常运行。

锁定 data-mover fbf59c0 后，成功、权限拒绝、持续断连、静默超时、取消、闪断恢复和超时恢复
7 类场景通过。只有目标确认成功才删除 Local 源项；持久化拒绝保留源内容，错误携带
TargetPersistenceUnconfirmed 及 PermissionDenied／Connectivity／Timeout／Cancelled。
断连约 6 ms、静默检测约 1 s、取消约 256 ms；瞬时失败保留源后，用例显式恢复代理并重试才删除；不冒充应用自动重试已验收。
独立 boto3 检查确认各前缀的三个原版本与哨兵保持不变，代理累计数据 GET 和写操作均为零。
原生存储 role 的另 7 类真实回归通过；S3 库 408 项测试通过（2 项 ignored），完整 make ci 通过。
应用 workspace 检查、Clippy、测试和 16 项串行远程进程回归通过（本机 Rust 1.98.1，保留已有警告）；
最终 workspace 测试程序另复跑同一物理矩阵通过。容器、网络命名空间和 tmpfs 随 runner 退出清理，未修改防火墙或共享服务。

本用例不执行整轮 CLI、数据库或外部 callback，不覆盖非版本化真实目标、其他 S3 厂商、
NAS 桶持久化、掉电、全历史永久删除、统一网络预算或整轮吞吐／RSS。已有目标的内容由独立
夹具核验，此用例验证删源门槛，不冒充完整数据复制／全量数据校验。当前删源仍等待数据库收尾。

### 全量 worker 的共享失败与初始化取消

`sender::cancellation_tests` 在旧实现中分别复现预取消仍发请求、等待扫描输入不退出、
发送背压无法取消三个 500 ms 超时；修复后与原有三个 Sender 回归一起通过。
作用域复用 CopyDatabaseExecution，模拟一个 worker 的原始 TimedOut 终态错误后，
等待发送的 worker 及时退出；两个结果持有同一个原始错误，活动计数归零、发送 future 释放。
该确定性作用域用例只构造数据库对象，不发查询，不冒充实际 ClickHouse 故障。
另两项单测覆盖挂载槽位等待取消、初始化失败后在重试预算耗尽前取消，并核验配额释放。

`python3 tests/lab/run-copy-worker-cancellation-local.py --binary <app-lib-test-binary>`
运行精确 ignored 用例
`orchestrator::cancellation_tests::real_storage_initialization_observes_cancellation_without_source_or_object_mutation`。
runner 先用 --list 确认测试存在，然后创建独占 RustFS 容器、桶、哨兵版本和 Local 源文件。
正常初始化发送一次真实 S3 请求；取消场景在请求抵达代理后等待 250 ms 再取消，
未返回响应的初始化总计约 301 ms，取消发出后不到一秒返回 Cancelled。
父进程独立核验源内容、mode／size／mtime／ctime 和哨兵原版本／内容未改变，
零数据 GET、零修改请求、无未完成上传，测试进程退出后代理连接全部关闭。
容器、网络命名空间、tmpfs 和临时源文件随 runner 清理，未改共享服务或防火墙。

全量 Sender／Receiver 的终态失败接入既有共享复制执行域，等待输入、命令发送、
挂载配额、初始化与退避均响应已有取消信号；启用该执行域的增量初始化同样接入。
本机 Rust 1.98.1 的 workspace 检查、Clippy、测试和 16 项串行远程进程回归通过，已有警告保留。
上述物理验证限于 Local／S3 的初始化，不代表其他协议初始化的后台资源回收、
常驻进程内 SDK 池立即关闭、复制中断网的统一检测／恢复预算、实际 ClickHouse／callback
或整轮吞吐／RSS 已验收。没有数据库执行域的任务仍按原配置工作。
这一步补齐清理队列所依赖的取消链路，源清理目前仍等待数据库收尾。

### 增量搬迁跳过项的数据全量校验

`source_cleanup::content_tests` 在旧实现复现相同长度、相同 mtime 的坏目标被放行并删源。
修复后，显式全量校验复用原生完整性比较，绑定原始源观察及已确认目标身份，
检查实际源读取量和两端 BLAKE3；源／目标读取失败保留原始类型、出错侧与诊断。
新增 5 项确定性测试覆盖内容损坏、quick 零内容读取、空／非空文件的 full 双端读取、
双方同时短读、源／目标读取权限／连接／取消错误及读取中取消，原有清理测试继续通过。
全量复制沿用传输阶段的数据校验，增量删源补齐跳过／仅修改属性条目；
两种清理入口都接入既有复制执行的取消信号。

`python3 tests/lab/run-source-content-local.py --binary <app-lib-test-binary>`
运行精确 ignored 用例
`source_cleanup::content_tests::real_existing_s3_content_verification_gates_source_removal`。
独占 RustFS 的 9 类验证包括 quick、full、相同大小的错误内容、403、持续断连、
静默超时、取消、闪断后显式重试及超时后显式重试。quick 零数据 GET，正常 full 一次
固定最新版本 ID 的 GET；断连约 15 ms、配置的静默检测约 1 秒、取消约 270 ms。
重试由用例显式恢复代理后发起，不作为产品自动恢复已验收的证据。
父进程用独立 boto3 检查三个版本的内容／身份及哨兵未改变，无修改请求和未完成上传，
测试进程退出后连接全部关闭；源文件只在相应校验成功后删除。

`python3 tests/lab/run-notification-archive-local.py --copy-content` 覆盖真实 Local、
Single／双副本 ClickHouse 和 HTTP 204 callback，共 12 轮迁移。
每组先 full 保留源，再 incremental 删除源，分别验证未变化、目标损坏、
源仅改 mode 且目标损坏。第二轮数据传输均为零；4 个损坏项保留两端，2 个正常项删源。
独立父进程检查文件及 4 条逐项 callback 原因；Rust 用例核验终态、空 base、
累计 task_error_count／target_preserved_count 和数据库保存的最终报告。
范围级计划保留事件独立核验，不能混入逐项保留数量。
两个 runner 都清理各自的容器、网络命名空间和临时数据，未改共享服务或防火墙。

这些证据不覆盖所有协议、保留源增量的 full 校验、自动网络恢复、掉电或整轮吞吐／RSS。
增量中本轮已复制的条目仍可能多一次 full 回读；尚未以持久化的数据校验证据去重。
未新增本地扫描暂存，删源与数据库收尾的解耦仍待实现。

后续固定 data-mover `bea56a42`，完整性引擎分别核对源／目标已知长度，
短读或超出声明长度均返回带出错侧的 Read／Protocol 原因及期望／实际字节数，
双方摘要相同也不能绕过；未知长度维持 EOF 比较，quick 不新增内容读取。
原生 44 项完整性测试与完整 make ci 通过，无新增公开接口或 SDK 改动。
主仓库只替换 13 个 git 包的精确来源及两个 manifest 引用，其余依赖图保持不变。
主仓库短读用例核验该类型和诊断继续保留两端；锁定后的真实 S3 九类回归、
Single／双副本的 12 轮迁移及 HTTP 204／报告归档再验证通过。
短读由确定性 role 注入，真实网络矩阵覆盖正常读取、权限、断连、静默、取消与恢复，
未将两者混称为真实网络截断已验证。

### 全量删源的有界就绪窗口

最新实现对启用数据库的全量删源增加一个普通条目窗口：仅取最先完成的
`concurrency.clamp(1, 64)` 次候选，不随总 entry 数增加编码次数；Arc 保留原观察，
原值快照临时编码后立即释放，按编码长度四倍加 1024 的权重控制总准入不超过 8 MiB。
这不是严格 RSS 上限，临时编码、allocator 及现有广播队列均计入实测。
所有复制与枚举成功且没有选中目录／历史流程时，窗口先核验目标、确认持久性、
复核源并受保护删除，与数据库最终冲刷并行。范围外、超预算、目录及历史流程
仍沿用原收尾路径；本项没有实现全部条目的异步删源，也没有新增磁盘暂存或公开存储接口。

`python3 tests/lab/run-notification-archive-local.py --copy-ready-cleanup`
运行精确 ignored 用例
`copy_execution::ready_cleanup_tests::real_full_move_ready_window_does_not_wait_for_fact_receipts`。
真实 Local＋Single／双副本 ClickHouse 共 12 轮，覆盖成功、INSERT 转发前 403、
目录退回原流程、窗口外条目、数据读取失败、元数据写入失败。
父进程在真实事实 INSERT 尚未转发时核验窗口内源已删除、目标数据正确；
窗口外及目录项保留到回执释放。数据失败的未完成目标清除，元数据失败的两端数据保留。
权限失败使用独占目录中 uid/gid 65534 子进程，不修改共享权限。
HTTP 204 callback 核验具体原因，数据库核验失败／成功代次、最终报告及空 base。

同一命令也在两种 ClickHouse 部署上各运行 5 轮增量搬迁。先保留源建立基线，再新增和
修改各一个普通文件。扣住本轮的 base INSERT，独立核对两个目标的内容、新增和修改的
源文件已清理、未变化的源文件仍存在。随后分别放行、返回 403、在写入成功后丢弃回执、
持续断开或静默阻塞。代理检查丢失回执后重投使用同一去重标识，callback 和已归档终态
均核对本轮字节与新增／修改计数。持续故障时检查已完成的搬迁保留、未变化的源文件仍在，
并记录从故障注入至第一次重试及终态返回的耗时。增量范围为 Local 目标和有界提前清理窗口。

在上述命令末尾加 `--multi-shard` 可用三个分片、每分片两个真实 ClickHouse 容器复跑。该选项
也适用于 `--consumer-recovery` 和 `--copy-data-cancel`。集群目录按作业路由，单个作业的事实位于
同一分片。测试使用私有网络和独占数据库，退出时销毁容器。

跨进程接管通过实际 CLI 验证，使用 `LAB_TERRASYNC_BIN` 指向本次构建的二进制，使用
`LAB_TAKEOVER_LOG_DIR` 指定一个尚不存在的子目录保存各进程日志。
需要 Linux Docker 及进入容器网络命名空间的权限（`nsenter`）。nightly 的 `copy-takeover` 作业在独立的
GitHub 托管 runner 上构建 release CLI 并运行此测试，上传包含总日志及逐进程日志的 `copy-takeover-*` 产物。

```sh
cargo build --locked --bin terrasync
LAB_TERRASYNC_BIN=/absolute/path/to/target/debug/terrasync \
LAB_TAKEOVER_LOG_DIR=/absolute/path/to/new-process-logs \
python3 tests/lab/run-notification-archive-local.py --multi-shard --copy-takeover
```

用例分别扣住旧进程的事实 INSERT 或代次提交 INSERT，核对已搬迁目标和最新代次，再从相同或不同作业目录
启动第二进程，跨目录时另验证普通扫描不能与复制争用该作业。强制结束旧进程后从竞争进程的目录接管，
核对全量恢复、实际传输字节和已搬迁目标，再向真实库重放旧请求并要求成功入库。通过直接读表核对事实和有效
状态，再运行两轮增量，验证目标保留和真实删除。保留同一路径的新旧底层物理行，确认真实删除后一并清理旧事实。
Direct、Checkpointed 和迟到状态场景均覆盖单机及集群各分片，副本路由由直接读表验证。违规时退出非零并保留逐进程日志。
该实验覆盖同一 Linux 网络命名空间中使用同一 ClickHouse DSN 的多个进程。

`python3 tests/lab/run-source-ready-nfs.py` 运行精确 ignored 用例
`copy_execution::ready_cleanup_nfs_tests::real_nfs_full_move_ready_window_and_combined_failure`。
复用实验室授权的 PVE SSH／qm guest exec 通道，仅创建随机独占 NFS 前缀；
两个隔离 ClickHouse 容器使用 localhost 随机端口，不修改共享服务、导出或防火墙。
真实 v3／v4.1、quick／full、Single／双副本的 8 轮正常用例，在 INSERT 转发前通过
服务端独立读取核验源消失、目标数据与 mode 正确。另两轮 v4.1 用私有 RPC 代理拦住
源 REMOVE，在确认该请求到达后拒绝事实写入：源未删除、目标正确，任务保留数据库原因，
callback 同时包含具体源路径及 source_deletion_unconfirmed／Unknown，最终报告入库。
这些用例不把取消的未知结果当作源仍存在的证据，源是否存在由独立服务端检查确认。
所有独占前缀、容器、代理、临时程序及文件均在 runner 退出时清理。

本项正常开销 A/B 使用同机 Rust 1.98.1、dev debug=0、incremental=0，
基线 `da72f50`，变更版只增加就绪窗口产品逻辑。每种场景以
before／after／after／before／before／after 顺序各运行三次；
每轮新建 3000 个 1 KiB 文件，目录场景为 100×30，创建数据不计时；
并发 16、批次 100000、unordered、Direct、quick，使用同一隔离 Single ClickHouse。
`/usr/bin/time` 记录进程整轮耗时与峰值 RSS，另以 50 ms 间隔采样实际 CLI 的 RSS；
时间包含数据库与删源收尾，独立核验数据、源删除及 base 清空。

| 场景 | before 耗时三次／秒 | after 耗时三次／秒 | before 峰值 RSS 三次／KiB | after 峰值 RSS 三次／KiB |
| --- | --- | --- | --- | --- |
| 保留源、不落库 | 0.54 / 0.43 / 0.53 | 0.53 / 0.53 / 0.43 | 166492 / 174256 / 172148 | 164468 / 169868 / 167344 |
| 平铺删源、落库 | 5.53 / 5.27 / 5.10 | 5.66 / 5.33 / 5.20 | 199000 / 214536 / 207764 | 203188 / 205612 / 202844 |
| 目录删源、落库 | 5.07 / 4.87 / 4.97 | 4.87 / 5.11 / 4.97 | 234176 / 225980 / 223844 | 226164 / 230728 / 221612 |

耗时中位数变化分别为 0%、+1.14%、0%，RSS 中位数变化为 −2.79%、−2.20%、+0.08%。
原始样本与每轮日志位于 `/tmp/terrasync-ready-window-perf/`；基线／变更程序 SHA-256
分别为 `67cac56a0443fa9fbd46b9609c626741e6590f3329362dee7d7eeeaef1b47134`、
`5ee66e95d0c00dbee7f5bbd03182cb859b7dde4e91a01372c15a74ba2ba31e97`。
这一小样本只能限定本项 Local 正常开销，不能证明加速、release 达标或约 200 MiB 目标已满足；
目录场景仍超过该目标。它与 PR #135／#137 的纯 NFS 扫描基线工作量不同，不直接计算差值。
大规模、续扫、NFS 性能、目录／历史异步删源、全部后端及统一网络预算仍需后续验收。

本项最终 Rust 检查已通过：fmt、workspace/all-targets check 与 Clippy、workspace 测试、
16 项串行远程进程回归；本机使用 Rust 1.98.1，保留已有大型错误类型等警告。
架构审查结论：迁移时序与组合错误在 app，目标持久性和受保护删除复用 data-mover，
NFS 协议继续由 backend／SDK 负责；未增加公开存储入口，也未改变 SDK。

### 普通未版本化 S3 源的受保护清理

`python3 tests/lab/run-notification-archive-local.py --copy-object-cleanup`
运行精确 ignored 用例
`copy_execution::object_cleanup_tests::real_unversioned_object_source_cleanup_and_failures`。
使用已有隔离 RustFS 与 Single／双副本 ClickHouse 夹具，只访问独占桶／前缀和本机 HTTP 204 callback。
先在旧代码复现未开版本的 `tree/file` 被 PermanentVersionCleanupUnavailable 拒绝清理，
再发现并修复 Native 对 LIST 毫秒／HEAD 秒直接比较的误判；最终固定 data-mover `cd1ede0b`。
依赖图只替换 13 个 Git 包的精确来源及两个 manifest revision，SDK 保持 `fdc541cc`。

普通无历史对象复用 Namespace::purge_observed；虚拟前缀不对应删除实体，不递归处理。
没有选中目录／历史且符合窗口预算时，对象源也能先于入库确认清理；
真实目录标记与历史条目仍明确报错保留，不当成普通当前对象删除。
Native 按两者较粗的实际精度比较时间，保留原始时间、身份／ETag／大小及缺失时间检查；
源元数据仍复核，无额外请求或新公开接口。排除并发写入的要求不变，同一 HEAD 时间刻度内
同 ETag 的变化不能由这个头检测；不宣称原子 compare-and-unlink。

36 轮真实迁移包括 12 轮正常、12 轮失败和 12 轮同 job ID 恢复。
正常覆盖 Local／普通 S3／版本化 S3 目标和 quick／full，两种数据库部署均检查最终字节、
对象 tags、源实际存在性、代次和报告。Local 目标没有源 tags GET，非空尾斜杠对象保留并交付
跳过事件；对象目标保留完整 key。4 轮普通 S3 目标的事实 INSERT 在转发前等待真实源 DELETE，
父进程核验源消失、目标字节／tags 正确后才放行，不以请求发出代替实际就绪。

故障注入限于源 DELETE 的 403、持续断连、静默超时、丢回执，以及目标 CopyObject／tags PUT 的 403。
数据／tags 失败均未发源 DELETE，每个失败 entry 计一；tags 失败的两端数据保留。
数据拒绝发生在 CopyObject 发布前，目标不存在。后续接入的本次写入回执清理消除了
未写入情况下无意义的 Unsupported 次因；发布后数据校验失败的清理另见下节。
8 条源清理未确认事件带 `payload` 路径及 Unknown；父进程另外证实拒绝／断网时源仍在，
丢回执时源已消失。12 次下一代全部为 Full，源已消失时目标保留。
HTTP 204 接收具体后端、阶段、路径及 HTTP 403／AccessDenied 原因；36 份最终汇总保存到数据库，
所有删源代次 base 清空。S3 诊断保留脱敏的状态与错误码，不要求回传任意原始服务端消息。

从首个注入请求到最终汇总，断连约 145／237 ms、静默超时约 1097／1244 ms；
Native 普通桶／版本化桶另各通过 7 类真实角色回归，取消约 253 ms。
普通对象成功清理仅一次 payload HEAD、零数据 GET；闪断／丢回执的角色恢复由测试显式重试。
Native 完整 make ci 通过；这些秒级值使用测试显式超时，不作为默认／统一任务预算已满足的证据。
本项不认证其他 S3 厂商、NAS、真实掉电、历史永久删源、目录标记或整轮吞吐／RSS；
也不把两次迁移代次之间的恢复当作同代次自动重试。

锁定后的最终程序再次运行全部 36 轮通过，版本化目标的每个原始 key 只有一个版本且无删除标记。
主仓库 fmt、workspace/all-targets check 与 Clippy、workspace 测试及 16 项串行远程回归通过；
app 默认库测试 328 项通过、45 项 ignored，物理夹具单独显式执行，不能把其他 ignored 当作通过。
本机 Rust 1.98.1，保留已有警告。架构审查确认：任务层仅决定就绪与清理时序，
精度处理和精确对象删除均在 S3 backend，不增加任务层协议分支或新存储入口。

### S3 Direct 发布后数据失败的本次输出清理

固定 data-mover `7d9016c1`，只替换两个 manifest revision 与 13 个 Git 包来源；
SDK 仍为 `fdc541cc`。复用已有 FailedDataCleanup::RemoveOwned 和 remove_failed_direct，
没有新增公开函数或类型。backend 私有状态保存原始写入成功回执，按回显的不可变版本 ID
精确删除，或对可变／null 槽 fresh HEAD 核对大小与 ETag 后删除。推断出内容相同的旧对象、
未知写入、缺少版本证据和已替换目标均不能授权删除；NAS 输出尚未认证，仍明确保留。
默认 Keep、普通 discard 和元数据失败不删除最终数据。可变 HEAD＋DELETE 不是原子条件删除，
继续要求排除并发写入。

原生 60 个隔离 RustFS 场景通过，覆盖小文件原生复制、12 MiB multipart、非空尾斜杠流式 PUT、
未开版本／开版本目标，以及回读损坏、回读 403、数据／tags 写入 403、清理 403、持续断连、
静默超时、丢 DELETE 回执和在途取消。独立读取确认源各版本与旧目标版本保留，只清理本次
有确认回执的输出，无删除标记。尾斜杠在 RustFS 开版本桶中实际为 null 单槽，如实按可变槽验证。
现有 VerifyRequest.cancel 覆盖在途只读 HEAD／GET，真实取消返回 Cancelled，不再等 SDK GET 超时。
不确定 DELETE 保持 Unknown，不因父进程另查到目标已消失就伪报确认成功；未增加盲重试。

上节原有 36 轮真实任务扩为 48 轮全部通过：Single／双副本 ClickHouse 各新增发布后回读损坏、
失败输出清理 403、清理成功但回执丢失三种失败和同 job ID 下一代恢复。
六个新增失败均未发源 DELETE，各按一个 entry 计错且 task_error_count=0。
HTTP 204 callback 保留原始 checksum mismatch；清理失败同时带 PermissionDenied 或 Connectivity／Unknown，
每个有确认回执的输出恰好一次目标 DELETE。成功清理及丢回执后的实际目标不存在；清理 403 保留目标；
下一代均走 Full 并完成复制与源清理。48 份最终报告存入 ClickHouse，删源代次 base 清空。
测试损坏的是代理回读响应，不冒充真实磁盘损坏。最初夹具只处理 HTTP 200，漏掉实际 Range GET 的
206；修正并审计实际 206 注入后才计上述通过。首次离线启动缺少新 Git revision，先 fetch 后重跑。

架构审查：任务层仍负责就绪、统计、代次和通知；通用传输引擎保留显式清理授权；
写入归属、版本检查、精确 DELETE 和读请求取消在 S3 backend，无新任务层协议分支或 SDK 修改。
原生完整 CI 范围通过：初次 make ci 的批量链接因本机构建盘耗尽失败；前置守卫、feature 和严格 Clippy
已通过，剩余 52 个等价顺序步骤及最终 fmt 全通过。ignored 的其他环境不算通过。
本项不等同所有厂商、NAS、物理掉电、全部源历史清理、统一网络预算或整轮迁移性能验收。

主项目最终 fmt、workspace/all-targets check 与 Clippy、workspace 测试和 16 项串行远程进程回归均通过；
本机 Rust 1.98.1，保留已有警告。默认 app 库测试 328 项通过、45 项 ignored；
上述实际外部夹具单独显式执行，未执行的 ignored 不计通过。

本项正常路径同机 A/B 六轮，基线为主项目 `4100426`／Native `cd1ede0b`，变更为 Native `7d9016c1`。
两份 dev debug=0／incremental=0／Rust 1.98.1 程序，在同一独占 2 CPU RustFS 服务上按
before、after、after、before、before、after 交错执行；每轮 1000 个 1 KiB 普通 S3 对象，
源保留、无数据库、unordered／Direct／quick、并发 16、批次 100000，包含原始 tags。
计时仅覆盖完整 CLI 进程；轮后独立核验全部目标字节／tags、源项保留及无残留 upload。

| 样本 | before 耗时／秒 | after 耗时／秒 | before 峰值 RSS／KiB | after 峰值 RSS／KiB |
| --- | --- | --- | --- | --- |
| 1 | 4.50 | 4.40 | 197172 | 194456 |
| 2 | 4.30 | 4.30 | 195384 | 196028 |
| 3 | 4.39 | 4.30 | 197376 | 199172 |

耗时中位数 4.39→4.30 秒（−2.05%），RSS 中位数 192.55→191.43 MiB（−0.58%）。
这些小样本显示本项未出现明显正常开销增长，不能据此宣称加速、release 或整轮规模达标；
不替代此前仍超约 200 MiB 的 Local 目录场景或大规模／其他后端验收。
原始样本、配置、运行日志与程序位于 `/tmp/terrasync-s3-direct-cleanup-perf/`，
基线／变更程序 SHA-256 分别为 `b8a57a2d85059136721041d70c86d2ddb34d863e00069c790dc6c40a5f07b028`、
`cd743d8c7d2862e0d90b17aa53fe6ff9194a648f66fafbc14388113e316630fc`。
该性能夹具及功能夹具的独占容器均已清理。

### Direct 历史发布的版本确认与顺序阻断

固定 Native `7f029054`，复用已有 PublicationEvidence／TransferOutcome.destination_version，
以及本次写入回执的私有状态，没有新公开函数、类型或 SDK 修改。Direct 返回的创建版本只来自
本次成功写入的原始回执，内容相同的旧版本或 HEAD／LIST 推断版本不能作为本次创建证明；
数据可能已满足普通当前复制要求，而创建版本仍为 None。正常有版本回执的写入继续返回实际 ID。
普通发布只增加一次现有私有回执锁的读取，不增加存储请求或按条目累计的常驻集合。

任务层区分两类缺失：仅重试元数据时缺少既有目标绑定仍走原来的属性错误；新复制历史数据
缺少确认的目标版本则使用既有 HistoricalPublicationUnconfirmed，阻止该 entry 后续版本的数据发布。
若已发布数据的元数据同时失败且目标版本也未确认，复用 TransferAndMetadata 保留两个有类型原因，
按数据发布未确认阻断；有确认版本的普通元数据失败继续保留数据并允许后续历史版本的数据复制。
不会把元数据失败改成坏数据删除，也不会保存没有确认映射的完成条目。

`--version-catalogue` 现有真实夹具扩为 26 轮：两种数据库部署各保留原有 9 轮；新增同 job ID
三代“缺少版本回执”的失败，以及一轮同时发生 tags 403 的失败。代理在本次 CopyObject 成功响应
移除版本头，并拒绝目标版本 LIST，独立 boto3 通过真实服务读取，核验每种故障 key 都只有一个
最老版本的数据（payload-0）、没有删除标记；仅发一个 CopyObject，后续五个源版本被阻止。
第二、三代为 Incremental，既不重放旧数据，也不增加目标版本；源六个历史版本保留。
既有最新删除标记保留目标场景继续通过，测试自己写入的一个源删除标记被独立区分。

26 份最终报告已保存，HTTP 204 接收明细；16 轮成功、10 轮失败，每个失败 entry 只计一且
没有重复 task_error。共有 52 条版本错误细节、42 条后续版本阻断；组合故障同时保留历史发布
未确认及 PutObjectTagging 的 PermissionDenied／HTTP 403。失败的首次全量没有成功完成条目，
现有 Full.scanned_files 仍是成功条目计数；增量扫描数为一。本项核验失败 entry 计数为一，
不声称已统一全量／增量扫描与完成数的命名／呈现。

先在旧代码复现缺少目标版本被误归为 metadata-only，后续版本仍发布；修改后先运行 24 轮通过，
再在新依赖下加入组合故障，26 轮通过。扩充的测试 future 曾触及测试线程默认栈，
改为 Box::pin 大型测试 case 后复跑通过，未提高产品运行时栈限制。
Native 已有 13 项 Direct 单元回归、13 项真实原生复制、60 项真实失败数据清理、
完整 CI 范围分步和最终严格 Clippy 通过。在本步骤完成时，checkpointed ambiguous-completion 仍保留旧断言；
AtomicReplace／Checkpointed 的推断版本及恢复证据由下一节继续处理；
历史永久删源仍明确拒绝，不以本项 Direct 确认代替全部历史目标就绪证明。

新依赖与本次任务层修改的最终 Rust 五项检查均通过：fmt、workspace/all-targets check、
workspace/all-targets Clippy、workspace 测试、16 项串行远程进程回归。
本机 Rust 1.98.1，保留已有警告；app 328 项库测试通过、45 项 ignored。
另外再次完整运行普通对象清理的 48 轮真实任务，通过 Source 删除、数据／元数据失败保留、
清理未知结果、HTTP 204、最终报告和 base 清空的独立核验。外部夹具之外的 ignored 不计通过。
架构审查确认：S3 原始回执仍由 backend 单一私有状态管理；任务层只判断本次历史迁移是否有
所需目标绑定及错误是否阻断后续版本，复用已有类型保存组合原因；HTTP／版本列表注入只在测试夹具。
新增大型测试 case 的 heap future 不改变产品执行策略或运行时栈配置。

本项另以 `eace467`／Native `7d9016c1` 为基线，和 Native `7f029054` 加本次任务层修改，
在同一独占 2 CPU RustFS 上进行六轮正常 A/B；复用上一节配置：每轮 1000 个 1 KiB 对象含 tags，
保留源、无数据库、unordered／Direct／quick、并发 16、批次 100000，dev debug=0／incremental=0／
Rust 1.98.1，两份程序按 before、after、after、before、before、after 交错。

| 样本 | before 耗时／秒 | after 耗时／秒 | before 峰值 RSS／KiB | after 峰值 RSS／KiB |
| --- | --- | --- | --- | --- |
| 1 | 4.40 | 4.50 | 194620 | 196792 |
| 2 | 4.50 | 4.60 | 196516 | 193984 |
| 3 | 4.50 | 4.50 | 196444 | 193512 |

耗时中位数均为 4.50 秒，RSS 中位数 191.84→189.44 MiB（−1.25%）。
计时仅覆盖完整 CLI，轮后核验全部目标数据／tags、源保留及无残留 upload。
本项小样本未见明显正常开销增长，不作为加速、release、大规模历史或全部后端达标结论。
完整样本、配置、程序与日志在 `/tmp/terrasync-direct-version-perf/`；两份程序 SHA-256 为
`cd743d8c7d2862e0d90b17aa53fe6ff9194a648f66fafbc14388113e316630fc`、
`eb508dce9ce5c321db34f392918d8781e4498672019d52eb5b18e710c73f7b96`。
功能和性能夹具的独占容器均已清理。

### 全部 S3 发布模式与 checkpoint 的创建版本证据

后续固定 Native `8bfa329c`，将同一约束扩展到 AtomicReplace／Checkpointed 的普通、流式、
最终路径和恢复路径。现有 S3WriteFacts 分开保存观测到的读回版本与本次写入原始回执：
HEAD／LIST 推断只固定读回，不授予创建版本证明。Direct 继续复用原有私有清理回执。
不增加公开接口、SDK 修改、普通路径存储请求或按总 entry 数保留的集合。

私有 S3 checkpoint 格式更新为 DMS3CP02，分别保存读回 pin 和原始回执；拒绝旧布局、截断、
超过 4 KiB 或相互矛盾的版本证据。保存过的真实成功回执可以恢复创建版本，Ready 或丢回执后
推断完成的恢复则仍为 None。恢复保留实际观测版本，不能把相同内容的替换版本当成原目标。
四处恢复成功日志保留原始有类型协议原因；已恢复错误不会进入 callback 错误统计。

原生 backend 414 项通过／2 项 ignored、transfer 329 项通过／1 项 ignored；完整 CI 范围
分步完成 52 项，最终严格 Clippy 通过。真实 RustFS 原生复制 33 场景覆盖 Direct 13、
AtomicReplace 10、Checkpointed 10：正常 quick／full、拒绝、断连、静默超时、闪断后显式重试、
丢响应、相同旧内容及缺失版本头。独立核验内容、版本数量、源和哨兵保留、无 marker／残余 upload；
恢复日志含原始 Connectivity。非 Direct 的元数据拒绝及在途取消不在这 33 场景内，
本项不宣称统一操作预算或这两个分支已经修复。

另以生成的 9 MiB 源和真实 S3 目标运行五个 checkpoint 场景：保存 ACK、Complete 响应丢失、
发布指针拒绝后的 Ready 恢复，以及两种确认状态下的同内容版本替换。确认与未确认的版本返回值
符合上述约束，替换均返回 Conflict；每次只创建一个 upload、上传两 part、完成一次，
恢复不重传。独立读取所有目标版本及哨兵，核对状态根清理／保留和没有未完成 upload。
这是目标角色及实际 checkpoint 恢复验证，不能替代完整 CLI、数据库、callback 或掉电验收。
原有 Direct 失败数据清理 60 场景亦完整复跑通过。

主项目版本目录真实回归已扩为 28 轮：Single／双副本数据库各新增一轮 Checkpointed 历史发布
缺少版本头的失败；该分支允许正常版本 LIST，验证观测结果不能冒充创建回执。独立核验每个
故障目标只有 payload-0 的一个版本，源六个版本保留，后续五个版本被阻止。其余 26 轮亦重跑，
共 16 轮成功、12 轮失败；每个失败 entry 计一、task_error 为零，64 条版本明细包含 52 条阻断，
组合故障仍保留发布未确认及 tags PermissionDenied／HTTP 403。28 份最终报告已入库，HTTP 204
接收全部明细；缺失回执的任务没有伪造 base 完成事实。普通对象清理 48 轮亦完整重跑通过。
新增小对象 Checkpointed 案例走原生复制而非大文件数据续传；真正恢复证据由上述五个目标角色
场景分别验证，未将二者混称完整 CLI 断点续传验收。

扩充后端到端夹具再次触及默认测试线程栈，将三个测试辅助函数中的大型 orchestrator future
放到堆上后，默认线程栈完整通过；未修改生产执行策略或栈限制。主项目 fmt、workspace/all-targets
check 与 Clippy、workspace 测试、16 项串行远程进程回归通过，本机 Rust 1.98.1，已有警告保留；
fixture 调整后补跑 app/all-targets Clippy。架构审查：读回 pin／原始回执及 checkpoint codec
集中在 S3 backend，任务层沿用历史绑定检查，ClickHouse／callback 留在主项目；未引入跨层
协议判断、新的恢复循环或公开入口。历史永久删源尚未接通。

本项以主项目 `efac930`／Native `7f029054` 为基线和本次固定 `8bfa329c` 的程序交错执行六轮
正常 A/B。仍为独占 2 CPU RustFS、每轮 1000 个 1 KiB 对象含 tags、保留源、无数据库、
unordered／Direct／quick、并发 16、批次 100000，dev debug=0／incremental=0／Rust 1.98.1；
计时前已结束本项重型构建和故障夹具。

| 样本 | before 耗时／秒 | after 耗时／秒 | before 峰值 RSS／KiB | after 峰值 RSS／KiB |
| --- | --- | --- | --- | --- |
| 1 | 4.65 | 4.30 | 192696 | 190424 |
| 2 | 4.40 | 4.50 | 186984 | 191600 |
| 3 | 4.49 | 4.50 | 182056 | 185644 |

耗时中位数 4.49→4.50 秒（+0.22%），RSS 中位数 182.60→185.96 MiB（+1.84%）。
每轮完整 CLI 结束后独立核验全部目标字节／tags、源保留及无未完成 upload。
该小样本正常 Direct 路径没有明显开销增长，不作为 Checkpointed／AtomicReplace 吞吐、
release、大规模历史、全部后端或约 200 MiB 目标的整体验收结论。
样本、配置、程序和日志保存在 `/tmp/terrasync-all-policy-version-perf/`；程序 SHA-256 为
`eb508dce9ce5c321db34f392918d8781e4498672019d52eb5b18e710c73f7b96`、
`9c9353fe01172d365de6218fa4380f46b6cca23ca4a3171f73c10ebf25fef4ec`。
本项独占容器全部清理，未修改 nfs-rs。

### 流式回读复制事实与精确目标绑定

扩展已有 `query_current_copy_entries`，返回已有任务类型 CopyCompletion：选中源版本的原生
snapshot、原始元数据、实际 copy_target_version 和 Ready 状态共同交付。普通目录查询仍返回
ObservedEntry，保持原有最小投影。两个查询共用过滤、游标和恢复逻辑；当前执行查询依旧只交付
copy_path_confirmed 且本次 copy_execution_id 选中的事实，不依赖 C/W 相等授予清理权限。
SQL 在原有子目录优先、路径排序之后增加 history_segment／history_index，按源历史顺序交付。

数据库内部新增私有行解码，不新增公开接口或新的任务 DTO；源版本必须与单版本 snapshot
一致，错误保留 ObservationProjectionMismatch。缺失／空目标绑定继续为 None，不能从源版本、
当前目标或历史列表推断。生产代码不保留全库或整个 key 的常驻明细，不逐版本追加数据库查询；
目录收尾和普通源清理已更新调用契约。data-mover 和 nfs-rs 未修改，历史源清理仍保留原安全拒绝，
不能把“可回读绑定”当成“全部目标历史已就绪”。

新增三个行解码单元测试通过，db 库 140 项通过／40 项 ignored；目录收尾四项单元回归通过。
真实 Single／双副本各验证最新目标和历史目标，共四组：最新投影一行、历史投影三行，包含
null 删除标记、分段顺序和原始 tags；DataOnly 不出现在就绪查询中，缺少绑定不会补造，
下一执行未重新选中前不返回旧事实，重新选中后原映射仍在。两个部署另各对实际绑定查询注入
断连、静默超时、闪断及取消，八组均通过：断连约 0.8～0.9 毫秒返回有类型错误，静默超时
约 1.002 秒，闪断后显式重读约 103 毫秒，取消约 251 毫秒，恢复后绑定不变。
这是 read_timeout=1 秒的查询角色验证，闪断由测试显式重读，不宣称任务自动恢复或统一预算。

代次策略及 base 回收原有真实故障回归同套重跑通过，包括源保留策略切换与零复制的增量删源。
扩充夹具曾漏掉下一代 commit_scan_generation，被生产门槛准确拒绝；补齐正常生命周期后通过。
故障断言最初仅接受 ClickHouseError，实际传输层返回带源错误的 RequestOutcomeUnconfirmed；
断言按现有错误契约接受两种类型，未修改生产错误分类或增加恢复循环。
版本目录 28 轮和普通清理 48 轮亦完整重跑通过；主项目五项 Rust 检查与最后 db/all-targets
Clippy 通过，本机 Rust 1.98.1、已有警告保留。首次 Clippy 拦截测试里的 unwrap，已替换为
显式 Option 条件，未放宽 lint。架构审查：SQL 行及投影一致性留在 db 私有编解码；任务只接收
既有 CopyCompletion，存储协议、任务状态与通知未下沉到行 DTO。历史永久删源仍未接通。

本项正常开销针对实际变更的数据库收尾路径执行十二轮 A/B：同机 Rust 1.98.1、dev debug=0／
incremental=0，基线主项目 `25a1ed8`，两份程序均固定 Native `8bfa329c`。每轮新建 3000 个
1 KiB Local 文件，平铺和 100×30 目录两种布局均删源并落库，同一独占 2 CPU Single ClickHouse，
并发 16、批次 100000、unordered／Direct／quick；每种按 before／after／after／before／before／after
交错三次。计时前结束本项构建和功能故障夹具，数据生成和事后核验不计入 CLI 耗时。

| 场景 | before 耗时中位数／秒 | after 耗时中位数／秒 | before RSS 中位数／MiB | after RSS 中位数／MiB |
| --- | --- | --- | --- | --- |
| 平铺删源、落库 | 5.38 | 5.37 | 203.02 | 203.05 |
| 目录删源、落库 | 5.02 | 5.12 | 218.82 | 220.17 |

耗时分别 −0.19%、+1.99%，RSS 分别 +0.01%、+0.62%；目录场景变更版最大峰值 225.16 MiB，
仍未达到约 200 MiB 优化目标，不能以接近基线替代该限制说明。本项小样本未见明显开销增长，
不能代替 release、大规模历史、多后端或全方案性能验收。
每轮独立核验全部目标内容、源文件删除、base 清空和终态报告入库；目录场景额外核验 100 个
目标目录的纳秒 mtime／mode 与原值一致、源目录消失。未配置 callback，按计划保留旧目标范围的
状态明细仍待外部接收，归档 diagnostics_complete=false，任务 Completed 且错误数为零；
没有将“已保存报告”误作“外部已接收明细”。初次性能夹具错误要求此标志为 true，基线已拒绝
该断言；修正为真实未投递状态后重跑全部十二轮，该试跑不纳入样本。
完整逐轮样本、50 ms RSS 采样、配置、程序与日志保存在 `/tmp/terrasync-copy-binding-perf/`。
基线／变更程序 SHA-256 分别为
`9c9353fe01172d365de6218fa4380f46b6cca23ca4a3171f73c10ebf25fef4ec`、
`fcb480c4a6efe557339efe1b14509374ac09b076cd85c1601e6a0398d6b99238`。
本项独占容器全部清理。

### 版本化源到最新状态目标的永久清理

`--copy-versioned-source-cleanup` 复用普通对象 runner 的独占 S3／ClickHouse 服务和 HTTP 204 callback，
将源桶启用版本化。普通 key 准备三个数据版本和一个已有删除标记；非空尾斜杠对象在当前 RustFS
实际只有可变 null 槽，单独核验，不声称它有四个不可变历史。Local 目标跳过该非空对象，
S3 最新状态目标保留它的原始 key／内容／tags。

应用仅在目标不保留历史、源事实为单个最新可见文件版本时授权整 key 清理；目标数据、
所需元数据、所选校验及 persist_observed 成功后，按该版本捕获的原值重新读取当前源元数据，
再复用已有 Namespace::purge_observed。历史容器属性不能替代所选版本属性，捕获时的任何
元数据失败先行拒绝删除。版本化源当前路径不可见不能证明全部历史已清空，明确返回
SourceHistoryCurrentUnconfirmed 并保留历史、交付 source_deletion_unconfirmed。

data-mover／nfs-rs 和公开存储接口未修改。Native 已有受保护清理每批至多 128 条旧历史，
逐条指定 versionId 删除，当前版本最后处理；每批再次核对当前锚点，不递归删除前缀。
本次只放行最新状态目标；历史目标仍拒绝永久清理，等待每个数据版本／标记的整体就绪核验。
源和目标 key 仍要求既定独占，读回后再删除不提供并发写入下的原子条件保证。

真实 Single／双副本 72 轮已经通过：Local／非版本化 S3、quick／full、只读、断连、静默超时、
删除响应丢失、复制／tags 拒绝、发布后数据损坏及失败数据清理拒绝／响应丢失；另在删源前
注入新源版本、当前删除标记、源 tags 改变与 tags 读取 403。各场景独立核验源版本 ID 集合、
目标内容与元数据、源删除的真实结果、base 回收和最终报告。网络／清理响应未知不能当成未执行，
停止新增删除并准确报告；本夹具丢回执发生在首条旧历史删除，独立确认当前版本仍在。
下一代按前代删源策略走 Full，清理剩余历史。

两种部署另各覆盖“保留源 Full → 删源 Incremental”的 full 校验：未变化条目不增加数据 PUT，
数据传输为零，仍删除全部四条源历史；将已有目标改成等长坏数据后，增量 full 校验明确失败、
源四条记录全部保留、目标坏数据未被冒充本次拥有的失败输出删除。随后 Full 修复并清理源。
第一轮保留源的 base 恰为最新版本一行，删源轮次结束 base 清空，未保存不迁移的旧源历史行。
历史目标的独立保护场景确认三个数据版本及一个标记已复制，但源四条记录仍保留并准确上报。

清理失败沿用当前任务错误口径，数据／元数据复制失败按 entry 计一，具体源版本和原因进入
callback，HTTP 204 后明细清理、累计数保持；本项未统一数据完成／源清理／扫描统计的展示。
目标就绪检查和源复核归 app，指定版本删除、当前锚点及协议错误归 Native；没有在应用增加
S3 删除循环、协议拼接或新的恢复入口。元数据捕获失败的新增单元测试确认源和目标都保留。
主项目五项 Rust 检查及 16 项串行远程回归通过；最终 app 329 项通过／45 项 ignored，
source_cleanup 25 项通过／8 项 ignored，最后 app/all-targets Clippy 通过（Rust 1.98.1，已有警告保留）。
普通未版本化源 48 轮在共享 runner 更新后亦完整复跑通过，未将 ignored 项全部计为通过。

当前历史源路线仍等待整轮消费者／数据库收尾，未放入普通文件的先于事实确认删除窗口；
“全部版本化源逐项异步删源”尚未完成。NFS／CIFS 等其他目标、物理掉电、跨主机排他控制、
统一故障预算及大规模吞吐／RSS 不在上述证据内。

大历史各一轮准备 256 条源记录（171 个数据版本、85 个已有删除标记），跨越 128 条历史分段及
清理批次边界；最终只复制最新数据，callback 文件完成计一、传输字节只取最新载荷。
代理独立计数源 GetObjectTagging=2（扫描与删除前复核），源数据 GET=0、目标数据发布 PUT=1，
没有为 255 条旧历史获取 tags 或复制数据。256 次源删除都显式指定版本 ID，当前版本最后删除；
独立查询源历史为空、目标数据／tags 正确。网络计数不代表服务端内部磁盘 I/O。

这两轮的迁移和报告核验耗时分别 34.313、35.417 秒，包含现有串行历史清理，未单独剥离各阶段。
初次 1201 条种子准备超过夹具预算，改用 256 条验证相同的 128 边界；不声称验证了超过 1000 条
版本列表分页。首次 256 轮的 Single 通过、Cluster 超过原 35 秒测试总期限；仅将大历史测试总期限
改为 90 秒后完整 72 轮通过，普通故障任务仍为原 35 秒测试上限，产品检测／恢复预算未改变。
初次删除顺序日志对尾斜杠使用 strip 错失 key 末尾，改为 lstrip 保留原 key 后复跑；没有放宽
“指定 ID、当前版本最后删除”的断言。串行请求成本和大规模历史吞吐仍需后续优化与验收。

该步骤普通清理路径另与 `e8ad0eb` 做十二轮正常 A/B，确认公共门槛没有明显回归：每轮新建
3000 个 1 KiB Local 文件，平铺／100×30 目录，删源落库，同一独占 2 CPU Single ClickHouse，
并发 16、批次 100000、unordered／Direct／quick；同机 Rust 1.98.1、dev debug=0／incremental=0，
各布局按 before／after／after／before／before／after 交错，计时前结束本项构建及功能夹具。

| 场景 | before 耗时三次／秒 | after 耗时三次／秒 | before RSS 三次／KiB | after RSS 三次／KiB |
| --- | --- | --- | --- | --- |
| 平铺删源、落库 | 5.42 / 5.32 / 5.27 | 5.42 / 5.42 / 5.07 | 167916 / 200400 / 202400 | 191784 / 202168 / 202088 |
| 目录删源、落库 | 5.11 / 5.07 / 5.11 | 5.07 / 5.21 / 5.17 | 203864 / 203208 / 203064 | 204584 / 205380 / 204836 |

耗时中位数 5.32→5.42 秒（+1.88%）、5.11→5.17 秒（+1.17%）；RSS 中位数
195.70→197.35 MiB（+0.84%）、198.45→200.04 MiB（+0.80%），变更版最大约 200.57 MiB。
每轮核验目标内容、源文件删除、base 清空、归档报告；目录另核验原始纳秒 mtime／mode 和源目录删除。
无 callback 时计划保留事件仍待外部接收，diagnostics_complete=false，并非归档或任务失败。
此小样本不说明此前目录约 220 MiB 已修复，仍不代替约 200 MiB 目标、release、其他后端或大规模验收。
旧版拒绝历史源清理，不能用普通清理 A/B 冒充新历史清理路线的成功吞吐对照；上面的绝对大历史耗时
单独保留，后续仍需优化串行历史删除及全部条目的异步删源。
样本、50 ms RSS 采样、日志和固定程序保存在 `/tmp/terrasync-versioned-latest-cleanup-perf/`。
基线／变更程序 SHA-256 分别为
`fcb480c4a6efe557339efe1b14509374ac09b076cd85c1601e6a0398d6b99238`、
`86924b205a1b19b2445394b78d684ab1284e217521caceae96fb58bc13ecd4b7`。
Native 始终为 `8bfa329c`，本项独占容器全部清理。

### 矛盾源版本清单在删除前拒绝（Native e294de47）

固定 Native `e294de476202bfdb94bd493cb229ad5590ee5842`，沿用已有 purge_observed 与
Protocol／Permanent；同批次同一版本 ID 同时标为当前和历史时，不得将当前记录排入旧历史删除。
null 槽按相同规则处理，不新增公开接口、SDK 修改或正常路径请求。原生单元覆盖数据／标记、
真实／null ID 及两种顺序共八个组合；两项新增单元已先验证旧版失败，再验证修复通过。
真实 RustFS 清单注入覆盖数据／标记与两种顺序四组，独立读取原版本及哨兵完整保留、零删除。
原生历史清理共 11 组、普通源清理 7 组通过；backend 416 项通过／2 项 ignored。
原生完整 CI 范围分步通过，首次 Clippy 的夹具长度和随后示例链接空间问题已修正并补跑，
最终严格 Clippy、全部示例及 Local E2E 通过；未将首次失败写成一次性全绿。

主项目 `--copy-versioned-source-cleanup` 现为 Single／双副本共 76 轮：16 正常、32 失败、
28 恢复。两种部署分别在目标已就绪后的源读取阶段启用矛盾清单，独立核验源四条历史及哨兵
保留、零源删除、目标数据／tags 正确；HTTP 204 交付的路径明细包含 current and historical
与 Protocol 原因，任务错误计一，最终报告保存、base 清空。下一代 Full 恢复并清除全部历史。
共享 runner 的普通源 48 轮亦通过；主项目 fmt、workspace/all-targets check／Clippy、
workspace 测试和 16 项串行远程回归通过（Rust 1.98.1，保留既有警告）。
保护只基于当前批次可见证据，不声称全历史清单或并发写入下的原子保证；既定 key 独占要求不变。

正常公共路径另与主项目 `f2d9a20`／Native `8bfa329c` 做十二轮交错 A/B；本次只换 Native
清单保护。每轮 3000×1 KiB Local，平铺／100×30 目录、删源落库、独占 2 CPU Single
ClickHouse、16／100000、unordered／Direct／quick，Rust 1.98.1 dev debug=0／incremental=0，
构建与功能回归结束后计时。平铺耗时中位数 5.42→5.46 秒（+0.74%），峰值 RSS
202.66→202.60 MiB；目录 5.28→5.03 秒（−4.73%），RSS 223.59→223.71 MiB（+0.06%），
变更版目录最大约 226.55 MiB。每轮独立核验全部内容、源文件／目录删除、目录纳秒 mtime／mode、
base 与最终报告；未配置 callback 的计划保留事件待外部接收，diagnostics_complete=false。
小样本未见明显公共路径开销增长，不宣称加速、约 200 MiB 目标或新历史路径大规模性能达标。
原始十二轮、50 ms RSS 采样、日志与程序在 `/tmp/terrasync-purge-conflicting-current-perf/`。
基线／变更 SHA-256 为
`86924b205a1b19b2445394b78d684ab1284e217521caceae96fb58bc13ecd4b7`、
`e2ff05fe74da33b7cb41012a86aeffc39ea268a714b1e9faa3d9bb5b6b21445e`；独占容器已清理。

### Native 版本批量删除的阶段证据（尚未接入任务）

Native `d1e69a5` 已扩展已有 S3Protocol::delete_objects，复用 SourceVersion，支持精确版本／
标记／null 槽，100 项／请求，逐 key＋ID 回执和完整逐项错误，版本批次失败后停止后续提交。
普通 Current 删除保持历史保留语义，丢回执不盲重放；未新增公开入口、类型或 SDK 修改。
Native backend 429 项通过／3 项 ignored，新私有 SDK 契约另外显式执行 19 个真实场景通过，
原有历史清理 11 场景、完整 CI 范围分步及最终严格 Clippy 通过（Rust 1.98.1）。
首次夹具 Clippy 行数问题已经修正并补跑；完整实验入口与范围在 Native 的 tests/lab/README.md。

同一固定测试程序、独占 1 CPU／768 MiB RustFS、dev debug=0／incremental=0，六轮交错清理
128 条历史中的 127 条旧记录（125 条数据版本、一个 null、一个标记），当前版本／哨兵完整保留；串行 delete_version
为 127 次请求，批量为 100＋27 的两次请求。删除阶段耗时中位数 10617.775→230.613 ms，
整个测试进程 10.68→0.30 s，峰值 RSS 28.6875→30.00 MiB。基于同一 SDK 程序的两条调用路径，
不等于两个产品版本或含就绪／源复核的完整 purge，更不等于 CLI／ClickHouse／callback、
release 或整轮大规模迁移性能达标。日志 `/tmp/native-exact-bulk-perf6.log`，样本／固定程序
`/tmp/native-exact-bulk-perf/`，程序 SHA-256
`6e5897f390101828714e7cdf802674481b85593d735c9b04a1b47e1d53a6b4ed`；独占容器已清理。

上述阶段本仓库固定 Native e294de47；当时新批量能力尚未用于源清理。须先让批内所有版本原因保持
有类型传播并进入 callback，失败 entry／任务不能因版本数量重复累计，才切换旧历史清理批次；
当前版本必须独立最后删除。完整历史目标核验、逐项异步删源及整轮 RSS 等未完成项不变。


### 专用永久清理错误与任务内的有界批量历史清理

用户确认后新增 PurgeFailure，仅扩展既有 Namespace::purge_observed 的错误返回；首个原始
StorageRoleFailure 作为标准 source 保留，精确版本可选，其余逐版本原因受单批 100 项限制。
Local／NFS 与默认文件系统包装原来的单项原因，通用 role 错误未增加版本批次分支。
主项目固定 Native `69814a5f112ae6f2eccde284fd6556f6e745a069`：旧数据版本分批删除，
历史删除标记按精确 ID 逐个删除，重列举复核当前锚点，当前项独立最后删除。

真实 256 历史场景曾暴露服务端差异：同 key 的多个历史标记放在一个 DeleteObjects 请求时，
当前 RustFS 确认所有 ID，却每批只实际删除一个。独立 boto3 最小复现请求两个标记，确认两个、
剩下一个；两秒后仍存在，单条精确删除后消失。实际 Namespace 五条历史的回归在修复前返回
六条已删除；修复后正确返回五条，且一批数据、两条旧标记、最后当前项共四次删除请求。
旧 SDK 性能夹具仅有一个标记，不能证明多标记路径。底层同时拒绝前批确认 ID 再次出现在列表，
保留当前项并报告 Protocol，避免重复提交／虚增统计；生产临时状态仍按 100 条有界。

应用复用 Diagnostic／Delete 和现有归档／HTTP 2xx 机制交付版本、阶段和原始原因；
不虚构列举故障的版本，不把未提交的标记写成删除失败。逐版本明细不增加数据失败或任务计数，
整项源清理失败累计一次任务错误，并保留未确认删除的源项和目标。成功接收后清理该批明细，
累计统计不清零；通知错误保持现有归属。三项专门单元验证逐版本原因、无版本整项故障和附加
取消原因，包含 ACK 后统计保留；未新增操作入口或改动 nfs-rs。

Single／双副本真实版本化源夹具现为 88 轮：16 轮正常、38 轮失败、34 轮恢复。新增每种部署
各一组部分拒绝、畸形回执、100 条拒绝与后续全量恢复，核验真实已删除／保留的历史 ID、
最新版本最后删除、目标内容／tags、base 清空及最终报告入库。部分失败只报告两条真实拒绝；
畸形回执报告两条实际提交的数据版本，保留尚未提交的标记及当前项；100 条拒绝的 entry 只计
一次任务错误，至少两批 callback 明细且每批不超过 64 事件。208 条新增逐版本原因通过
HTTP 204 接收；取消／网络／源变化／历史目标拒绝及原有恢复亦通过。大历史两种部署均精确
删除 256 条记录，不增加数据 GET 或目标复制次数：源 tags 两次，目标数据写入一次。
原始日志 `/tmp/terrasync-marker-safe-versioned88-real.log`。

主项目 fmt、workspace/all-targets 检查与 Clippy、workspace 测试（不含 root 包）和
16 项串行远程进程回归通过；同一依赖下普通源清理 48 轮全部复跑通过。工具链为 Rust 1.98.1，
已有警告及 ignored 条目保留，不计为真实后端验收。上述错误契约没有解除历史目标整体就绪
核验的安全门槛，也不宣称逐项异步删源、统一阶段统计或整个优化方案已经完成。

架构审查：批量回执核对和标记差异留在 Native S3 adapter；专用错误属于 namespace role，
通用 StorageRoleFailure 保持 entry／session 轴。应用的逐版本事件集中 consumer/purge_reporting，
计数仍归 ConsumerManager／StatisticConsumer，未向 SDK 引入 callback／ClickHouse 概念。
没有新增重复恢复循环、对外操作入口或按总版本数常驻的任务状态；新增原因受批次约束。

整轮版本化源对照使用固定的变更前／后 CLI，基线为 `6477f5b`／Native e294de47，
各跑三轮交错（before、after、after、before、before、after），dev debug=0／incremental=0，
Rust 1.98.1。独占 2 CPU／1536 MiB Single ClickHouse、1 CPU／768 MiB RustFS；128 条
源历史含 42 个删除标记及 null，目标非版本化 S3，最新内容 1 KiB 含 tags、删源、Direct／quick，
并发 16、批次 100000。种子准备在计时前完成；整个 CLI 进程计时含复制、校验、源清理、数据库
收尾及报告，无其他 CI／构建并发。源清理从 128 次单条请求变为一次批量加 43 次单条；
整轮耗时中位数 12.15→3.65 秒（-69.96%），峰值 RSS 中位数 201.28125→202.7109375 MiB
（+0.71%），变更后最大 203.18359375 MiB。每轮独立验证 128 个唯一删除 ID、当前项最后删除、
源历史清空、目标内容／tags、两端哨兵、base 清空及最终报告；源 tags 两次、数据 GET 零次、
目标数据 PUT 一次。性能场景无 callback 配置，其交付证据来自另行运行的 88 轮故障回归。

六轮样本、50 ms RSS 采样、日志、固定程序与校验和在
`/tmp/terrasync-purge-failure-history-perf/`，程序 `/tmp/run_purge_failure_history_perf.py`，
日志 `/tmp/terrasync-marker-safe-history-perf.log`。基线 SHA-256 为
`e2ff05fe74da33b7cb41012a86aeffc39ea268a714b1e9faa3d9bb5b6b21445e`，变更为
`6a0511a748c6bbfb625af15d9c6f5954dfafe22409ee48b386769af17a712cd6`。
该有限历史规模的收益不代表 release、任意版本数、其他对象设备或全后端约 200 MiB 目标达标。

普通 Local 公共路径另跑十二轮交错对照，使用同一前／后 CLI、Single ClickHouse、3000 个
1 KiB 文件、删源、并发 16／批次 100000，unordered／Direct／quick；平铺与 100×30 目录
各六轮。计时含整轮数据库和目录收尾，50 ms RSS 采样；每轮验证全部目标内容、源文件／
目录删除、目录 ns mtime／mode、base 和最终报告。无 callback 配置，保守范围事件作为未
交付诊断保留，diagnostics_complete=false 不被当成通知已完成。

| 场景 | 耗时中位数（秒） | 峰值 RSS 中位数（MiB） | 变更后最大 RSS（MiB） |
| --- | --- | --- | --- |
| 平铺 3000 文件 | 5.32→5.52（+3.76%） | 198.15→196.41（-0.88%） | 204.57 |
| 100×30 目录 | 4.97→4.97（0%） | 219.70→221.82（+0.96%） | 222.73 |

有限样本中开销与优化前相当，平铺耗时变化小于 5%；目录 RSS 仍高于约 200 MiB 目标。
不宣称普通路径加速或大规模／release 性能达标。十二轮样本、日志和 RSS 采样在
`/tmp/terrasync-purge-failure-local-perf/`，脚本 `/tmp/run_purge_failure_local_perf.py`，
日志 `/tmp/terrasync-marker-safe-local-perf.log`；固定程序与上面的版本化对照相同。
两组性能测试及全部故障夹具的独占容器均已清理。


### 历史目标的数据版本整体就绪与永久清理（不含删除标记）

在专用清理错误接入之后，历史目标现可清理只含数据版本的文件历史：已有
query_current_copy_entries 扩展回读 CopyCatalogueEntry，保留 CopyCompletion 的实际目标绑定，
同时返回原始 history_segment／history_index／history_records／history_complete。
单版本 snapshot 仍保留全部原值；它自己的 complete 不再被当作整 key 完整性的证据。
新增两个边界列由 Single／Cluster 共用记录与列定义写入；这是首个开发格式调整，不提供旧表
兼容或迁移。查询同时要求本执行、路径与数据已确认，不从缺失绑定推断实际目标 ID。

产品逐行检查首段首项、段内连续、段长一致、段间连续及完整末段的最后当前记录。
每个实际目标版本核对所需元数据、类型／大小及持久性，再按精确源版本复核捕获的源属性。
全量校验逐个读取旧源／目标版本；当前目标仍按当前发布绑定核对，避免把旧版本的 pin 用来
替代当前发布状态。一个 key 只保留一份位置状态和一个最新快照，不缓存完整历史／已完成集合。
最后一个 key 等数据库流成功结束后才清理；已知路径的查询失败保留原始类型和路径上下文。
全部所需版本成功后重新确认当前源，再由 Native 受保护永久清理全部历史、当前项最后删除。

历史版本错误包含源／目标版本、阶段和已有脱敏诊断，保持原始有类型原因；整项清理只计一次
任务错误。MetadataVerification::Cancelled 及历史包装中的取消保持取消分类。包含任何删除标记
或目录标记的历史仍明确拒绝清理；不把“数据版本已就绪”扩大为“删除标记也已核验”。
本步骤未修改 data-mover、nfs-rs 或新建存储操作入口，也未接通全部条目先于入库的清理。

六个边界单元覆盖跨段、缺前缀、缺中间项、重放、边界矛盾、缺当前项、查询失败路径／原始类型
和取消。新增数据库单元验证投影仍是一条版本，但原始位置和段长独立保留、非法长度拒绝。
Single／双副本的既有三组真实代次测试通过，新增检查分段 0/1/2、段长 1、末段标记、当前执行
授权以及旧映射与原始元数据；原有断连、静默超时、闪断恢复和取消读取也通过。
原始日志 `/tmp/terrasync-history-data-generations-real.log`。

版本化源真实全套扩为 108 轮：24 正常、50 失败、34 恢复，两种部署均独立核验源／目标历史、
base 和最终报告，HTTP 204 排空明细。新增 quick／full 的三数据版本迁移；旧目标 tags 改变、
源历史 tags 改变、旧目标版本真实缺失、数据库中间行被省略、目标断连、源历史 tags 读取静默
超时均保留三条源历史且零源删除，并交付具体版本／阶段／原因。断连注入到最终回调约
156／244 ms，源静默超时约 1192／1284 ms；这是本地受控代理结果，不是统一故障窗口认证。
同 job ID 上代保留源、本代删源的历史目标增量两轮也通过：先保留三条基线，下一代不重新发布
数据、transferred_bytes=0、按一个 entry 计数，逐版本全量校验后源历史清空、base=0、目标仍
恰好三个原发布版本。删除标记保护和已有 256 历史的最新状态目标场景保持通过。
聚焦入口为在现有 --copy-versioned-source-cleanup 之前设置 LAB_OBJECT_HISTORY_ONLY=true，
仅执行两种部署共 22 轮（8 正常、14 失败）；不把该聚焦结果当作 108 轮全套通过。
原始日志 `/tmp/terrasync-history-data108-real.log` 与 `/tmp/terrasync-history-focused22-real.log`。

普通源 48 轮及主项目五项 Rust 检查（含 16 项远程进程回归）通过，Rust 1.98.1；已有警告和
ignored 条目保留。初次额外真实夹具修正了 SELECT 位于 HTTP body 的故障注入位置及 bootstrap
尚无活动状态的处理；测试大 future 放堆上，未提高产品栈限制。初次 Clippy 的新增测试 unwrap
已改为显式检查并复跑通过。取消、源变化和元数据错误保持失败关闭；统一预算／闪断防抖、
真实历史路径在途取消、历史删除标记、无数据库目录收尾、大规模／release 仍未在本步认证。

架构审查：分段边界与查询投影属于 db，Single／Cluster 共用，不在 app 拼 SQL；完整性状态和
删源决策集中 source_cleanup/history，校验借用现有 metadata／read／integrity／namespace roles，
没有应用内 S3 协议或存储重试循环。逐版本原因复用产品通知与累计统计，未增加无界版本集合。

普通 Local 路径对 0af0d90 的固定 CLI 做交错 dev A/B，3000×1 KiB，删源、Single ClickHouse、
并发 16／批次 100000、unordered／Direct／quick；无 callback 配置。每轮核验目标、源删除、
目录 ns mtime／mode、base 和最终报告，50 ms RSS 采样。首次平铺六轮耗时中位数 +6.21%，
超过 5% 后原样保留样本并交错复测；第二组六轮为 +3.56%。合并两轮各六个程序样本，平铺
5.52→5.675 秒（+2.81%），RSS 185.15→185.65 MiB（+0.27%，变更后最高 196.85 MiB）；
目录三对样本 5.01→5.17 秒（+3.19%），RSS 183.57→183.82 MiB（+0.13%，最高 190.06 MiB）。
不将单轮波动隐去或宣称已定位固定性能回退；这些有限样本不认证所有规模／后端约 200 MiB。
性能期间无 CI／构建并发，独占容器已清理。样本及两组原始结果在
`/tmp/terrasync-history-data-perf/`，程序 `/tmp/run_history_data_local_perf.py` 与复测脚本，
变更前 SHA-256 `6a0511a748c6bbfb625af15d9c6f5954dfafe22409ee48b386769af17a712cd6`，
变更后 `f1d75f8d40a117eaf592d948576472185ffe66547a8f98b093e69174d9a0f6e6`。


### Historical data and deletion-marker readiness (2026-11-09)

The full-migration plan's file-history cleanup now uses data-mover revision
`45e70fa8d79d010643a357154bf30f33e2b93d5d`. Existing selected-version descriptions
can represent a deletion marker as non-data with a partial one-record history.
Existing persistence confirmation checks its precise ID; changing that descriptor's
selector to Current additionally requires the same marker to remain the unique
latest record. The application preserves published bindings and original segment
positions, verifies every data version and marker, and only then requests permanent
source purge. Ordinary transfer still refuses markers before destination mutation.
The backend owns response parsing, marker identity, current-binding pagination and
persistence; the application owns selected-history completeness and copy policy.
No new public data-mover type/function or nfs-rs change was introduced.

Native commits `b8d1c21`, `cd54bda` and `45e70fa` each passed the complete 53-step CI scope
(including feature/architecture checks, strict Clippy, libraries, integration targets,
doctests, examples and Local E2E) with Rust 1.98.1. Evidence is in
`/tmp/native-marker-observation-ci{,-driver}.log` and
`/tmp/native-null-anchor-ci{,-driver}.log`. Actual marker HEADs were probed with boto3
(`/tmp/native-marker-real-head-probe.log`), and fifteen real marker persistence
scenarios passed (`/tmp/native-null-anchor-markers-real3.log`): HEAD permission refusal,
HEAD/LIST connectivity, stall and cancellation; explicit HEAD recovery after disconnect/stall; malformed
marker/version headers; permanent marker removal and current replacement. Independent
checks found no payload GET or mutation by confirmation or rejected transfer.
Four real suspended-bucket null-slot cases and the red/green normalized-null regression
are under `/tmp/native-null-anchor-*`; unchanged data/markers can be purged, while
same-identity replacements with changed time are retained before any DELETE. These
are RustFS storage contracts, not a claim about every vendor or a unified retry budget.

Final architecture review found and fixed an additional pagination proof issue before Main
commit: the current-marker confirmation compared an opaque continuation against a relative
path. `45e70fa` consumes every continuation unchanged until real EOF. Deterministic and
real SDK tests first reproduced erroneous readiness despite a page-two disconnect; the
fixed seventeen-mode suite passes, with six actual RustFS one-record pages and five
MinIO-shaped opaque tokens in the normal case, and typed Connectivity with all records
retained on page two. The read-only fixture's boto3 side obtains real MaxKeys=1 pages;
the proxy decorates/round-trips only the continuation. Evidence:
`/tmp/native-marker-pagination-{red,green}.log`, `-{red,green}-real.log`, and
`/tmp/native-marker-pagination-ci{,-driver}.log` (53/53). Prefix confirmation can include
neighboring names; it does not assume the cursor is a lexical key or a relative path.

Main checks are `/tmp/terrasync-history-marker-pagination-final-{fmt,check,clippy,tests,remote}.log`
and `-ci-driver.log`: all five passed, including 16 serial remote-process cases;
existing large-error and other project warnings remain. The meaningful source-marker
unit covers immutable marker evidence, mixed timestamp precision and changed/missing
null evidence without calling ordinary current-path or content reads.

Run the owned real fixture with the usual external Cargo target/profile environment:

```sh
python3 tests/lab/run-notification-archive-local.py --copy-versioned-source-cleanup
python3 tests/lab/run-notification-archive-local.py --copy-object-cleanup
LAB_OBJECT_HISTORY_ONLY=true python3 tests/lab/run-notification-archive-local.py --copy-versioned-source-cleanup
```

The final versioned run passed 126 generations (36 normal, 56 faulted, 34 recovery)
across Single and replicated Cluster, including quick/full with the latest record a
marker; independently checked all source records were permanently gone, current last.
New faults remove a real historical target marker, replace the target current binding,
stall marker HEAD, or remove an old source marker after publication: source histories
are retained, completed target data preserved, and HTTP 204 diagnostics include exact
source/target marker IDs and cause. For these readiness faults each path has one task error, no additional
copy-data error, and scans one logical entry. An additional 40-generation focused run
passed (20 normal, 20 failures). Retain-full then delete-incremental now also covers
three data versions plus two markers: base 5→0, zero transfer bytes, original target
version IDs unchanged, and all source history removed after full verification.
The ordinary fixture passed 48 generations (12 normal, 18 faulted, 18 recovery).
Logs: `/tmp/terrasync-history-marker-{full126,ordinary48}-after-pagination-real.log` and
`/tmp/terrasync-history-marker-focused40-real.log`. Reports remain queryable, diagnostics
are drained by HTTP 204, and deleting generations leave base empty. The earlier
122-generation run remains under `-full122-real.log`; it predates the added mixed-marker
incremental case and is not counted as the expanded final suite.

Normal-path performance uses physical copies of the complete CLI, not feature-mixed
Cargo executables. Before: main `54cb831` production code / Native `69814a5`, SHA-256
`f1d75f8d40a117eaf592d948576472185ffe66547a8f98b093e69174d9a0f6e6`.
Final after: this feature's production code / Native `45e70fa`, SHA-256
`260c0115eabead3e05ad3aa958098744ed0eac5f9996a27ca375eab225eb934b`.
Fixed binaries and metadata are in `/tmp/terrasync-history-marker-perf/`;
the final pair is recorded in `final-binaries.json`.
The final 24 measurements use 3000 × 1 KiB Local files, flat and 100 × 30 tree,
DeleteSource, Single ClickHouse limited to 2 CPU/1536 MiB, 16/100000,
unordered/Direct/quick, dev, no callback, and no concurrent task build/CI/functional
suite. The predefined order is ABBAABBAABBA per scenario, six samples per version.
Each run independently checks target bytes, source file/directory removal,
nanosecond directory mtime/mode, empty base and saved final report. No callback means
the planned target-retention status remains undelivered (`diagnostics_complete=false`);
this is not callback success.

| Scenario | Median seconds before → final after | Time change | Median peak RSS MiB before → after | RSS change |
| --- | --- | --- | --- | --- |
| Flat, six pairs | 5.265 → 5.375 | +2.09% | 201.28 → 197.82 | -1.72% |
| Tree, six pairs | 5.170 → 5.170 | 0.00% | 204.37 → 203.76 | -0.30% |

Final raw samples, 50 ms RSS, stdout, stderr, time files and summary are in
`/tmp/terrasync-history-marker-perf/final-local/`; the driver is
`/tmp/run_history_marker_final_local_perf.py`, log
`/tmp/terrasync-history-marker-final-local-perf.log`. The fixed threshold checker
passes both scenarios. Highest final-after RSS was 210.46 MiB. Directory median
remains above roughly 200 MiB; release/large/other-backend scope and uninterrupted
memory growth remain uncertified.

Earlier diagnosis is preserved separately. The pre-pagination candidate / Native
`cd54bda` SHA-256 was `fc95679cb5dd49964b4b3d0fff12a1e080f9ca172fa0156c6c6f024a28c26e5b`.
Its initial twelve ABBAAB measurements breached flat time (+6.90%). Before changing
code, a fixed additional twelve BAABBA measurements captured CPU user/system time.
No code/configuration changed during that experiment; all 24 candidate samples,
including the breach, are in `combined-{results,summary}.json`, with raw records in
`local/` and `local-reversal/`. Candidate combined flat/tree time changed -3.13%/-1.01%,
and reversal median CPU -1.06%/-1.74%; a stable fixed-cost regression was not supported.
These are not speedup claims. Those earlier candidate samples are not mixed into the
final 45e70fa program's 24 measurements above.

The first focused run had a fixture-only mistake: invisible current keys were treated
as empty history even though five original records were correctly retained. The
inspector now checks visible objects and actual version sets separately; the failed
log remains `-focused36-real.log`. An initial Main link failed from ENOSPC. Only unused
old-revision .rlib/.rmeta artifacts in this task's external cache were released; current
builds, source, all fixed benchmark binaries and samples were preserved. The subsequent
complete checks passed. No production change was made to hide either test limitation.

This step covers file histories, including deletion markers. Directory-object marker
cleanup remains guarded. All-entry asynchronous source cleanup, database-independent
directory finalization, uniform stage counters and a shared cross-stage network-jitter
budget/execution fence are separate pending work; the read-only cancellation and
explicit recovery tests above do not certify those goals. All owned containers/volumes
were cleaned after each run. Final review keeps S3 decoding in its adapter, selection
and completeness in the task layer, and bounded row-by-row proof rather than a retained
full-history/completed-key set; the library continuation tracker still has its own bound.


### 单进程接收队列取消（2026-11-09）

结构提交 `02bfa9a` 将已有接收循环、结果广播、错误和活动计数移入
`receiver/in_process`，没有行为或测试调整。随后单独修复接收队列取消：
共享执行已取消时不再取出排队项，复用 transport.close 关闭生产端；
空闲等待可退出，已有真实失败仍按原类型返回。未开始项不产生虚构的逐项失败；
已经在途的复制仍由原生取消和清理收束。

结构、行为两步分别通过五项 Rust CI，证据为
`/tmp/terrasync-copy-receiver-{extract,cancel}-ci-results.json` 和对应检查日志；
已有大错误类型等 warning 保留，没有新增存储接口。新增三个确定性回归验证
128 项预取消队列、空闲等待及此前真实失败的保留，原测试未改。

真实故障命令为 `python3 tests/lab/run-notification-archive-local.py --copy-data-cancel`
（使用上述外置 Cargo target 和 dev/test profile 环境）。证据
`/tmp/terrasync-copy-receiver-cancel-active-data-real.log`：Single／双副本、全量／增量、
Direct／Checkpointed 的 16 个持续断连／静默超时故障及 8 次检查点复用均通过；
独立核验源完整、Direct 坏目标移除、恢复字节少于整文件、32 个 HTTP 204 终态汇总
诊断完整，数据库原始原因仍在 callback 且最终报告可查询。注入到收尾约 3.3～6.3 秒，
含检测、收束和通知，不证明统一三秒或所有后端的防抖预算。独占 ClickHouse 容器已清理。
架构审查确认队列取消归 receiver，执行最初原因和跨 worker 取消继续复用
CopyDatabaseExecution；未复制协议重试、未直接丢弃在途传输，也未改变源清理证据。
本小步骤未单独认证正常吞吐或约 200 MiB，完整程序性能继续单独验收。


### 普通对象逐项异步删源（2026-11-09）

全量、启用数据库、普通 File 且无 history 的对象到对象路线，复用现有 namespace 的
`allows_paged_delete` 与目录对象能力选择。在复制数据、所需元数据及所选校验成功、
New 事实进入有界消费者队列后，复用源快照绑定的已校验目标描述，做当前目标持久性、源元数据和受保护
purge，不等待逐项事实入库。物理目录、历史、打包及 file-list 路线沿用原屏障。
未修改 data-mover 或 nfs-rs，Native 仍固定 `45e70fa`，无新增公开存储入口或类型。

ReadyFileCleanup 保持原清理的一项在途上限，等待者最多随复制并发存在，每个 worker
至多一个清理 future；不增加完成路径集合、待删除队列或磁盘暂存。失败先记录最初原因并
取消共享执行，再释放 mutex；另外只保留唯一在途清理的实际错误，数据库先失败也能报告
该路径的保留／未知结果。完整成功后以执行内覆盖证明跳过普通文件的后续数据库核验，
历史及目录照常收尾，不把证明用于失败轮或后续代次。

两个确定性门闩及一个实际 Local 源快照绑定回归通过，原有清理测试不改变行为，日志为
`/tmp/terrasync-streamed-object-gate-pair-tests.log`。
五项 Rust CI 全部通过（包括 16 个串行远程进程场景）：
`/tmp/terrasync-published-target-proof-reviewed-ci-results.json` 及同名前缀各检查日志。
最终全 workspace/all-targets Clippy 为
`/tmp/terrasync-published-target-proof-reviewed-clippy.log`；既有大错误类型等 warning
保留。工具链 Rust 1.98.1，未在 CI 配置的 1.95 工具链上另跑。

真实专项命令（沿用外置 Cargo target/profile 环境）：

```sh
LAB_OBJECT_STREAM_ONLY=true python3 tests/lab/run-notification-archive-local.py --copy-object-cleanup
LAB_OBJECT_COMBINED_ONLY=true python3 tests/lab/run-notification-archive-local.py --copy-object-cleanup
```

前者在 Single／双副本、quick／full、最新状态／历史目标共 8 轮，每轮 98 个文件。
独立阻塞最后一个数据 PUT 和 base 事实转发，先确认 96 个源对象已删除、相应目标内容正确，
最后一个源仍在；最后核验恰好 98 次删除、没有删后源读取、恰好 98 次目标 tags 校验、98 个扫描文件、零错误、完整
HTTP 204 诊断／可查询报告及 base=0。专项持有请求使用 30 秒读取／操作时限，生产默认及
普通故障场景仍保持各自原时限。日志：
`/tmp/terrasync-published-target-proof-final-eight-real.log`。

后者在真实 DELETE 在途时拒绝数据库事实，两个失败轮及两个恢复轮均通过；失败源及
已复制目标保留，callback 同时有数据库原因和 source_deletion_unconfirmed 的具体路径，
一项任务错误、零复制数据错误，恢复后 base=0，最终报告可查。日志：
`/tmp/terrasync-published-target-proof-final-combined-real.log`。
最终组合另重跑普通对象 48 轮、版本化源 126 轮、持续数据库故障 16 场景及 8 次检查点
复用，全部通过，日志为 `/tmp/terrasync-published-target-proof-final-{ordinary48,versioned126,active-data}-real.log`，
汇总 `/tmp/terrasync-published-target-proof-final-real-results.json`。

失败样本 `/tmp/terrasync-streamed-object-eight-real{,2,3,4,5}.log` 保留：先修正夹具初始化
尚无 active state 的处理；随后出现默认 dev 栈溢出，测试入口单独 Box 和缩短 match
临时值都未解决。GDB `/tmp/terrasync-streamed-object-gdb-real.log` 定位实际 Receiver→S3
SDK HEAD 调用链；最终仅把符合条件的清理 future 放堆上（每 worker 一项）后，默认线程栈
通过，没有使用 RUST_MIN_STACK。第五轮已证明前 96 个源提前删除，但故意持有尾部请求超过
原一秒检测时限；专项夹具独立调整时限后完整通过，原断连／静默超时故障仍用原配置。

最终生产代码的 48 轮固定程序 A/B：每组 ABBAABBAABBA、6 前／6 后，dev、16／100000、
unordered／Direct／quick、删源、隔离 Single ClickHouse；S3 额外隔离 RustFS、256 个文件含
嵌套前缀。每轮独立核验数据、tags／目录 ns mtime 与 mode、源删除、base、最终报告，
50 ms RSS 采样，无本轮构建／CI／功能测试并行。全部通过 5% 耗时阈值。

| 场景 | 耗时中位数（秒） | 变化 | 峰值 RSS 中位数（MiB） | 变化 |
| --- | --- | --- | --- | --- |
| Local 平铺 3000×1 KiB | 5.060→5.090 | +0.59% | 202.59→203.06 | +0.23% |
| Local 目录 100×30×1 KiB | 5.055→4.965 | -1.78% | 218.32→220.81 | +1.14% |
| S3→最新状态目标 256×1 KiB | 8.595→6.580 | -23.44% | 218.92→221.85 | +1.34% |
| S3→历史目标 256×1 KiB | 8.425→6.670 | -20.83% | 219.05→224.48 | +2.48% |

最大峰值 228.60 MiB，约 200 MiB 未全面达成；不以这些有限样本认证大规模／release／
全后端或持续增长。无 callback 配置，diagnostics_complete=false 为计划内保留事件，
不冒充外部接收成功。固定前程序为主项目 2d5cdc7／Native45，SHA-256
`260c0115eabead3e05ad3aa958098744ed0eac5f9996a27ca375eab225eb934b`；
修复后程序为本步骤生产实现，SHA-256
`a11d52c4005fea9b7e45d800e59b31a73ada1e1ea1bae838ea5f52f4adf86ee4`。
固定程序及元数据 `/tmp/terrasync-streamed-object-perf/published-proof-binaries.json`，
最终原始样本／RSS／汇总在 `published-proof-{local,s3}/`，脚本为
`/tmp/run_streamed_object_published_proof_{local,s3}_perf.py`，日志为
`/tmp/terrasync-streamed-object-published-proof-{local,s3}-perf.log`。

原候选 SHA75afd0 的 Local24 通过，但 S3 历史目标先回退6.23%；反向12轮后合并
12前／12后仍回退约7.30%，未按达标提交。固定同一清理连接的8轮试验未改善（+4.64%），
代码撤回；并发一的8轮对照基本持平（-0.12%），说明应继续检查并发等待。
真实 red 回归 `/tmp/terrasync-streamed-object-target-proof-red-real.log` 在6.41秒明确断言
98个文件发生196次目标tags读取：复制刚校验过，清理又在门闩内重复读取。
随后扩展应用内既有复制结果为私有字段、不可克隆、绑定源 Arc 的 CopiedPublication，
成功的数据／元数据校验才能构造；不同源快照（即使同路径／同身份）不能消费。
即时清理以其当前目标身份持久性复核，源属性复核和受保护purge仍执行；其他清理传None
保持原路径。CommitRecorder.finish保留原收尾语义并泛化返回值，续传也能交付该事实。
没有新增Native接口或共享输出缓存。实际Local绑定用例及真实green8轮通过，目标tags
恰好98次，默认栈验证继续通过。

原始失败候选样本 `s3/`、反向 `s3-reverse/`、未采用 `context-probe/`、并发一
`s3-concurrency-one/` 和对应脚本／日志均保留，未混入最终程序统计。
诊断日志 `/tmp/terrasync-streamed-object-{s3,s3-reverse,context-probe,s3-concurrency-one}-perf.log`。
这些实测及一读预算回归定位了重复目标核验和串行等待；不把未改善的连接池假设写成原因。


架构审查：receiver 所有复制结果与调用时机，source_cleanup 所有能力判定、清理门闩、
执行内覆盖和实际原因，协议安全由 Native 所有，数据库与最终通知沿用现有模块。
没有新增逐项数据库回执、协议恢复循环或全量常驻集合。剩余范围包括文件系统／历史
全部条目的异步清理、无数据库目录收尾、统一阶段统计、统一防抖／预算和 release／
大规模／全后端约 200 MiB 验收；本步骤的受控结果不替代这些目标。


### Directory metadata failures and complete callback pages (2026-11-09)

`python3 tests/lab/run-notification-archive-local.py --directory-completion` runs
10 migration generations against isolated Single/two-replica ClickHouse: two
retain-source bootstraps, two full failures, two incremental failures, and four
recovery full migrations. Each failure uses an owned uid-65534 child, 70 existing
root-owned writable target directories and one child-owned target directory.
File writes succeed while final owner/mode/timestamp writes fail on the 70
root-owned directories. The child checks every source/target body remains and
the writable directory still receives its original mtime. This exercises real
permission failures; it is not a network-outage or NFS claim.

The independent HTTP receiver returns 204 and verifies 280 unique `Copy` errors
across bounded pages, every directory path, `backend=Local`,
`phase=directory_metadata_finalize`, no omitted events, and cumulative error
counts of 70 with zero additional task errors after delivery. Planned retention
status shares the same pages and is checked separately without incrementing
error counts. Each final report is independently queried from ClickHouse.
Recovery uses the same job, requires Full after a deleting generation, verifies
all target bodies/mtime, source directory removal and empty base. All owned
containers, volumes, databases and local prefixes are removed on exit.

Three new deterministic tests cover Full/Incremental entry counting, multiple
failed directories plus a later successful directory, a panicked worker's path
and typed cause, and simultaneous query/update failure diagnostics. Existing
query-type, drain and cancellation assertions remain unchanged. The production
change uses existing `Error(Copy)` and the diagnostic journal; at most 32 task IDs
retain their directory snapshots, rather than a whole-task error collection.
`ReportedCopyFailure` preserves the original typed cause and prevents the final
coordinator counting the first directory failure again. A separate query failure
is still a task error. Statistics workers share `TaskStatistics` without owning
other consumers or lifecycle guards. No public data-mover API was added.

Five Rust CI checks passed using Rust 1.98.1, including workspace tests (app
348 passed/47 ignored) and all 16 serial remote-process scenarios. Existing
large-error warnings remain. Logs are `/tmp/terrasync-directory-errors-{fmt,check,clippy,tests,remote}.log`
and results `/tmp/terrasync-directory-errors-ci-results.json`. Real permission
and callback results are `/tmp/terrasync-directory-errors-reviewed-real.log` (including
the final writable-directory mtime assertion); the original
12 ready-window generations also pass in
`/tmp/terrasync-directory-errors-ready-window-real.log`.
The first independent checker mistakenly treated planned retention events as
Copy errors; its failed run is preserved at
`/tmp/terrasync-directory-errors-retention-fixture-first-real.log`. Production
code was not changed to accommodate that checker. The red regression before
error reporting is `/tmp/terrasync-directory-errors-red.log`.

The fixed full-CLI before SHA-256 is
`a11d52c4005fea9b7e45d800e59b31a73ada1e1ea1bae838ea5f52f4adf86ee4`
(main 83b4f92 production code); after is
`44749452931678ac92f09bcde0e49b666b4921ebfcba958e0c59107a8f682a7c`.
Both use Native 45e70fa, dev/debug0 and Rust 1.98.1. The 24 normal-path
measurements use 3000 × 1 KiB Local files, flat and 100 × 30 tree, each in
ABBAABBAABBA order, 16/100000, unordered/Direct/quick/DeleteSource,
Single ClickHouse limited to 2 CPU/1536 MiB. No task build, CI or functional suite
runs during measurement. Each run checks data, source cleanup, nanosecond directory
mtime/mode, empty base and saved final report, and samples RSS every 50 ms.

| Scenario | Median seconds before → after | Time change | Median peak RSS MiB before → after | RSS change |
| --- | --- | --- | --- | --- |
| Flat | 5.235 → 5.320 | +1.62% | 161.79 → 160.53 | −0.78% |
| Tree | 5.120 → 5.110 | −0.20% | 207.89 → 210.27 | +1.15% |

The fixed 5% time checker passes both. Maximum after RSS is 215.23 MiB.
Fixed binaries, hashes and all raw time/RSS/stdout/stderr records are under
`/tmp/terrasync-directory-errors-perf/`, driver
`/tmp/run_directory_errors_local_perf.py`, log
`/tmp/terrasync-directory-errors-local-perf.log`. No callback is configured for
performance samples, so their planned retention events are undelivered
(`diagnostics_complete=false`), not external delivery success. This does not
certify a 200 MiB ceiling, release/large workloads, sustained memory growth,
other backends or the shared cross-stage network-jitter budget. Database-independent
directory finalization and all-entry asynchronous cleanup remain pending.


### Reported entry failure must drain already published facts (2026-11-09)

Main commit `d241814` distinguishes an already reported entry outcome from a
fatal execution/database failure. Both still fail the task. The former leaves
the fact scope open to drain; the latter saves the original typed cause and
cancels peers. A later fatal peer cannot be hidden by an earlier reported entry
result at the root. Storage/copy layers keep their original responsibilities;
there is no additional IO, buffer, per-file receipt or source-deletion gate.

The existing 56-generation metadata regression failed on both main 229e8dd in
an isolated clean worktree and the metadata-cancellation candidate: source tags
were denied, but the expected one DataOnly fact disappeared before flush.
A deterministic red test confirmed `run_child` cancelled the fact scope for
`ReportedCopyFailure`. The isolated fix and its two regression tests passed
five Rust CI checks, app 350 passed/47 ignored and 16 remote-process scenarios.
The temporary worktree was removed after cherry-picking the independent fix.
Its build-generated npm lockfile change was restored and not committed.

The real 56-generation suite then passes Single/two-replica, latest/history,
quick/full, repeated source-tag acquisition and destination-tag application
failures, recovery and old-version-only metadata changes. It retains all raw
facts and actual target-version bindings, with zero body replay in the six
incremental rounds per case. Independent HTTP 204 inspection checks 32 failed
logical entries, 80 version causes, 40 source observation errors and 40
conservative retained ranges, no extra task errors, and queryable final reports.
A separate final run covers 16 real database disconnect/stall failures and eight
checkpoint recoveries, checking retained sources, Direct cleanup, checkpoint
reuse and complete callbacks. Fault-to-stop time includes detection and final
notification and is not a universal three-second recovery claim.

Evidence: `/tmp/terrasync-metadata-cancel-{baseline-version56,version56}-real.log`
are the two original failing runs;
`/tmp/terrasync-reported-entry-fact-drain-red-runtime.log` is the focused red;
`/tmp/terrasync-reported-entry-fact-drain-{focused,version56-real,final-active-data-real}.log`
are successful focused, metadata and final fatal-database checks.
`/tmp/terrasync-reported-entry-fact-drain-ci-results.json` records final five-check
results; the driver is `-final-ci-driver.log`. Existing typed large-error warnings
remain. This fixes deterministic lost facts after reported copy errors, without
removing the existing allowance for genuinely unavailable asynchronous facts.

### Metadata-only cancellation and matched normal-path measurements (2026-11-09)

`python3 tests/lab/run-notification-archive-local.py --metadata-cancel` runs six
real S3 cases, using owned held tags PUT/GET requests for ordinary metadata-only
updates, selected current updates and explicitly bound historical updates.
The existing app metadata helpers now accept the caller's optional cancellation
and share it between application and readback. Ordinary incremental workers
forward their existing token instead of recreating default options, and the
selected-version workflow forwards `CopyOptions.cancel`. No data-mover public
function/type was added. Other helper callsites explicitly retain their previous
independent execution semantics; directory finalization, rename and remote
session cancellation remain pending work.

Two new deterministic tests cover pre-cancelled updates and in-flight application/
verification in both workflows, with typed cancellation, no success publication,
and both bodies retained. Three additional actual-worker regressions cover an
open idle producer, 128 queued metadata entries and a prior real metadata error.
The incremental worker checks cancellation before dequeue, closes its input and
settles started history; it does not drop in-flight storage work or count
unstarted entries as failures. These cases exit within 0.5 seconds and preserve
the earlier typed cause. Their red and green logs are
`/tmp/terrasync-incremental-metadata-queue-cancel-{red,focused}.log`.
The real S3 function-level cancellation cases return
within one second of the caller signal. The independent target proxy observes
zero data GET, data PUT or DELETE, exactly one held request per case and exact
historical version selectors. Recovery applies only tags; source/target bodies,
record count and current head remain correct. This is not a full CLI user-cancel,
NFS/other-protocol or shared network-jitter-budget certification.

After integrating the independent fact-drain fix, five Rust CI checks pass again:
app 355 passed/48 ignored, all 16 serial remote-process scenarios, Rust 1.98.1.
The 56-generation metadata suite and six real S3 cancellations pass on the
combined source. Final logs are `/tmp/terrasync-metadata-cancel-final-{fmt,check,clippy,tests,remote}.log`
and `-final-{six,version56,active-data}-real.log`. The final sequential CI/runtime/
build driver and results are `/tmp/terrasync-metadata-cancel-final-validation-{driver.log,results.json}`;
five-check results are `/tmp/terrasync-metadata-cancel-final-ci-results.json`.
This final run also repeats all 16 real database failures/eight checkpoint recoveries.
The red is `/tmp/terrasync-metadata-cancel-red.log`. The first new S3 fixture
wrongly rejected the normal bucket-versioning connection probe; only its proxy
was fixed. That failed run is preserved at
`/tmp/terrasync-metadata-cancel-bucket-probe-fixture-first-real.log`.

Fixed full-CLI before SHA-256 (229e8dd):
`44749452931678ac92f09bcde0e49b666b4921ebfcba958e0c59107a8f682a7c`.
After (d241814 plus metadata cancellation and incremental queue guard):
`3e13852acce572092bd4e272807cae63f93f41beaf6bd243bc30c5e236a92418`.
Both use Native 45e70fa, Rust 1.98.1 dev/debug0, 16/100000 and
unordered/Direct/quick. Single ClickHouse has 2 CPU/1536 MiB and a 512-thread
global pool, identical for both binaries. Per scenario ABBAABBAABBA gives six
samples per version, 36 measured runs total. Local full DeleteSource uses
3000 × 1 KiB in a 100 × 30 tree. Incremental RetainSource uses 3000 flat files,
unchanged or 30 mode changes, with zero measured data transfer. Its 24 bootstrap
full runs are separately saved and excluded from incremental timing. No task
build, CI or functional suite runs during measurement. Every run checks source/
target data, directory nanosecond mtime/mode where applicable, exact base count,
actual generation and final report, with 50 ms RSS samples.

| Scenario | Median seconds before → after | Time change | Median peak RSS MiB before → after | RSS change |
| --- | --- | --- | --- | --- |
| Full tree/delete | 5.245 → 5.200 | −0.86% | 196.01 → 194.89 | −0.57% |
| Incremental unchanged | 1.930 → 1.880 | −2.59% | 176.58 → 172.81 | −2.13% |
| Incremental 1% metadata | 2.110 → 2.060 | −2.37% | 180.78 → 182.25 | +0.81% |

The respective full 5%/incremental 10% time limits pass. Highest after RSS is
198.06 MiB. These samples establish comparable normal-path costs for this scope,
not a general speedup, 200 MiB cap, release/large/other-backend or uninterrupted
growth certification. No callback is configured in performance runs: full
Preserve status is undelivered (`diagnostics_complete=false`); unchanged/metadata
incremental has no pending events. Neither is external callback receipt proof.

Raw records, bootstrap logs, RSS, time/stdout/stderr, fixed binaries, hashes and
owned server logs/state are at `/tmp/terrasync-metadata-cancel-perf/`;
`final-binaries.json` records the final pair. The earlier pre-queue-guard candidate
and its separate 36 samples remain in `local-final-pool512/` and are not mixed;
final rows/summary are `local-final-queue-guard/{results,summary}.json`, driver
`/tmp/run_metadata_cancel_queue_guard_local_perf.py`, log
`/tmp/terrasync-metadata-cancel-queue-guard-local-perf.log`. Containers/volumes are removed.
Initial lab qualification is preserved separately: 12 full samples under pool192
preceded a first baseline incremental health-probe timeout; a fresh identical
container also failed with no OOM. Only changing the global pool 192→512 made
that same before-binary incremental pass in 1.86s with zero changes/data. The
observed container process count increased 227→274, supporting pool starvation.
Final measurements use pool512 for both; they do not mix the earlier pool192
samples. `first-2cpu-default-threads/`, `fresh-incremental-probe/` and
`pool512-incremental-probe/` retain all failures, logs and state. Product fault
budgets and migration concurrency were not changed to make the benchmark pass.

Final architecture review keeps failure/cancellation policy in the existing
execution scope, metadata actions in existing copy helpers and protocol safety
in Native roles. Caller tokens are cloned with bounded in-flight work; no whole-task
error collection or new durable side channel is introduced. Database-independent
directory finalization, all-entry asynchronous cleanup, uniform stage counters,
and cross-stage fault budgets/jitter handling remain pending in the full plan.

## Directory completion cancellation and bound-root isolation

`python3 tests/lab/run-directory-cancel-nfs.py` uses the existing VM 104 NFSv4.1
export through PVE (`LAB_PVE_SSH_KEY`, default the existing laboratory key), fresh
localhost-only ClickHouse ports and an owned `nightly-directory-cancel-*` prefix.
It changes no export, service, firewall or unrelated data. Set
`LAB_DATA_MOVER_ROOT` when the companion repository is not the default sibling;
its existing bounded RPC decoder is reused. The runner removes its services,
containers and exact owned prefix on exit.

A real directory-only catalogue SELECT arms the private NFS relay after all data
copy workers finish. The relay suppresses three directory SETATTR requests and
then disconnects only this job's ClickHouse data connections; notification storage
and HTTP 204 callback remain available. The fault is one-shot. It is released only
on an observed cancellation request or this job's independent terminal generation
request after leaving the execution scope. An idle execution can join every
request before its optional KILL QUERY manager runs, so the test does not require
that particular management RPC. No fixed delay hides an active fault.

The final exact-revision run passed ten generations across Single and a
one-shard/two-replica deployment: two retain-source full baselines, two failed full
moves, two failed incremental moves, and four full recovery moves. File timestamps
are normal filesystem timestamps, each root/entry has different bytes, and
independent guest reads use O_NOATIME. Every failure retained all source payloads
and target data. The four callbacks delivered all twelve directory paths with
backend, finalization phase and cancelled/unconfirmed write causes; counts remained
three entry errors and one task error after HTTP 204, with no omitted diagnostics.
The failed incrementals transferred zero file data. Recovery restored exact
nanosecond directory mtime and mode, purged source contents and retired all base
facts. Both failed and successful final reports were independently queried from
ClickHouse. Fault injection to the first independent terminal-generation request
was 3.59–3.99 seconds; whole failed tasks including finalization/reporting took
6.40–8.63 seconds. Neither number proves the detection or shared recovery budget
for all backends, a whole CLI user-cancel path, or cross-generation late-write fencing.

Six new deterministic cases cover pre-cancel before query/commit, 128 queued
entries, idle query ownership, 32 started writes without admitting entry 33, an
already-completed query failure, and the saved execution root cause. The directory
module passed thirteen tests (two existing environment cases ignored). The final
five Rust CI checks passed: app 361 passed/49 ignored and sixteen remote-process
cases passed. The earlier 70-directory permission regression also passed ten
real generations, delivering 280 errors with no omissions, preserving source/data,
completing a writable sibling, and recovering with correct directory metadata.

The controlled tests exposed a separate NFS bound-prefix cache defect. Independent
Native v3/v4.1 cases first proved that changing B/item changed A/item instead;
57e5d00 updates the existing cache-root identifier when binding the actual root
FH. e426d09 adds in-flight cancellation to the existing Metadata::apply token.
Neither introduces a public function/type or SDK change. Both Native changes
completed the entire CI scope in documented sequential steps; the root fix also
reran all sixteen metadata fault cases. With 57e5d00, the complete application
matrix passed under ordinary atime; no production timestamp check or persistence
check was relaxed. Prior failed atime and fixed-time experiments remain archived
as diagnostic evidence and are not successful acceptance samples.

The common isolated ClickHouse profile now uses a global thread pool of 512;
its two-CPU/1536-MiB limits and product timeouts are unchanged. The old 192 profile
failed in the replicated permission test with probe/diagnostic timeouts; the
unchanged suite passed at 512. The earlier fixed-program incremental benchmark
had independently demonstrated the same pool-capacity issue. The private NFS
runner and both benchmark binaries use this same profile. Guest Agent briefly
became unavailable during one preparation attempt; read-only probes recovered
and its exact potentially-created prefix was cleaned. No VM restart or service
change was performed.

Final evidence on the development host:

- `/tmp/terrasync-directory-cancel-final-ci-results.json` and the five CI logs.
- `/tmp/terrasync-directory-cancel-final-nfs10-real.log`, including exact callback
  counts, zero incremental replay and independent source/target checks.
- `/tmp/terrasync-directory-cancel-final-permission10-pool512-real.log`;
  the failed 192-profile log is preserved separately as
  `/tmp/terrasync-directory-cancel-final-permission10-real.log`.
- `/tmp/native-nfs-metadata-cancel-final-real-driver.log`,
  `/tmp/native-nfs-prefix-cache-{red,green}-real.log`,
  `/tmp/native-nfs-prefix-cache-metadata16-real.log` and both Native CI scopes.
- The first fixture's KILL assumption, the repeated rearming, ordinary-atime
  failures and Guest Agent preparation failure remain in the
  `/tmp/terrasync-directory-cancel-*-real.log` diagnostic series.

The fixed before/after CLI benchmark used Rust 1.98.1 dev/debug=0,
16 workers/100000 batch, unordered/Direct/quick and the shared Single ClickHouse
profile. ABBAABBAABBA per scenario produced 36 measured runs, with 24 separate
unmeasured incremental full baselines. No task-owned build, CI or functional test
ran alongside measurement. Every body, source retention/deletion, mode, directory
nanosecond mtime, generation/base state and saved final report was checked.
Incremental data transfer was zero. Fifty-millisecond RSS samples and GNU time
peaks are retained with each result.

| Scenario | Median seconds before → after | Change | Median peak RSS MiB before → after | RSS change |
| --- | --- | --- | --- | --- |
| Full move, 3000 × 1 KiB, 100 directories | 5.555 → 5.605 | +0.90% | 219.68 → 222.35 | +1.21% |
| Retained-source unchanged incremental, 3000 files | 1.855 → 1.865 | +0.54% | 233.93 → 239.38 | +2.33% |
| Retained-source incremental, 30 mode changes | 2.190 → 2.060 | −5.94% | 241.85 → 233.96 | −3.26% |

Time passed the respective 5%/10% gates, with comparable memory overhead.
After max RSS was 243.72 MiB: this sample does not satisfy a universal 200-MiB
bound, certify large/release/all-backend behavior, or measure sustained growth.
No callback was configured in performance runs; full planned-retention diagnostics
were pending and cannot be counted as externally delivered. Before SHA-256 is
`3e13852acce572092bd4e272807cae63f93f41beaf6bd243bc30c5e236a92418`;
after is `90d7a5645609070ec8396304c156c0203c0752eff936d46bc20da7ce322d76b4`.
Exact binaries, hashes, all raw/summary results, bootstrap runs, sampled RSS and
server diagnostics are under `/tmp/terrasync-directory-cancel-perf/`.

Final architecture review keeps migration/source-deletion decisions in the task
layer, execution cancellation in the existing copy_execution domain, bounded
directory work/path diagnostics in copy_completion, and protocol/binding behavior
in Native. No ClickHouse/callback policy was moved into storage. Directory/root
metadata without a database, asynchronous source cleanup for every entry class,
unified stage counters and shared fault/late-write control remain plan items.


### Root metadata capture and completion (2026-11-10 historical record)

The execution figures and `/tmp` paths in this subsection are historical
summaries from November 10. The temporary raw logs and fixed binaries did not
survive the environment refresh. They cannot be inspected as current evidence
or used to accept the November 16 working tree. Current independent results and
remaining gates are recorded in the following subsection.

The task owns one root observation captured before content enumeration. Filesystem
roots use the same selected raw metadata plan as entries; object-store virtual
roots are omitted. Root facts are stored in the existing generations table's
binary `root_snapshot String` column, separately from bounded descriptor JSON.
The SQL transport uses hex/unhex rather than a JSON Value per byte. Readback
validates source binding, empty root path, directory kind and absence of history.
Root facts do not enter base or entry counts. A large-raw roundtrip, idempotent
terminal saves and invalid-root rejection are covered by database tests.

After all target namespace mutations, the task applies/verifies root metadata,
confirms required persistence for source deletion, and commits the generation.
The bounded early-file window must also finalize root metadata first: its
eligibility already excludes every child directory and unfinished content.
It runs concurrently with fact draining, and its private completion proof avoids
reapplying root attributes at the later empty-directory finish. The original
unconfirmed-receipt source-deletion behavior is preserved. A real root cancellation
initially exposed premature flat-file deletion; that failed run was recorded at
`/tmp/terrasync-root-cancel-nfs-second-real.log`, and was not passing evidence.
The first attempt stopped at a misplaced test assertion caught by compilation.

Local/NFS reuse their existing Metadata/Namespace/persist_observed operations;
Native commits `1fcac13f5af9d29ee02986467e7c781cd52e459d` and
`69a5ce8df627ddeccd797fc201f1f9a00cbb2494` add root-path support without a new public
function/type. Root creation/deletion remain refused. Native unit suites passed
141 Local and 280 NFS tests. Both complete CI scopes passed the existing 53-step
sequence (bounded example-artifact release and the unchanged Local E2E tests).
The initial Local examples link ran out of disk; its driver records failure,
and the retry succeeded. The initial NFS Clippy rejected an oversized test helper;
it was extracted and the entire scope rerun. This is not a claim that one unchanged
`make ci` invocation passed. No nfs-rs changes were needed.

The real NFS v3/v4.1 root contract reuses the existing metadata-cancel relay:
select `--test-name direct::metadata_cancel::real_nfs_root_metadata_fault_contract`
with `--contract metadata-cancel`. Sixteen actual cases cover mode flash,
disconnect, silent timeout, cancellation and lost acknowledgement per dialect;
owner/time cancellation per dialect; v4.1 ACL SET/readback cancellation. Each
explicit recovery is independently inspected on VM 104 for body and requested
attributes, with root deletion refused. Cancellation returned under one second;
uncancelled silent loss still uses the existing per-protocol detection times.
No VM/service/export/firewall changes were made, and exact owned prefixes were
removed. Windows root ACL and other filesystem backend certification remain out
of this evidence.

Run product matrices with:

```bash
python3 tests/lab/run-directory-cancel-nfs.py --root-metadata
python3 tests/lab/run-directory-cancel-nfs.py --root-metadata --source-metadata-failure
```

The first injects a root SETATTR stall plus ClickHouse data-channel disconnect.
Its private decoder selects the root's 0750 mutation, rather than guessing from
request counts or directory-query timing. The second changes exactly the first
source-root ACL reply to PermissionDenied while preserving successful SEQUENCE;
file metadata/data calls still succeed. Both run Single/two-replica Full and
Incremental failures, two bootstrap generations and four recoveries: 20 jobs in
all. Every failed job keeps all three source files and complete target bodies,
counts zero entry errors and one precise task error, saves raw root facts and the
final report, and completes HTTP 204 diagnostics without omissions. Root facts
never increase the three-file/zero-directory scan or base count. Failed unchanged
incrementals transfer zero data bytes. Recovery is Full after a source-deleting
attempt and confirms root nanosecond mtime/mode, empty source and retired base.
Fault injection to unscoped terminal request was 3.60–4.65 seconds including
recognition; this is not a global strict-three-second certification. A final root
namespace read failure stops admission rather than starting another content-scan
recovery window. Independent metadata acquisition failure still allows data copy.

The unchanged directory-cancellation matrix passed 10 jobs/12 entry causes;
the permission matrix passed 10 jobs/280 entry causes; the ready-window suite
passed 12 jobs, including deletion before unforwarded receipts, rejected facts,
directory deferral, bounded fallback and metadata/data failures. Unprivileged
fixtures now own their source/target roots so only the originally intended
files/subdirectories receive permission faults. Their failure/count assertions
were preserved. The original combined command with both lab flags executes only
the directory suite; the ready-window suite was run separately.

Five Rust CI checks passed, including 365 app tests and 16 serial remote-process
tests (52 app environment tests remain ignored in default CI). Latest app Clippy
and the isolated no-database empty/nonempty-root test also passed. Existing
large-AppError and other repository warnings remain; they were not suppressed.
The earlier CI failure was a mock row missing the new binary column; the fixture
was updated and all five checks rerun.

Fixed CLI A/B used Rust 1.98.1 dev/debug=0, 16 workers/100000 batch,
unordered/Direct/quick, Single ClickHouse 2 CPU/1536 MiB/pool512 and no callback.
ABBAABBAABBA for four scenarios produced 48 measured runs plus 24 separate
unmeasured incremental Full baselines. No task-owned build, CI or functional test
ran during measurement. Bodies, source states, child directory metadata, base,
generation and saved reports were independently checked. The after program also
checked root nanosecond mtime/mode, raw column and root exclusion from counters;
the before program retains its known root-metadata gap. The no-database case is
flat and has no ClickHouse final report. Incremental transferred bytes are zero.
RSS samples use 50 ms intervals plus GNU time peaks.

| Scenario | Median seconds before → after | Change | Median peak RSS MiB before → after |
| --- | --- | --- | --- |
| Full move, 3000 × 1 KiB/100 directories | 5.235 → 5.225 | −0.19% | 223.04 → 223.99 |
| Unchanged retained-source incremental | 1.770 → 1.860 | +5.08% | 243.79 → 239.58 |
| Retained-source incremental, 30 mode changes | 2.065 → 2.060 | −0.24% | 243.39 → 239.09 |
| No-database retained-source Full, 3000 flat files | 0.530 → 0.480 | −9.43% | 163.36 → 168.58 |

All respective 5%/10% time gates passed, with comparable memory overhead. The
after maximum was 248.47 MiB; these finite dev samples do not establish a universal
200-MiB bound, release/large/all-backend performance, or sustained growth. No
callback was configured in benchmarks; database-enabled Full diagnostics remain
pending, not externally received. Before SHA-256 is
`90d7a5645609070ec8396304c156c0203c0752eff936d46bc20da7ce322d76b4`; after is
`3fa0624743a5e143a641eb4f667f19bad08bf95b21cec65894fe891c5dcd774f`.

Historical evidence locations (no longer available in the current environment):

- `/tmp/terrasync-root-final-ci-results.json`, five CI logs,
  `/tmp/terrasync-root-final-reviewed-clippy.log`, `/tmp/terrasync-root-final-no-db.log`.
- `/tmp/terrasync-root-cancel-nfs-third-real.log` and
  `/tmp/terrasync-root-source-acquisition-first-real.log` (20 passing jobs).
- `/tmp/terrasync-root-old-directory-cancel-real.log`,
  `/tmp/terrasync-root-old-directory-ready-first-real.log` (directory permissions),
  `/tmp/terrasync-root-ready-window-first-real.log`.
- `/tmp/native-local-root-{red,first-green,ci-driver,ci-remaining-retry-driver}.log`,
  `/tmp/native-nfs-root-{red,focused,ci-driver,ci-remaining-driver}.log`,
  `/tmp/native-nfs-root-metadata16-real/{results.json,*.log}`.
- `/tmp/terrasync-root-perf/` for both binary identities, all measured/bootstrap
  results, raw RSS, summaries and isolated server diagnostics.
- Initial compile/mock-row failures, the actual premature-deletion reproduction,
  Native Clippy failure and owned cache-release inventories remain in the matching
  `/tmp/terrasync-root-*` and `/tmp/native-*-root-*` diagnostic series.

Final architecture review keeps root observation/proof in copy_root, ordering and
commit in copy_completion, source-deletion permission in the task cleanup layer,
binary persistence/deployment differences in db, and protocol root support in
Native. This closes root metadata only. Bounded completion for every no-database
child directory, all-entry asynchronous source deletion, unified stage counts,
and shared fault/late-write control remain plan work.

### Root metadata independent revalidation (2026-11-16)

Independent `dev_test_runner` execution and `dev_gatekeeper` review are separate
from implementation and its targeted self-tests. The frozen root-feature tree
at `3d97ccf1096db68cef3b2779400a15decfd85361` plus its recorded diff passed
the five Rust CI commands: formatting, workspace/all-target check and Clippy,
workspace tests excluding the root package, and serial remote-process E2E.
App results were **370 passed / 52 ignored**, database results **144 passed /
40 ignored**, and remote-process results **16 passed**. The isolated ignored
Local no-database empty/nonempty-root case also passed. Existing Clippy warnings
remain. Raw commands, exit codes, source manifests and the reviewed diff are at
`/root/.cache/terrasync-validation-20261116/independent-software/`.
Subsequent runtime changes need new source-bound validation.

The passive lab observer was independently reviewed before execution. Its
frozen manifest SHA-256 is
`498a6a173da6c59c15ff369fd85ee572e4968f29b7ef8de4e96430900d1ccba9`.
It records existing successful ClickHouse reads and callback requests/responses;
it does not add a query or probe before forwarding `DROP`. Decoding happens
after the original suite finishes. Missing or undecodable required evidence
does not become a pass. Some permission-suite generations have no original base
count query; those counts remain unverified rather than inferred.

The first `root-source-acl` attempt failed during environment preparation:
VM104 could not start `python3`, returning I/O error and status 29. No product
migration round, database fact or callback was produced, and the remaining
suites were stopped. After the user authorized restart and then snapshot-backed
filesystem repair, the guest root was repaired and independently checked;
guest execution and NFS/RPC recovered. This is environment recovery, not product
fault acceptance. Evidence is under `independent-lab/root-source-acl/` and
`independent-lab-after-vm-reset/recovery-acceptance/` in the same persistent
results directory. This initial attempt did not validate the product fault
suites or Native root contract; their subsequent execution is recorded below.

Independent Local A/B used the original fixed baseline (`3d97ccf`, Native
`57e5d00`) and the frozen root feature (Native `69a5ce8`). Binary SHA-256:

- Before: `6a92778dfadbe1165f87592a69534be77714b9750578f0f7d8688f3a9b8d748a`.
- After: `ebe846b8e84f8d95a2ea6fbfb32b69bcc964b6f5908dd6fd695e55e07f5730e9`.

All 48 measured samples and 24 separate incremental bootstrap runs were retained.
The reviewed results are:

| Scenario | Median seconds before → after | Change | Time gate |
| --- | --- | --- | --- |
| Full move, 3000 × 1 KiB/100 directories | 5.175 → 5.225 | +0.97% | Pass, 5% |
| Unchanged retained-source incremental | 1.855 → 1.765 | −4.85% | Pass, 10% |
| Retained-source incremental, 30 mode changes | 1.930 → 1.965 | +1.81% | Pass, 10% |
| No-database retained-source Full, 3000 flat files | 0.430 → 0.530 | +23.26% | **Fail, 5%** |

Performance acceptance therefore failed overall. No-database median peak RSS
changed by about +0.02%; the maximum after RSS across scenarios was 250.34 MiB.
These finite dev samples do not establish a 200-MiB bound or large-scale behavior.
Raw results, 50-ms RSS samples, GNU time records, correctness checks, binary and
helper identities, and resource supervision are retained in
`/root/.cache/terrasync-validation-20261116/independent-performance/`.

An independently executed 8-round thread trace and two empty-root traces showed
that synchronous progress-thread joins can actually wait 5–100 ms for the
existing sleep polling to end. They support fixing that unnecessary shutdown
delay while retaining root metadata application and readback. The tracing itself
increased each run to roughly 8–9.6 seconds; it does not prove the complete cause
of the untraced regression or replace fresh performance acceptance. Evidence is
at `independent-performance-diagnosis/run-20261116T0728Z/` in the persistent
results directory. The failure above remains part of the evidence; the final
revalidation below uses a new frozen implementation and separate samples.

#### Final accepted results for this batch

The independent progress-thread fix is committed as `9693136`. It wakes each
reporter after publishing completion, retains the fixed display deadline and
100-ms fallback, and preserves final callback/statistics ordering. Eight new
deterministic regressions passed. The final combined root-feature tree passed
the same five Rust CI commands: **app 378 passed / 52 ignored**, **db 144 passed /
40 ignored**, **remote process 16 passed**, and the isolated ignored no-database
root case **1 passed / 429 filtered**. Clippy still emits warnings. Final software
evidence is in `independent-software-progress-wakeup/`.

The final after CLI SHA-256 is
`1e5132d820b7a11add879c4806a66221bdc5227e5b1b225677d4757c5467edc1`;
the fixed original baseline SHA remains `6a92778d…`. Separate results in
`independent-performance-progress-wakeup/` contain all 48 measured runs and
24 incremental bootstrap runs, using the same configuration and sequence:

| Scenario | Median seconds before → after | Change | Median peak RSS MiB before → after |
| --- | --- | --- | --- |
| Full move, 3000 × 1 KiB/100 directories | 4.995 → 4.940 | −1.10% | 222.68 → 226.27 |
| Unchanged retained-source incremental | 1.785 → 1.670 | −6.44% | 243.81 → 238.88 |
| Retained-source incremental, 30 mode changes | 2.005 → 1.865 | −6.98% | 245.90 → 244.00 |
| No-database retained-source Full, 3000 flat files | 0.430 → 0.430 | 0.00% | 165.17 → 166.99 |

All four time gates passed with comparable median memory overhead. Maximum
after RSS was 252.73 MiB; this does not establish a 200-MiB bound or certify
release, large/deep trees, other backends or sustained memory behavior.

The independent product suites in `independent-lab-final/` ran 52 rounds:

| Suite | Rounds | Product test exit | Observer exit | Evidence boundary |
| --- | --- | --- | --- | --- |
| Source-root ACL refusal | 10 | 0 | 0 | All root snapshots, final reports and base reads captured |
| Root mutation cancellation + CH fault | 10 | 0 | 0 | Same per-round evidence; failed source files retained |
| Child-directory cancellation + CH fault | 10 | 0 | **2** | Two successful retained bootstrap rounds never query base; those counts remain unverified |
| Directory permission failures | 10 | 0 | 0 | 280 failure causes; two unqueried bootstrap counts explicitly unverified |
| Ready cleanup before fact receipts | 12 | 0 | 0 | Receipt/rejection, directory deferral, bounded fallback and data/metadata failure cases |

The directory observer's original `complete=false` and exit 2 are preserved.
Independent review confirmed that its missing reads were not lost responses:
the original non-root bootstrap branch does not query base. All four failed and
four recovery rounds did read base zero. This is acceptance of the suite's
original contract with a named unverified subitem, not complete observer
acceptance. No extra query was added to a fault path or cleanup.

Raw callback requests and their actual HTTP 204 response headers are retained
for all five suites. Source/target bodies, source retention on failure, recovery,
task/entry counters, persisted final reports and root snapshot bindings were
independently checked where required. Root facts remain outside ordinary base
and counters. Product root suites use NFS v4.1. Their end-to-end or
fault-to-terminal timings include detection and other stages and do not prove
a shared three-second recovery window. ClickHouse reads certify the observed
node and moment, not every replica.

The unchanged Native `69a5ce8` root contract additionally passed **8 groups /
16 actual cases** on NFS v3 and v4.1. Mode covers flash, disconnect, silent stall,
cancellation and lost acknowledgement on both dialects; owner/time cancellation
is tested on both, and v4.1 ACL SET/readback cancellation separately. Test binary
SHA-256 is `28f85cf31608ff567ce301ec682f63f8890cb2bebadfccc95c3724bcc112d0da`.
Each group has nonzero actual RPC fault counters, explicit recovery, independent
server body/attribute inspection, confirmed process-group shutdown and removal
of its own UUID prefix. All `cleanup_pending` values are false.

Native evidence is in `native-root-final/`. Cancellation tests assert return
within one second and preserve Cancelled/Unknown with unconfirmed write outcome;
logged latency was zero at millisecond resolution. Silent-stall detection was
about 5 seconds for v3 and 15 seconds for v4.1, not an identified-failure recovery
measurement. ACL inspection compares owner/mode and presence/raw bytes of both
server POSIX ACL xattrs (both absent in these fixtures); it does not independently
certify every NFSv4 ACE. Birth/ctime/atime and all xattr/tag bytes are not broadly
certified by this batch.

Source contents and artifacts were frozen throughout acceptance. Current
product suites have actual before/after 559-file manifests; Native has matching
1189-file manifests. The earlier software/performance binding was strengthened
by combining the original complete snapshot, reviewed repair manifest and
current full comparison (`supplemental-binding/`), explicitly not a fabricated
contemporaneous full pre-test snapshot. Runtime contents did not change when
the independent fix was committed. Final documentation edits are separate.

Independent gatekeeping accepts the root-metadata batch within these limits.
All-entry asynchronous cleanup, bounded completion for every no-database child
directory, unified stage counters and shared fault/late-write control remain
plan work. The overall optimization plan is not complete. Historical Native
53-step CI summaries above still have no surviving raw evidence in this session.
