# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

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
