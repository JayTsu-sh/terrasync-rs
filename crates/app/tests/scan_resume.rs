//! Real CLI termination and restart against ClickHouse.
#![cfg(feature = "lab-fault-injection")]

use clickhouse::{Client, query::Query};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn client(database: &str) -> TestDatabase {
    let mut value = Client::default()
        .with_url(std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into()))
        .with_database(database)
        .with_user(std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()));
    if let Ok(password) = std::env::var("LAB_CLICKHOUSE_PASSWORD") {
        value = value.with_password(password);
    }
    if std::env::var("LAB_CLICKHOUSE_CLUSTER").is_ok() {
        value = value
            .with_option("select_sequential_consistency", "1")
            .with_option("insert_quorum_parallel", "0")
            .with_option("prefer_localhost_replica", "0")
            .with_option("load_balancing", "in_order");
    }
    TestDatabase(value)
}

struct TestDatabase(Client);
impl TestDatabase {
    fn query(&self, sql: &str) -> Query {
        let Ok(cluster) = std::env::var("LAB_CLICKHOUSE_CLUSTER") else {
            return self.0.query(sql);
        };
        assert!(cluster.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
        let mut sql = sql.to_string();
        if sql.starts_with("CREATE DATABASE") || sql.starts_with("DROP DATABASE") {
            sql.push_str(&format!(" ON CLUSTER {cluster}"));
        } else if sql.contains("system.tables") && sql.contains("name='incremental_resume_case'") {
            sql = "SELECT count() FROM incremental FINAL WHERE job_id='resume_case'".into();
        } else {
            for (legacy, shared) in [
                ("base_resume_case", "base"),
                ("incremental_resume_case", "incremental"),
                ("scan_resume_resume_case", "scan_resume"),
                ("state_resume_case", "state"),
            ] {
                let source = format!("FROM {legacy} FINAL");
                sql = sql.replace(
                    &format!("{source} WHERE "),
                    &format!("FROM {shared} FINAL WHERE job_id='resume_case' AND "),
                );
                sql = sql.replace(&source, &format!("FROM {shared} FINAL WHERE job_id='resume_case'"));
            }
            if sql.contains("toUInt64(scan_state)") {
                sql = format!(
                    "SELECT ifNull(({}),toUInt64(0))",
                    sql.replace("scan_state", "generation")
                );
            }
        }
        self.0.query(&sql)
    }
}

struct DatabaseGuard(String);
impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        let name = self.0.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                let _ = runtime.block_on(
                    client("default")
                        .query(&format!("DROP DATABASE IF EXISTS {name}"))
                        .execute(),
                );
            }
        })
        .join();
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn paths(root: &Path) -> Result<BTreeSet<String>> {
    fn walk(root: &Path, parent: &Path, values: &mut BTreeSet<String>) -> Result {
        for child in fs::read_dir(parent)? {
            let path = child?.path();
            values.insert(path.strip_prefix(root)?.to_string_lossy().into_owned());
            if path.is_dir() {
                walk(root, &path, values)?;
            }
        }
        Ok(())
    }
    let mut values = BTreeSet::new();
    walk(root, root, &mut values)?;
    Ok(values)
}

struct ScanCommand {
    command: Command,
    log: PathBuf,
}
fn command(root: &Path, config: &Path, source: &Path, name: &str) -> Result<ScanCommand> {
    command_for_mode(root, config, source, name, true)
}
fn command_for_mode(root: &Path, config: &Path, source: &Path, name: &str, resume: bool) -> Result<ScanCommand> {
    // 普通文件无法作为临时目录，证明扫描不依赖本地暂存。
    fs::write(root.join("tmp-disabled"), b"no scan staging")?;
    let mut command = Command::new(std::env::var("TERRASYNC_TEST_BINARY")?);
    let log = root.join(format!("{name}.log"));
    let output = fs::File::create(&log)?;
    command
        .current_dir(root)
        .env("TMPDIR", root.join("tmp-disabled"))
        .arg("-c")
        .arg(config)
        .args(["scan", "--id", "resume_case"])
        .arg(source)
        .env("TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS", "0")
        .env_remove("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS")
        .env_remove("TERRASYNC_LAB_SCAN_STOP_AFTER_CHECKPOINT")
        .env_remove("TERRASYNC_LAB_SCAN_FAIL_AFTER_EVENTS")
        .env_remove("TERRASYNC_LAB_SCAN_FAIL_AFTER_FINALIZING")
        .stdout(Stdio::from(output.try_clone()?))
        .stderr(Stdio::from(output));
    if resume {
        command.arg("--resume");
    }
    Ok(ScanCommand { command, log })
}

fn discard_pod_local_state(root: &Path, phase: &str) -> Result {
    for name in ["jobs", "tmp"] {
        let path = root.join(name);
        if path.exists() {
            fs::rename(path, root.join(format!("old-pod-{phase}-{name}")))?;
        }
    }
    Ok(())
}

fn write_scan_config(config: &Path, database: &str, batch_size: usize) -> Result {
    let dsn = serde_json::to_string(
        &std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into()),
    )?;
    let user = serde_json::to_string(&std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()))?;
    let password = serde_json::to_string(&std::env::var("LAB_CLICKHOUSE_PASSWORD").unwrap_or_default())?;
    let cluster = std::env::var("LAB_CLICKHOUSE_CLUSTER")
        .map(|value| format!("mode=\"cluster\"\ncluster={value:?}\n"))
        .unwrap_or_default();
    fs::write(
        config,
        format!(
            "[database]\nenabled=true\ntype=\"clickhouse\"\nbatch_size={batch_size}\n[database.clickhouse]\ndsn={dsn}\ndatabase=\"{database}\"\nusername={user}\npassword={password}\n{cluster}[scan]\nconcurrency=1\n"
        ),
    )?;
    Ok(())
}

async fn interrupt_unordered_round(mut scan: ScanCommand, database: &TestDatabase, total: u64) -> Result {
    let previous = count(database, "SELECT version FROM scan_resume_resume_case FINAL")
        .await
        .unwrap_or_default();
    let mut process = ChildGuard(scan.command.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "no partial unordered output: {}",
            fs::read_to_string(&scan.log)?
        );
        assert!(
            process.0.try_wait()?.is_none(),
            "scan exited before interruption: {}",
            fs::read_to_string(&scan.log)?
        );
        if let Ok((version, phase, generation, epoch)) = database
            .query("SELECT version,phase,generation,event_time FROM scan_resume_resume_case FINAL")
            .fetch_one::<(u64, u8, u8, i64)>()
            .await
            && version > previous
            && phase == 0
        {
            let sql = if std::env::var("LAB_CLICKHOUSE_CLUSTER").is_ok() {
                format!("SELECT count() FROM base_resume_case FINAL WHERE epoch={epoch} AND is_deleted=0")
            } else {
                format!("SELECT count() FROM base_resume_case FINAL WHERE current_state={generation}")
            };
            let rows = count(database, &sql).await?;
            if rows > 0 && rows < total {
                break;
            }
        }
        sleep(Duration::from_millis(5)).await;
    }
    let started = Instant::now();
    assert!(
        Command::new("kill")
            .args(["-INT", &process.0.id().to_string()])
            .status()?
            .success()
    );
    loop {
        if let Some(status) = process.0.try_wait()? {
            assert!(!status.success());
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "unordered cancellation exceeded two seconds"
        );
        sleep(Duration::from_millis(10)).await;
    }
    let (observed, cursor): (u64, u8) = database
        .query("SELECT observed_entries,notEmpty(checkpoint) FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!(
        (observed, cursor),
        (0, 0),
        "Unordered must not publish a resumable cursor"
    );
    eprintln!(
        "Unordered cancellation: {:.3}s; no resumable checkpoint",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse and TERRASYNC_TEST_BINARY built with lab-fault-injection"]
async fn unordered_full_and_incremental_restart_cleanly_without_resumable_checkpoints() -> Result {
    let name = format!(
        "ts_scan_unordered_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    client("default")
        .query(&format!("CREATE DATABASE {name}"))
        .execute()
        .await?;
    let _database_guard = DatabaseGuard(name.clone());
    let database = client(&name);
    let work = tempfile::tempdir()?;
    let source = work.path().join("source");
    fs::create_dir(&source)?;
    for index in 0..5000 {
        fs::write(source.join(format!("file{index:05}")), b"value")?;
    }
    let config = work.path().join("config.toml");
    write_scan_config(&config, &name, 16)?;
    interrupt_unordered_round(
        command_for_mode(work.path(), &config, &source, "unordered-full-cancel", false)?,
        &database,
        5000,
    )
    .await?;
    let tail = database
        .query("SELECT relative_path FROM base_resume_case FINAL LIMIT 1")
        .fetch_one::<String>()
        .await?;
    fs::remove_file(source.join(&tail))?;
    discard_pod_local_state(work.path(), "unordered-full")?;
    write_scan_config(&config, &name, 1024)?;
    complete(command_for_mode(
        work.path(),
        &config,
        &source,
        "unordered-full-restart",
        false,
    )?)
    .await?;
    assert_paths(&database, &source).await?;
    assert_eq!(
        count(
            &database,
            "SELECT toUInt64(incremental) FROM scan_resume_resume_case FINAL"
        )
        .await?,
        0
    );
    complete(command_for_mode(
        work.path(),
        &config,
        &source,
        "unordered-incremental-unchanged",
        false,
    )?)
    .await?;
    assert_paths(&database, &source).await?;
    let epoch = database
        .query("SELECT event_time FROM scan_resume_resume_case FINAL")
        .fetch_one::<i64>()
        .await?;
    assert_eq!(
        count(
            &database,
            &format!(
                "SELECT count() FROM incremental_resume_case FINAL WHERE toUnixTimestamp64Nano(create_at)>={epoch}"
            )
        )
        .await?,
        0
    );
    let existing: Vec<_> = paths(&source)?.into_iter().collect();
    // 先新增再删除，避免文件系统立即复用删除条目的 inode，使变化量精确可核验。
    for index in 0..20 {
        fs::write(source.join(format!("added{index:05}")), b"new")?;
    }
    for path in &existing[10..30] {
        fs::write(source.join(path), b"modified-value")?;
    }
    for path in &existing[..10] {
        fs::remove_file(source.join(path))?;
    }
    complete(command_for_mode(
        work.path(),
        &config,
        &source,
        "unordered-incremental-one-percent",
        false,
    )?)
    .await?;
    assert_paths(&database, &source).await?;
    let epoch = database
        .query("SELECT event_time FROM scan_resume_resume_case FINAL")
        .fetch_one::<i64>()
        .await?;
    let events=database.query(&format!("SELECT countIf(operation_type='new'),countIf(endsWith(operation_type,'changed')),countIf(operation_type='deleted') FROM incremental_resume_case FINAL WHERE toUnixTimestamp64Nano(create_at)>={epoch}")).fetch_one::<(u64,u64,u64)>().await?;
    assert_eq!(events, (20, 20, 10));
    write_scan_config(&config, &name, 16)?;
    interrupt_unordered_round(
        command_for_mode(work.path(), &config, &source, "unordered-incremental-cancel", false)?,
        &database,
        5009,
    )
    .await?;
    discard_pod_local_state(work.path(), "unordered-incremental")?;
    write_scan_config(&config, &name, 1024)?;
    complete(command_for_mode(
        work.path(),
        &config,
        &source,
        "unordered-incremental-restart",
        false,
    )?)
    .await?;
    assert_paths(&database, &source).await?;
    assert_eq!(
        count(
            &database,
            "SELECT toUInt64(incremental) FROM scan_resume_resume_case FINAL"
        )
        .await?,
        1
    );
    Ok(())
}

async fn count(database: &TestDatabase, sql: &str) -> Result<u64> {
    Ok(database.query(sql).fetch_one::<u64>().await?)
}

async fn interrupt_after_checkpoint(mut scan: ScanCommand, database: &TestDatabase, graceful: bool) -> Result {
    let previous_version = database
        .query("SELECT version FROM scan_resume_resume_case FINAL")
        .fetch_one::<u64>()
        .await
        .unwrap_or_default();
    scan.command
        .env("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS", "10000");
    let mut process = ChildGuard(scan.command.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "checkpoint not saved: {}",
            fs::read_to_string(&scan.log)?
        );
        assert!(
            process.0.try_wait()?.is_none(),
            "scan exited early: {}",
            fs::read_to_string(&scan.log)?
        );
        if let Ok((entries, phase, cursor, version)) = database
            .query("SELECT observed_entries, phase, notEmpty(checkpoint), version FROM scan_resume_resume_case FINAL")
            .fetch_one::<(u64, u8, u8, u64)>()
            .await
            && entries > 0
            && phase == 0
            && cursor == 1
            && version > previous_version
        {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    if graceful {
        let started = Instant::now();
        assert!(
            Command::new("kill")
                .args(["-INT", &process.0.id().to_string()])
                .status()?
                .success()
        );
        loop {
            if let Some(status) = process.0.try_wait()? {
                assert!(!status.success());
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "scan did not cancel within two seconds"
            );
            sleep(Duration::from_millis(10)).await;
        }
        eprintln!("Graceful scan cancellation: {:.3}s", started.elapsed().as_secs_f64());
    } else {
        process.0.kill()?;
        assert!(!process.0.wait()?.success());
    }
    Ok(())
}

async fn complete(mut scan: ScanCommand) -> Result {
    // 中断夹具用零间隔迅速取得边界；正常完成使用产品的 60 秒间隔。
    scan.command.env("TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS", "60000");
    let mut process = ChildGuard(scan.command.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = process.0.try_wait()? {
            assert!(status.success(), "scan failed: {}", fs::read_to_string(&scan.log)?);
            for line in fs::read_to_string(&scan.log)?
                .lines()
                .filter(|line| line.contains("Recovered baseline paths"))
            {
                eprintln!("baseline confirmation: {line}");
            }
            break;
        }
        assert!(
            Instant::now() < deadline,
            "scan timed out: {}",
            fs::read_to_string(&scan.log)?
        );
        sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

async fn assert_cumulative_observations(database: &TestDatabase) -> Result {
    let (observed, saved): (u64, String) = database
        .query("SELECT observed_entries,statistics FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    let checkpoint: serde_json::Value = serde_json::from_str(&saved)?;
    let scan = &checkpoint["report"]["scan"];
    assert_eq!(
        scan["scanned_entries"].as_u64(),
        Some(observed),
        "checkpoint count must be the unique cumulative logical scan count"
    );
    assert!(observed <= 1000, "checkpoint must never add replayed entries");
    let scalar = checkpoint["stats"]
        .get("Full")
        .or_else(|| {
            checkpoint["stats"]
                .get("Incremental")
                .and_then(|value| value.get("scanned"))
        })
        .ok_or("missing confirmed scalar statistics")?;
    let scalar_total = scalar["dir_count"].as_u64().ok_or("missing directories")?
        + scalar["regular_file_count"].as_u64().ok_or("missing files")?
        + scalar["symlink_count"].as_u64().ok_or("missing symlinks")?;
    assert_eq!(
        scalar_total, observed,
        "raw replay must not alter durable scalar counts"
    );
    eprintln!("Confirmed checkpoint: logical={observed}, durable scalar={scalar_total}; Pod local state discarded");
    assert_eq!(scan["filtered_entries"].as_u64(), Some(0));
    assert_eq!(scan["retained_entries"].as_u64(), Some(observed));
    Ok(())
}

async fn assert_paths(database: &TestDatabase, source: &Path) -> Result {
    let rows = database
        .query("SELECT relative_path FROM base_resume_case FINAL")
        .fetch_all::<String>()
        .await?;
    let actual: BTreeSet<_> = rows.iter().cloned().collect();
    assert_eq!(rows.len(), actual.len(), "duplicate base rows");
    assert_eq!(actual, paths(source)?);
    eprintln!(
        "Actual source paths={}, unique database paths={}, FINAL rows={}",
        actual.len(),
        actual.len(),
        rows.len()
    );
    let raw = database
        .query("SELECT statistics FROM scan_resume_resume_case FINAL")
        .fetch_one::<String>()
        .await?;
    let snapshot: serde_json::Value = serde_json::from_str(&raw)?;
    let stats = snapshot["stats"]
        .get("Full")
        .or_else(|| {
            snapshot["stats"]
                .get("Incremental")
                .and_then(|value| value.get("scanned"))
        })
        .ok_or("missing durable statistics")?;
    assert_eq!(
        snapshot["report"]["scan"]["scanned_entries"].as_u64(),
        Some(actual.len() as u64)
    );
    assert_eq!(
        snapshot["report"]["scan"]["retained_entries"].as_u64(),
        Some(actual.len() as u64)
    );
    assert_eq!(snapshot["report"]["scan"]["filtered_entries"].as_u64(), Some(0));
    assert_eq!(snapshot["report"]["scan"]["complete"].as_bool(), Some(true));
    let files = count(database, "SELECT countIf(is_dir=0) FROM base_resume_case FINAL").await?;
    let dirs = count(database, "SELECT countIf(is_dir=1) FROM base_resume_case FINAL").await?;
    assert_eq!(
        stats["regular_file_count"].as_u64(),
        Some(files),
        "resume stats kind={:?}; file count mismatch",
        snapshot["stats"]
            .as_object()
            .map(|value| value.keys().collect::<Vec<_>>())
    );
    assert_eq!(
        stats["dir_count"].as_u64(),
        Some(dirs),
        "resume must not double-count replayed directories"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse and TERRASYNC_TEST_BINARY built with lab-fault-injection"]
async fn finalized_full_scan_reuses_its_sealed_metadata_after_pod_loss() -> Result {
    let name = format!(
        "ts_scan_finalizing_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    client("default")
        .query(&format!("CREATE DATABASE {name}"))
        .execute()
        .await?;
    let _guard = DatabaseGuard(name.clone());
    let database = client(&name);
    let work = tempfile::tempdir()?;
    let source = work.path().join("source");
    fs::create_dir(&source)?;
    for number in 0..512 {
        fs::write(source.join(format!("file{number:04}")), b"sealed")?;
    }
    let config = work.path().join("config.toml");
    write_scan_config(&config, &name, 1024)?;
    let mut first = command(work.path(), &config, &source, "failed-after-source-seal")?;
    first
        .command
        // 功能测试强制完整目录边界到期；短扫描成功结束时 SDK 不额外发送最终 checkpoint。
        .env("TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS", "0")
        .env("TERRASYNC_LAB_SCAN_FAIL_AFTER_FINALIZING", "1")
        .env("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS", "3000");
    let mut first = ChildGuard(first.command.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(30);
    let checkpoint_cut = loop {
        let current = database
            .query("SELECT phase,checkpoint_snapshot,observed_entries FROM scan_resume_resume_case FINAL")
            .fetch_one::<(u8, String, u64)>()
            .await;
        if let Ok((phase, token, entries)) = &current
            && *phase == 0
            && *entries == 512
        {
            break token.clone();
        }
        assert!(Instant::now() < deadline, "final checkpoint was not observed");
        assert!(
            first.0.try_wait()?.is_none(),
            "scan exited before its final checkpoint was observed: {current:?}; {}",
            fs::read_to_string(work.path().join("failed-after-source-seal.log"))?
        );
        sleep(Duration::from_millis(20)).await;
    };
    assert!(!first.0.wait()?.success());
    let (phase, sealed, observed): (u8, String, u64) = database
        .query("SELECT phase,checkpoint_snapshot,observed_entries FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!(phase, 1);
    assert!(!sealed.is_empty());
    assert_eq!(observed, 512);
    assert_eq!(
        sealed, checkpoint_cut,
        "FINALIZING must reuse a final checkpoint with no subsequent observation writes"
    );
    let metadata = "SELECT toJSONString(tuple(*)) FROM base_resume_case FINAL ORDER BY relative_path,version_id";
    let before = database.query(metadata).fetch_all::<String>().await?;
    assert_eq!(before.len(), 512);
    // 枚举已完成：恢复 FINALIZING 必须发布封存事实，新变化留给下一轮。
    fs::write(source.join("file0000"), b"modified-after-source-completion")?;
    fs::write(source.join("late-file"), b"late")?;
    discard_pod_local_state(work.path(), "finalizing")?;
    complete(command(work.path(), &config, &source, "recover-source-seal")?).await?;
    let (phase, resumed, observed): (u8, String, u64) = database
        .query("SELECT phase,checkpoint_snapshot,observed_entries FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!(phase, 2);
    assert_eq!(
        resumed, sealed,
        "read-only full finalization must retain its acknowledged metadata cut"
    );
    assert_eq!(observed, 512);
    assert_eq!(database.query(metadata).fetch_all::<String>().await?, before);
    complete(command(work.path(), &config, &source, "next-round-after-source-seal")?).await?;
    assert_paths(&database, &source).await?;
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='new'"
        )
        .await?,
        1
    );
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='data_changed'"
        )
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse and TERRASYNC_TEST_BINARY built with lab-fault-injection"]
async fn observations_after_an_acknowledged_checkpoint_require_a_new_metadata_cut() -> Result {
    let name = format!(
        "ts_scan_cut_updates_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    client("default")
        .query(&format!("CREATE DATABASE {name}"))
        .execute()
        .await?;
    let _guard = DatabaseGuard(name.clone());
    let database = client(&name);
    let work = tempfile::tempdir()?;
    let source = work.path().join("source");
    fs::create_dir(&source)?;
    for directory in 0..30 {
        let path = source.join(format!("dir{directory}"));
        fs::create_dir(&path)?;
        for file in 0..13 {
            fs::write(path.join(format!("file{file}")), b"facts")?;
        }
    }
    for file in 0..170 {
        fs::write(source.join(format!("root-file{file}")), b"facts")?;
    }
    let total = paths(&source)?.len() as u64;
    let config = work.path().join("config.toml");
    write_scan_config(&config, &name, 8)?;
    let mut scan = command(work.path(), &config, &source, "all-directory-checkpoints")?;
    scan.command
        .env("TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS", "0")
        .env("TERRASYNC_LAB_SCAN_FAIL_AFTER_FINALIZING", "1")
        .env("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS", "300");
    let mut process = ChildGuard(scan.command.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(60);
    let previous_cut = loop {
        if let Ok((phase, token, entries)) = database
            .query("SELECT phase,checkpoint_snapshot,observed_entries FROM scan_resume_resume_case FINAL")
            .fetch_one::<(u8, String, u64)>()
            .await
            && phase == 0
            && entries > 0
            && entries < total
        {
            eprintln!("Confirmed middle checkpoint: {entries}/{total}");
            break token;
        }
        assert!(Instant::now() < deadline, "middle checkpoint was not observed");
        assert!(
            process.0.try_wait()?.is_none(),
            "scan exited before a middle checkpoint: {}",
            fs::read_to_string(&scan.log)?
        );
        sleep(Duration::from_millis(20)).await;
    };
    assert!(!process.0.wait()?.success());
    let (phase, token, observed): (u8, String, u64) = database
        .query("SELECT phase,checkpoint_snapshot,observed_entries FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!((phase, observed), (1, total));
    assert_ne!(token, previous_cut, "later observations require a new metadata cut");
    let names = database
        .query("SELECT name FROM system.tables WHERE database=? AND endsWith(name,?)")
        .bind(&name)
        .bind(format!("_{token}_base"))
        .fetch_all::<String>()
        .await?;
    assert_eq!(names.len(), 1);
    let table = &names[0];
    assert!(table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    assert_eq!(
        database
            .0
            .query(&format!("SELECT count() FROM {table} FINAL"))
            .fetch_one::<u64>()
            .await?,
        observed
    );
    discard_pod_local_state(work.path(), "all-directory-checkpoints")?;
    complete(command(
        work.path(),
        &config,
        &source,
        "recover-last-directory-checkpoint",
    )?)
    .await?;
    assert_paths(&database, &source).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse and TERRASYNC_TEST_BINARY built with lab-fault-injection"]
async fn killed_scan_resumes_full_and_incremental_without_skips_or_duplicate_events() -> Result {
    let name = format!(
        "ts_scan_resume_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    client("default")
        .query(&format!("CREATE DATABASE {name}"))
        .execute()
        .await?;
    let _database_guard = DatabaseGuard(name.clone());
    let database = client(&name);
    let work = tempfile::tempdir()?;
    let source = work.path().join("source");
    fs::create_dir(&source)?;
    for index in 0..30 {
        let dir = source.join(format!("dir{index:03}"));
        fs::create_dir(&dir)?;
        for number in 0..if index < 20 { 27 } else { 26 } {
            fs::write(dir.join(format!("file{number}")), b"value")?;
        }
    }
    fs::write(source.join("victim"), b"will be deleted")?;
    for number in 0..169 {
        fs::write(source.join(format!("root-file{number}")), b"value")?;
    }
    assert_eq!(paths(&source)?.len(), 1000);
    let config = work.path().join("config.toml");
    let dsn = serde_json::to_string(
        &std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into()),
    )?;
    let user = serde_json::to_string(&std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()))?;
    let password = serde_json::to_string(&std::env::var("LAB_CLICKHOUSE_PASSWORD").unwrap_or_default())?;
    let cluster_setting = std::env::var("LAB_CLICKHOUSE_CLUSTER")
        .map(|value| format!("mode=\"cluster\"\ncluster={value:?}\n"))
        .unwrap_or_default();
    fs::write(
        &config,
        format!(
            "[database]\nenabled=true\ntype=\"clickhouse\"\nbatch_size=16\n[database.clickhouse]\ndsn={dsn}\ndatabase=\"{name}\"\nusername={user}\npassword={password}\n{cluster_setting}[scan]\nconcurrency=1\n"
        ),
    )?;
    interrupt_after_checkpoint(
        command(work.path(), &config, &source, "full-interrupted")?,
        &database,
        false,
    )
    .await?;
    let full_round: (bool, u8, i64) = database
        .query("SELECT incremental,generation,event_time FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert!(!full_round.0, "an interrupted first scan must remain full");
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        0
    );
    let before = count(&database, "SELECT version FROM scan_resume_resume_case FINAL").await?;
    let mut mismatch = command(work.path(), &config, &source, "mismatched-options")?;
    assert!(!mismatch.command.args(["--match", "type==file"]).status()?.success());
    assert_eq!(
        before,
        count(&database, "SELECT version FROM scan_resume_resume_case FINAL").await?
    );
    discard_pod_local_state(work.path(), "full-hard")?;
    interrupt_after_checkpoint(
        command(work.path(), &config, &source, "full-resume-interrupted")?,
        &database,
        true,
    )
    .await?;
    assert_cumulative_observations(&database).await?;
    discard_pod_local_state(work.path(), "full-graceful")?;
    complete(command(work.path(), &config, &source, "full-resumed")?).await?;
    let resumed_full: (bool, u8, i64) = database
        .query("SELECT incremental,generation,event_time FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!(resumed_full, full_round, "full resume must preserve its round and type");
    assert_eq!(
        count(&database, "SELECT count() FROM incremental_resume_case FINAL").await?,
        0,
        "full scans must not generate incremental events"
    );
    assert_paths(&database, &source).await?;
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        1
    );
    assert_eq!(
        count(&database, "SELECT toUInt64(phase) FROM scan_resume_resume_case FINAL").await?,
        2
    );

    fs::write(source.join("dir000/file0"), b"longer changed content")?;
    fs::write(source.join("added"), b"new")?;
    fs::remove_file(source.join("victim"))?;
    interrupt_after_checkpoint(
        command(work.path(), &config, &source, "incremental-interrupted")?,
        &database,
        false,
    )
    .await?;
    let incremental_round: (bool, u8, i64) = database
        .query("SELECT incremental,generation,event_time FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert!(incremental_round.0, "only a completed full scan can become incremental");
    assert_ne!(incremental_round.2, full_round.2);
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        1
    );
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM base_resume_case FINAL WHERE relative_path='victim'"
        )
        .await?,
        1
    );
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='deleted'"
        )
        .await?,
        0
    );
    discard_pod_local_state(work.path(), "incremental-hard")?;
    interrupt_after_checkpoint(
        command(work.path(), &config, &source, "incremental-resume-interrupted")?,
        &database,
        true,
    )
    .await?;
    assert_cumulative_observations(&database).await?;
    discard_pod_local_state(work.path(), "incremental-graceful")?;
    complete(command(work.path(), &config, &source, "incremental-resumed")?).await?;
    let resumed_incremental: (bool, u8, i64) = database
        .query("SELECT incremental,generation,event_time FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!(
        resumed_incremental, incremental_round,
        "incremental resume must preserve its round and type"
    );
    assert_paths(&database, &source).await?;
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='new' AND relative_path='added'"
        )
        .await?,
        1
    );
    assert_eq!(count(&database, "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='data_changed' AND relative_path='dir000/file0'").await?, 1);
    assert_eq!(count(&database, "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='deleted' AND relative_path='victim'").await?, 1);
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        0
    );

    fs::write(source.join("another"), b"event before base")?;
    let mut injected = command(work.path(), &config, &source, "durable-event-failure")?;
    assert!(
        !injected
            .command
            .env("TERRASYNC_LAB_SCAN_FAIL_AFTER_EVENTS", "1")
            .status()?
            .success()
    );
    assert!(fs::read_to_string(&injected.log)?.contains("Lab failure after durable events"));
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='new' AND relative_path='another'"
        )
        .await?,
        1
    );
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        0
    );
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM base_resume_case FINAL WHERE relative_path='another'"
        )
        .await?,
        0
    );
    complete(command(work.path(), &config, &source, "durable-event-replayed")?).await?;
    assert_paths(&database, &source).await?;
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM incremental_resume_case FINAL WHERE operation_type='new' AND relative_path='another'"
        )
        .await?,
        1
    );
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        1
    );
    complete(command(
        work.path(),
        &config,
        &source,
        "completed-scan-starts-new-round",
    )?)
    .await?;
    assert_eq!(
        count(&database, "SELECT toUInt64(scan_state) FROM state_resume_case FINAL").await?,
        0
    );
    let final_events = database.query("SELECT operation_type,relative_path,toUnixTimestamp64Nano(create_at),ifNull(comment,'') FROM incremental_resume_case FINAL ORDER BY create_at,operation_type,relative_path")
        .fetch_all::<(String,String,i64,String)>().await?;
    assert_eq!(final_events.len(), 4, "unexpected durable events: {final_events:?}");
    Ok(())
}
