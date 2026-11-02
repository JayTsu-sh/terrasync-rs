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
    let mut command = Command::new(std::env::var("TERRASYNC_TEST_BINARY")?);
    let log = root.join(format!("{name}.log"));
    let output = fs::File::create(&log)?;
    command
        .current_dir(root)
        .arg("-c")
        .arg(config)
        .args(["scan", "--id", "resume_case", "--resume"])
        .arg(source)
        .env("TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS", "0")
        .env_remove("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS")
        .env_remove("TERRASYNC_LAB_SCAN_STOP_AFTER_CHECKPOINT")
        .env_remove("TERRASYNC_LAB_SCAN_FAIL_AFTER_EVENTS")
        .stdout(Stdio::from(output.try_clone()?))
        .stderr(Stdio::from(output));
    Ok(ScanCommand { command, log })
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
        tokio::time::sleep(Duration::from_millis(25)).await;
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        eprintln!("Graceful scan cancellation: {:.3}s", started.elapsed().as_secs_f64());
    } else {
        process.0.kill()?;
        assert!(!process.0.wait()?.success());
    }
    Ok(())
}

async fn complete(mut scan: ScanCommand) -> Result {
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
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

async fn assert_cumulative_observations(database: &TestDatabase) -> Result {
    let (observed, saved): (u64, String) = database
        .query("SELECT observed_entries,statistics FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    let attempt: serde_json::Value = serde_json::from_str(&saved)?;
    let attempt = attempt[0]
        .get("Full")
        .or_else(|| attempt[0].get("Incremental").and_then(|value| value.get("scanned")))
        .ok_or("missing scanned statistics")?;
    let attempt_entries = attempt["dir_count"].as_u64().ok_or("missing directory count")?
        + attempt["regular_file_count"].as_u64().ok_or("missing file count")?
        + attempt["symlink_count"].as_u64().ok_or("missing symlink count")?;
    assert_eq!(
        attempt_entries, observed,
        "live resume statistics must include the previous checkpoint plus new observations"
    );
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
    let raw = database
        .query("SELECT statistics FROM scan_resume_resume_case FINAL")
        .fetch_one::<String>()
        .await?;
    let snapshot: serde_json::Value = serde_json::from_str(&raw)?;
    let stats = snapshot[0]
        .get("Full")
        .or_else(|| snapshot[0].get("Incremental").and_then(|value| value.get("scanned")))
        .ok_or("missing durable statistics")?;
    let files = count(database, "SELECT countIf(is_dir=0) FROM base_resume_case FINAL").await?;
    let dirs = count(database, "SELECT countIf(is_dir=1) FROM base_resume_case FINAL").await?;
    assert_eq!(
        stats["regular_file_count"].as_u64(),
        Some(files),
        "resume stats kind={:?}; file count mismatch",
        snapshot[0].as_object().map(|value| value.keys().collect::<Vec<_>>())
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
        for number in 0..3 {
            fs::write(dir.join(format!("file{number}")), b"value")?;
        }
    }
    fs::write(source.join("victim"), b"will be deleted")?;
    let config = work.path().join("config.toml");
    let dsn = serde_json::to_string(
        &std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into()),
    )?;
    let user = serde_json::to_string(&std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()))?;
    let password = serde_json::to_string(&std::env::var("LAB_CLICKHOUSE_PASSWORD").unwrap_or_default())?;
    let cluster_setting = std::env::var("LAB_CLICKHOUSE_CLUSTER")
        .map(|value| format!("cluster={value:?}\n"))
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
    interrupt_after_checkpoint(
        command(work.path(), &config, &source, "full-resume-interrupted")?,
        &database,
        true,
    )
    .await?;
    assert_cumulative_observations(&database).await?;
    complete(command(work.path(), &config, &source, "full-resumed")?).await?;
    let resumed_full: (bool, u8, i64) = database
        .query("SELECT incremental,generation,event_time FROM scan_resume_resume_case FINAL")
        .fetch_one()
        .await?;
    assert_eq!(resumed_full, full_round, "full resume must preserve its round and type");
    assert_eq!(
        count(
            &database,
            "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='incremental_resume_case'"
        )
        .await?,
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
    interrupt_after_checkpoint(
        command(work.path(), &config, &source, "incremental-resume-interrupted")?,
        &database,
        true,
    )
    .await?;
    assert_cumulative_observations(&database).await?;
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
