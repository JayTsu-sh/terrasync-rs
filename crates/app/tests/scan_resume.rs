//! Real CLI termination and restart against ClickHouse.
#![cfg(feature = "lab-fault-injection")]

use clickhouse::Client;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn client(database: &str) -> Client {
    let mut value = Client::default()
        .with_url(std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into()))
        .with_database(database)
        .with_user(std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()));
    if let Ok(password) = std::env::var("LAB_CLICKHOUSE_PASSWORD") {
        value = value.with_password(password);
    }
    value
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

async fn count(database: &Client, sql: &str) -> Result<u64> {
    Ok(database.query(sql).fetch_one::<u64>().await?)
}

async fn interrupt_after_checkpoint(mut scan: ScanCommand, database: &Client) -> Result {
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
        if let Ok((entries, phase, cursor)) = database
            .query("SELECT observed_entries, phase, notEmpty(checkpoint) FROM scan_resume_resume_case FINAL")
            .fetch_one::<(u64, u8, u8)>()
            .await
            && entries > 0
            && phase == 0
            && cursor == 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    process.0.kill()?;
    assert!(!process.0.wait()?.success());
    Ok(())
}

async fn complete(mut scan: ScanCommand) -> Result {
    let mut process = ChildGuard(scan.command.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = process.0.try_wait()? {
            assert!(status.success(), "scan failed: {}", fs::read_to_string(&scan.log)?);
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

async fn assert_paths(database: &Client, source: &Path) -> Result {
    let rows = database
        .query("SELECT relative_path FROM base_resume_case FINAL")
        .fetch_all::<String>()
        .await?;
    let actual: BTreeSet<_> = rows.iter().cloned().collect();
    assert_eq!(rows.len(), actual.len(), "duplicate base rows");
    assert_eq!(actual, paths(source)?);
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
    fs::write(
        &config,
        format!(
            "[database]\nenabled=true\ntype=\"clickhouse\"\nbatch_size=16\n[database.clickhouse]\ndsn={dsn}\ndatabase=\"{name}\"\nusername={user}\npassword={password}\n[scan]\nconcurrency=1\n"
        ),
    )?;
    interrupt_after_checkpoint(command(work.path(), &config, &source, "full-interrupted")?, &database).await?;
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
    complete(command(work.path(), &config, &source, "full-resumed")?).await?;
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
    )
    .await?;
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
    complete(command(work.path(), &config, &source, "incremental-resumed")?).await?;
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
    assert_eq!(
        count(&database, "SELECT count() FROM incremental_resume_case FINAL").await?,
        4
    );
    Ok(())
}
