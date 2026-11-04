#![allow(clippy::unwrap_used)]

use super::*;
use crate::ClickHouseConfig;
use crate::clickhouse::replica_read::tests::server;
use crate::scan_store::{ScanStore, with_scan_cancellation};
use clickhouse::test::{Mock, handlers};
use tokio_util::sync::CancellationToken;

fn client(mock: &Mock) -> Client {
    Client::default().with_url(mock.url()).with_validation(false)
}

fn row(directory: &str, payload: &str) -> ScanDirectoryStatisticsRow {
    ScanDirectoryStatisticsRow {
        directory: directory.into(),
        payload: payload.into(),
    }
}

#[tokio::test]
async fn history_is_filtered_before_argmax_without_replacing_or_final() -> Result<()> {
    let mock = Mock::new();
    let create = mock.add(handlers::record_ddl());
    let read = mock.add(handlers::record_ddl());
    let ledger = DirectoryStatisticsStore::single(client(&mock), "first");
    let rows = ledger.load(17, 5, &["a'quoted\\directory".into()]).await?;
    assert!(rows.is_empty());
    let ddl = create.query().await;
    assert!(ddl.contains("ENGINE = MergeTree()"));
    assert!(ddl.contains("ORDER BY (job_id,round,directory,version,revision)"));
    assert!(!ddl.contains("Replacing"));
    let query = read.query().await;
    assert!(query.contains("argMax(payload,(version,revision))"));
    assert!(query.contains("job_id='first' AND round=17 AND version<=5"));
    assert!(query.contains("directory IN {paths:Array(String)}"));
    assert!(!query.contains("FINAL"));
    Ok(())
}

#[tokio::test]
async fn cluster_ledger_scopes_reads_and_synchronous_mutations_to_job_and_round() -> Result<()> {
    let mock = Mock::new();
    let local = mock.add(handlers::record_ddl());
    let public = mock.add(handlers::record_ddl());
    let read = mock.add(handlers::record_ddl());
    let truncate = mock.add(handlers::record_ddl());
    mock.add(handlers::provide([1_u8]));
    let clear = mock.add(handlers::record_ddl());
    let ledger = DirectoryStatisticsStore::cluster(client(&mock), "fixture", "cluster", "first")?;
    ledger.load(17, 5, &["root".into()]).await?;
    ledger.truncate(17, 5).await?;
    ledger.clear_job().await?;
    let ddl = local.query().await;
    assert!(ddl.contains("scan_directory_statistics_local ON CLUSTER `cluster`"));
    assert!(ddl.contains("ENGINE = ReplicatedMergeTree("));
    assert!(ddl.contains("PARTITION BY job_id"));
    assert!(public.query().await.contains("Distributed(`cluster`, `fixture`"));
    assert!(
        read.query()
            .await
            .contains("job_id='first' AND round=17 AND version<=5")
    );
    assert!(truncate.query().await.contains(
        "ALTER TABLE `fixture`.scan_directory_statistics_local ON CLUSTER `cluster` DELETE WHERE job_id='first' AND round=17 AND version>5"
    ));
    assert!(
        clear
            .query()
            .await
            .contains("ON CLUSTER `cluster` DROP PARTITION 'first'")
    );
    Ok(())
}

#[tokio::test]
async fn cluster_truncation_waits_for_all_replicas_and_preserves_query_cancel_tag() -> Result<()> {
    let (url, requests) = server(vec![(200, vec![]), (200, vec![]), (200, vec![])]).await;
    let client = Client::default()
        .with_url(url)
        .with_option("log_comment", "owned-execution");
    let ledger = DirectoryStatisticsStore::cluster(client, "fixture", "cluster", "job")?;
    ledger.truncate(17, 5).await?;
    let requests = requests.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request.contains("log_comment=owned-execution"))
    );
    assert!(requests[2].contains("mutations_sync=2"));
    Ok(())
}

#[tokio::test]
async fn duplicate_directory_batch_is_rejected_before_any_rpc() {
    let mock = Mock::new();
    let ledger = DirectoryStatisticsStore::single(client(&mock), "fixture");
    let result = ledger.write(1, 2, &[row("same", "first"), row("same", "second")]).await;
    assert!(matches!(result, Err(DatabaseError::ConversionError(_))));
}

#[tokio::test]
async fn empty_batches_do_not_create_or_query_tables() -> Result<()> {
    let mock = Mock::new();
    let ledger = DirectoryStatisticsStore::single(client(&mock), "fixture");
    ledger.write(1, 2, &[]).await?;
    assert!(ledger.load(1, 2, &[]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_native_reset_does_not_delete_the_directory_ledger_first() -> Result<()> {
    let mock = Mock::new();
    let first_drop = mock.add(handlers::record_ddl());
    mock.add(handlers::exception(16));
    let config = ClickHouseConfig {
        mode: Default::default(),
        cluster: None,
        dsn: mock.url().into(),
        database: "fixture".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    let store = ScanStore::new(&config, "reset")?;
    assert!(matches!(
        store.clear_job().await,
        Err(DatabaseError::ClickHouseError(_))
    ));
    assert_eq!(first_drop.query().await, "DROP TABLE IF EXISTS `base_reset`");
    Ok(())
}

#[tokio::test]
async fn cancellation_during_validation_is_not_a_partial_success() {
    let mock = Mock::new();
    let ledger = DirectoryStatisticsStore::single(client(&mock), "fixture");
    let rows = (0..16_384)
        .map(|index| row(&index.to_string(), "{}"))
        .collect::<Vec<_>>();
    let cancel = CancellationToken::new();
    let (result, ()) = tokio::join!(
        with_scan_cancellation(cancel.clone(), ledger.write(1, 2, &rows)),
        async {
            cancel.cancel();
        },
    );
    assert!(matches!(result, Err(DatabaseError::ScanCancelled)));
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct WrittenRow {
    job_id: String,
    round: i64,
    version: u64,
    revision: u64,
    directory: String,
    payload: String,
}

#[tokio::test]
async fn each_logical_write_has_one_revision_and_later_write_orders_after_it() -> Result<()> {
    let mock = Mock::new();
    mock.add(handlers::record_ddl());
    let first = mock.add(handlers::record::<WrittenRow>());
    let second = mock.add(handlers::record::<WrittenRow>());
    let ledger = DirectoryStatisticsStore::single(client(&mock), "fixture");
    ledger.write(1, 2, &[row("a", "first"), row("b", "first")]).await?;
    ledger.write(1, 2, &[row("a", "second")]).await?;
    let first: Vec<_> = first.collect().await;
    let second: Vec<_> = second.collect().await;
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].revision, first[1].revision);
    assert!(first[0].revision < second[0].revision);
    assert_eq!(
        (&first[0].job_id, first[0].round, first[0].version),
        (&"fixture".into(), 1, 2)
    );
    assert_eq!((&first[0].directory, &first[0].payload), (&"a".into(), &"first".into()));
    assert_eq!(second[0].payload, "second");
    Ok(())
}
