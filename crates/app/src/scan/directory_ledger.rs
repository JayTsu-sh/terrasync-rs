//! 目录贡献暂存于磁盘；内存只保留小批次，检查点先确认贡献再确认游标。
use super::directory_ledger_codec::{decode, encode};
use super::statistics::ScanCounts;
use crate::{
    consumer::ScanStatistics,
    error::{AppError, Result},
};
use data_mover::traversal::{DirectoryStatistics, ListingOutcome};
use db::scan_store::{DirectoryStatisticsStore, ScanDirectoryStatisticsRow};
use sqlx::{
    Connection, QueryBuilder, Row, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
};
use std::collections::BTreeMap;
use tempfile::{Builder, TempDir};
use tracing::info;

const BATCH_SIZE: usize = 256;

pub(crate) struct DirectoryLedger {
    connection: SqliteConnection,
    directory: TempDir,
    pending: Vec<DirectoryStatistics>,
    baseline: Option<(DirectoryStatisticsStore, i64, u64)>,
    cut: u64,
}

impl Drop for DirectoryLedger {
    fn drop(&mut self) {
        if let Ok(metadata) = self.directory.path().join("contributions.sqlite").metadata() {
            info!(
                statistics_cache_bytes = metadata.len(),
                "Temporary scan statistics cache released"
            );
        }
    }
}

#[cfg(test)]
#[path = "directory_ledger_tests.rs"]
mod tests;

impl DirectoryLedger {
    pub(crate) async fn new(baseline: Option<(DirectoryStatisticsStore, i64, u64)>, cut: u64) -> Result<Self> {
        let directory = Builder::new().prefix("terrasync-scan-statistics-").tempdir()?;
        // 仅为当前执行的可重建缓存；已确认状态由 ClickHouse 检查点持有。
        let options = SqliteConnectOptions::new()
            .filename(directory.path().join("contributions.sqlite"))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Off)
            .synchronous(SqliteSynchronous::Off)
            .pragma("cache_size", "-2048")
            .pragma("temp_store", "FILE");
        let mut connection = SqliteConnection::connect_with(&options).await?;
        sqlx::query("CREATE TABLE contributions (directory TEXT PRIMARY KEY, payload TEXT NOT NULL, frozen INTEGER NOT NULL, revision INTEGER NOT NULL, cut INTEGER NOT NULL) WITHOUT ROWID")
            .execute(&mut connection).await?;
        sqlx::query("CREATE INDEX contribution_cuts ON contributions(cut,directory)")
            .execute(&mut connection)
            .await?;
        Ok(Self {
            connection,
            directory,
            pending: Vec::with_capacity(BATCH_SIZE),
            baseline,
            cut,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) async fn push(&mut self, value: DirectoryStatistics, statistics: &ScanStatistics) -> Result<()> {
        self.pending.push(value);
        if self.pending.len() >= BATCH_SIZE {
            self.flush(statistics).await?;
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self, statistics: &ScanStatistics) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut query =
            QueryBuilder::new("SELECT directory,payload,frozen,revision FROM contributions WHERE directory IN (");
        let mut keys = query.separated(",");
        for value in &self.pending {
            keys.push_bind(value.path.as_str());
        }
        keys.push_unseparated(")");
        let existing = query.build().fetch_all(&mut self.connection).await?;
        let mut saved = BTreeMap::<String, (ScanCounts, bool, u64)>::new();
        for row in existing {
            saved.insert(
                row.get("directory"),
                (
                    decode(row.get::<&str, _>("payload"))?,
                    row.get::<i64, _>("frozen") != 0,
                    row.get::<i64, _>("revision") as u64,
                ),
            );
        }
        if let Some((database, round, cut)) = &self.baseline {
            let missing: Vec<_> = self
                .pending
                .iter()
                .filter(|value| !saved.contains_key(value.path.as_str()))
                .map(|value| value.path.as_str().to_owned())
                .collect();
            for row in database.load(*round, *cut, &missing).await? {
                saved.insert(row.directory, (decode(&row.payload)?, true, 0));
            }
        }
        let mut changes = Vec::with_capacity(self.pending.len());
        let mut replayed = 0u64;
        for value in &self.pending {
            let previous = saved.get(value.path.as_str());
            let old = previous.map_or_else(ScanCounts::default, |value| value.0.clone());
            let previous_revision = previous.map_or(0, |value| value.2);
            if value.pass_start && previous.is_some() {
                replayed = replayed.saturating_add(1);
            }
            let frozen = if value.pass_start {
                previous.is_some()
            } else {
                previous.is_some_and(|value| value.1)
            };
            if !value.pass_start && (previous_revision == 0 || value.revision <= previous_revision) {
                return Err(AppError::ScanError(
                    "Directory statistics revision is out of order".into(),
                ));
            }
            let complete = value.outcome == Some(ListingOutcome::Complete);
            let new = if !frozen || complete {
                ScanCounts::from_directory(value)
            } else {
                old.clone()
            };
            saved.insert(
                value.path.as_str().into(),
                (new.clone(), frozen && !complete, value.revision),
            );
            changes.push((old, new));
        }
        let mut query = QueryBuilder::new("INSERT INTO contributions(directory,payload,frozen,revision,cut) ");
        let rows: Vec<_> = saved
            .iter()
            .map(|(path, (counts, frozen, revision))| {
                Ok((
                    path,
                    encode(counts)?,
                    i64::from(*frozen),
                    i64::try_from(*revision)?,
                    i64::try_from(self.cut)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        query.push_values(&rows, |mut row, (path, payload, frozen, revision, cut)| {
            row.push_bind(path.as_str())
                .push_bind(payload)
                .push_bind(*frozen)
                .push_bind(*revision)
                .push_bind(*cut);
        });
        query.push(" ON CONFLICT(directory) DO UPDATE SET payload=excluded.payload, frozen=excluded.frozen, revision=excluded.revision, cut=excluded.cut");
        let mut transaction = self.connection.begin().await?;
        query.build().execute(&mut *transaction).await?;
        transaction.commit().await?;
        self.pending.clear();
        statistics.replace_directory_contributions(&changes, replayed).await;
        Ok(())
    }

    /// 已写入但未确认的 cut 不会覆盖旧 cut；调用方随后保存包含累计数的游标。
    pub(crate) async fn persist(
        &mut self, database: &DirectoryStatisticsStore, round: i64, version: u64,
    ) -> Result<()> {
        if version != self.cut {
            return Err(AppError::ScanError("Scan statistics/checkpoint version differs".into()));
        }
        let mut after: Option<String> = None;
        loop {
            let rows = sqlx::query("SELECT directory,payload FROM contributions WHERE cut=? AND (? IS NULL OR directory>?) ORDER BY directory LIMIT 4096")
                .bind(i64::try_from(version)?).bind(&after).bind(&after).fetch_all(&mut self.connection).await?;
            if rows.is_empty() {
                break;
            }
            let rows: Vec<_> = rows
                .into_iter()
                .map(|row| ScanDirectoryStatisticsRow {
                    directory: row.get("directory"),
                    payload: row.get("payload"),
                })
                .collect();
            database.write(round, version, &rows).await?;
            after = rows.last().map(|row| row.directory.clone());
        }
        self.cut = version
            .checked_add(1)
            .ok_or_else(|| AppError::ScanError("Scan statistics version overflow".into()))?;
        Ok(())
    }
}
