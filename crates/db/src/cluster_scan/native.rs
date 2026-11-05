use super::record::ClusterNativeScanRecord;
use super::*;
use crate::native_scan::NativeScanAccess;
use crate::native_scan_record::{NativeScanKey, NativeScanRecord, OBSERVED_COLUMNS, event_records};
use crate::native_scan_record::{final_event_records, incremental_record};
use crate::scan_metadata::{CandidateMetadataCache, missing_hash_columns};
use crate::scan_store::ScanFinalEvent;
use crate::traits::IncrementalStorageEntryRecord;
use data_mover::model::ObservedEntry;
use std::borrow::Cow;
use storage_adapter::observed_scan::{ObservedScanEvent, ObservedView};

#[derive(Serialize, clickhouse::Row)]
struct ClusterNativeScanKey<'a> {
    job_id: &'a str,
    batch_id: u64,
    relative_path: Cow<'a, str>,
    version_id: &'a str,
    file_handle: Option<String>,
}

impl ClusterScanStore {
    pub(crate) async fn prepare_native_schema(&self) -> Result<()> {
        let database = identifier(&self.database)?;
        let cluster = identifier(&self.cluster)?;
        let tables: Vec<_> = ["base", "scan_resume_batch"]
            .into_iter()
            .flat_map(|name| [name.to_owned(), format!("{name}_local")])
            .collect();
        for table in missing_hash_columns(&self.client, &tables).await? {
            self.client.query(&format!("ALTER TABLE {database}.{table} ON CLUSTER {cluster} ADD COLUMN IF NOT EXISTS metadata_hash Nullable(String) DEFAULT NULL"))
                .execute_idempotent_scan().await?;
        }
        Ok(())
    }
    pub(crate) async fn write_native_keys(&self, entries: &[Arc<ObservedEntry>]) -> Result<()> {
        let _guard = self.writes.temporary.lock().await;
        let client = idempotent_insert_client(&self.client).with_validation(false);
        retry_quorum(RequestSafety::IdempotentWrite, || async {
            if entries.is_empty() {
                return Ok(());
            }
            let mut insert = client.insert::<ClusterNativeScanKey>("scan_resume_batch").await?;
            for entry in entries {
                for view in ObservedView::all(entry) {
                    let key = NativeScanKey::capture(view);
                    insert
                        .write(&ClusterNativeScanKey {
                            job_id: &self.job_id,
                            batch_id: self.batch_id,
                            relative_path: key.relative_path,
                            version_id: key.version_id,
                            file_handle: key.file_handle,
                        })
                        .await?;
                }
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }

    fn native_access(&self) -> Result<NativeScanAccess<'_>> {
        let database = identifier(&self.database)?;
        Ok(NativeScanAccess {
            client: &self.client,
            base: format!("{database}.base"),
            stage: format!("{database}.scan_resume_batch"),
            events: format!("{database}.incremental"),
            scope: format!("job_id='{}'", self.job_id),
            stage_scope: format!("job_id='{}' AND batch_id={}", self.job_id, self.batch_id),
            active: format!("epoch={}", self.epoch.load(Ordering::Relaxed)),
            live: "is_deleted=0",
            final_clause: "FINAL",
            typed_handles: true,
        })
    }
    pub(crate) async fn write_native(&self, entries: &[Arc<ObservedEntry>], temporary: bool) -> Result<()> {
        self.write_native_candidates(entries, temporary, &[]).await
    }

    pub(crate) async fn write_native_candidates(
        &self, entries: &[Arc<ObservedEntry>], temporary: bool, candidates: &[ObservedScanEvent],
    ) -> Result<()> {
        let cache = CandidateMetadataCache::new(candidates).await?;
        let gate = if temporary {
            &self.writes.temporary
        } else {
            &self.writes.base
        };
        let _guard = gate.lock().await;
        let client = idempotent_insert_client(&self.client).with_validation(false);
        retry_quorum(RequestSafety::IdempotentWrite, || async {
            if entries.is_empty() {
                return Ok(());
            }
            let table = if temporary { "scan_resume_batch" } else { "base" };
            let epoch = self.epoch.load(Ordering::Relaxed);
            // 仅原生固定布局批次使用 bulk RowBinary；查询和增量事件仍校验 schema。
            let mut insert = client.insert::<ClusterNativeScanRecord>(table).await?;
            for entry in entries {
                for view in ObservedView::all(entry) {
                    let mut record = NativeScanRecord::capture(view, self.generation.load(Ordering::Relaxed))?;
                    cache.apply(&mut record);
                    insert
                        .write(&ClusterNativeScanRecord::new(
                            self.job_id.clone(),
                            epoch,
                            if temporary { self.batch_id } else { 0 },
                            record,
                        ))
                        .await?;
                }
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }
    pub(crate) async fn compare_native(&mut self, entries: &[Arc<ObservedEntry>]) -> Result<Vec<ObservedScanEvent>> {
        let compared = self.native_access()?.compare(entries).await?;
        if !compared.displaced.is_empty() {
            let time = self.epoch.load(Ordering::Relaxed) as i64;
            let records = compared
                .displaced
                .into_iter()
                .map(|row| incremental_record(row, "displaced_old", None, time))
                .collect();
            self.write_native_events(records, time).await?;
        }
        Ok(compared.events)
    }
    pub(crate) async fn resolve_final_events_native(&self, time: i64) -> Result<Vec<ScanFinalEvent>> {
        self.native_access()?.final_events(time).await
    }
    pub(crate) async fn cleanup_candidates(&self, time: i64) -> Result<()> {
        let database = identifier(&self.database)?;
        let cluster = identifier(&self.cluster)?;
        self.client.clone().with_option("mutations_sync","2").query(&format!("ALTER TABLE {database}.incremental_local ON CLUSTER {cluster} DELETE WHERE job_id=? AND epoch=? AND operation_type IN ('candidate_new','displaced_old')")).bind(&self.job_id).bind(time as u64).execute_scan().await?;
        Ok(())
    }
    pub(crate) async fn merge_native(&self) -> Result<()> {
        let _guard = self.writes.base.lock().await;
        retry_quorum(RequestSafety::UnconfirmedWrite, ||async {
            let database=identifier(&self.database)?;
            self.client.query(&format!("INSERT INTO {database}.base (job_id,epoch,ver,{OBSERVED_COLUMNS}) SELECT job_id,epoch,ver,{OBSERVED_COLUMNS} FROM {database}.scan_resume_batch WHERE job_id=? AND batch_id=?")).bind(&self.job_id).bind(self.batch_id).execute_scan().await?;
            Ok(())
        }).await
    }
    pub(crate) async fn events_native(&self, events: &[ObservedScanEvent], time: i64) -> Result<()> {
        let records = event_records(events, time)?;
        self.write_native_events(records, time).await
    }
    pub(crate) async fn write_final_events_native(&self, events: &[ScanFinalEvent], time: i64) -> Result<()> {
        let records = final_event_records(events, time);
        self.write_native_events(records, time).await
    }
    async fn write_native_events(&self, records: Vec<IncrementalStorageEntryRecord>, time: i64) -> Result<()> {
        check_scan_cancellation()?;
        if records.is_empty() {
            return Ok(());
        }
        let _guard = self.writes.events.lock().await;
        let client = idempotent_insert_client(&self.client);
        retry_quorum(RequestSafety::IdempotentWrite, || async {
            let mut insert = client
                .insert::<ClusterIncrementalStorageEntryRecord>("incremental")
                .await?;
            for record in &records {
                insert
                    .write(&ClusterIncrementalStorageEntryRecord::new(
                        &self.job_id,
                        time as u64,
                        record,
                    ))
                    .await?;
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }
}
