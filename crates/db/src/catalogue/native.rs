use std::sync::Arc;

use data_mover::model::ObservedEntry;
use rustc_hash::{FxHashMap, FxHashSet};
use smol_str::SmolStr;
use task_model::observation::{ObservedScanEvent, ObservedView};

use super::ClickHouseDatabase;
use crate::execution::queries::{ScanExecute, check_scan_cancellation, execute_request, idempotent_insert_client};
use crate::execution::read::QuorumRead;
use crate::execution::recovery::RequestSafety;
use crate::scan::ScanFinalEvent;
use crate::scan::candidate_cache::CandidateMetadataCache;
use crate::scan::compare::NativeScanAccess;
use crate::scan::rows::{NativeScanKey, NativeScanRecord, NativeWriteBatches, OBSERVED_COLUMNS, event_records};
use crate::scan::rows::{final_event_records, incremental_record};
use crate::schema::identity::PERSISTED_IDENTITY_PREFIX;
use crate::schema::records::IncrementalStorageEntryRecord;
use crate::{DatabaseError, Result, get_incremental_scan_base_table_name, get_scan_base_table_name};

impl ClickHouseDatabase {
    pub(crate) async fn write_native_keys(&self, entries: &[Arc<ObservedEntry>]) -> Result<()> {
        check_scan_cancellation()?;
        if entries.is_empty() {
            return Ok(());
        }
        let access = self.native_access().await?;
        let client = idempotent_insert_client(&self.sync_client).with_validation(false);
        execute_request(RequestSafety::IdempotentWrite, || async {
            let mut insert = client.insert::<NativeScanKey>(&access.stage).await?;
            for entry in entries {
                for view in ObservedView::all(entry) {
                    insert.write(&NativeScanKey::capture(view)).await?;
                }
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }

    async fn native_access(&self) -> Result<NativeScanAccess<'_>> {
        Ok(NativeScanAccess {
            client: &self.sync_client,
            base: get_scan_base_table_name(&self.job_id),
            events: get_incremental_scan_base_table_name(&self.job_id),
            stage: self.scan_temp_table_name.clone().unwrap_or_default(),
            scope: "1".into(),
            stage_scope: "1".into(),
            active: format!("current_state={}", self.get_cached_scan_state().await?),
            live: "1",
            final_clause: "FINAL",
            typed_handles: self.resume_base_has_typed_handles.unwrap_or(false),
        })
    }
    pub(crate) async fn write_native(&self, entries: &[Arc<ObservedEntry>], temporary: bool) -> Result<()> {
        self.write_native_candidates(entries, temporary, &[]).await
    }

    pub(crate) async fn write_native_candidates(
        &self, entries: &[Arc<ObservedEntry>], temporary: bool, candidates: &[ObservedScanEvent],
    ) -> Result<()> {
        check_scan_cancellation()?;
        if entries.is_empty() {
            return Ok(());
        }
        let access = self.native_access().await?;
        let table = if temporary { &access.stage } else { &access.base };
        let generation = self.get_cached_scan_state().await?;
        let cache = CandidateMetadataCache::new(candidates).await?;
        for rows in NativeWriteBatches::new(entries, generation) {
            check_scan_cancellation()?;
            let mut rows = rows?;
            for row in &mut rows {
                cache.apply(row);
            }
            let client = idempotent_insert_client(&self.sync_client).with_validation(false);
            execute_request(RequestSafety::IdempotentWrite, || async {
                let mut insert = client.insert::<NativeScanRecord>(table).await?;
                for row in &rows {
                    insert.write(row).await?;
                }
                insert.end().await?;
                Ok(())
            })
            .await?;
        }
        Ok(())
    }
    pub(crate) async fn compare_native(&mut self, entries: &[Arc<ObservedEntry>]) -> Result<Vec<ObservedScanEvent>> {
        if self.resume_base_has_typed_handles.is_none() {
            let base = get_scan_base_table_name(&self.job_id);
            self.resume_base_has_typed_handles=Some(self.sync_client.query(&format!("SELECT count(file_handle)>0 AND countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle,'{PERSISTED_IDENTITY_PREFIX}'))=0 FROM {base}")).fetch_quorum_one().await?);
        }
        let compared = self.native_access().await?.compare(entries).await?;
        if !compared.displaced.is_empty() {
            let time = self
                .load_resume_state()
                .await?
                .ok_or_else(|| DatabaseError::TransactionError("Missing scan round for displaced source facts".into()))?
                .event_time;
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
        self.native_access().await?.final_events(time).await
    }
    pub(crate) async fn cleanup_candidates(&self, time: i64) -> Result<()> {
        self.sync_client
            .clone()
            .with_option("mutations_sync", "2")
            .query(&format!(
                "ALTER TABLE {} DELETE WHERE toUnixTimestamp64Nano(create_at)>=? AND operation_type IN ('candidate_new','displaced_old')",
                get_incremental_scan_base_table_name(&self.job_id)
            ))
            .bind(time)
            .execute_scan()
            .await?;
        Ok(())
    }
    pub(crate) async fn merge_native(&self) -> Result<()> {
        let access = self.native_access().await?;
        self.sync_client
            .query(&format!(
                "INSERT INTO {} ({OBSERVED_COLUMNS}) SELECT {OBSERVED_COLUMNS} FROM {}",
                access.base, access.stage
            ))
            .execute_scan()
            .await?;
        Ok(())
    }
    pub(crate) async fn events_native(&self, events: &[ObservedScanEvent], time: i64) -> Result<()> {
        let records = event_records(events, time)?;
        self.write_native_events(records, time).await
    }
    pub(crate) async fn write_final_events_native(&self, events: &[ScanFinalEvent], time: i64) -> Result<()> {
        let records = final_event_records(events, time);
        self.write_native_events(records, time).await
    }
    async fn write_native_events(&self, mut records: Vec<IncrementalStorageEntryRecord>, time: i64) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let table = get_incremental_scan_base_table_name(&self.job_id);
        let paths: FxHashSet<SmolStr> = records
            .iter()
            .map(|record| SmolStr::new(&record.relative_path))
            .collect();
        let keys = self.write_resume_keys(paths).await?;
        let existing=self.sync_client.query(&format!("SELECT operation_type,relative_path,version_id,create_at FROM {table} FINAL WHERE toUnixTimestamp64Nano(create_at)>=? AND relative_path IN (SELECT relative_path FROM {keys})")).bind(time).fetch_quorum_all::<(String,String,String,i64)>().await?;
        let mut next = existing.iter().map(|row| row.3).max().unwrap_or(time);
        let mut times: FxHashMap<_, _> = existing
            .into_iter()
            .map(|(op, path, version, time)| ((SmolStr::new(op), SmolStr::new(path), SmolStr::new(version)), time))
            .collect();
        for record in &mut records {
            let key = (
                SmolStr::new(&record.operation_type),
                SmolStr::new(&record.relative_path),
                SmolStr::new(&record.version_id),
            );
            record.create_at = if let Some(time) = times.get(&key) {
                *time
            } else {
                next = next
                    .checked_add(1)
                    .ok_or_else(|| DatabaseError::ConversionError("Scan event timestamp overflow".into()))?;
                times.insert(key, next);
                next
            };
        }
        let client = idempotent_insert_client(&self.sync_client);
        execute_request(RequestSafety::IdempotentWrite, || async {
            let mut insert = client.insert::<IncrementalStorageEntryRecord>(&table).await?;
            for record in &records {
                insert.write(record).await?;
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }
}
