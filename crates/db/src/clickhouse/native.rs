use super::*;
use crate::native_scan::NativeScanAccess;
use crate::native_scan_record::incremental_record;
use crate::native_scan_record::{NativeScanRecord, OBSERVED_COLUMNS, event_records};
use crate::scan_store::ScanDeletion;
use storage_adapter::observed_scan::{ObservedScanEvent, ObservedView};

impl ClickHouseDatabase {
    async fn native_access(&self) -> Result<NativeScanAccess<'_>> {
        Ok(NativeScanAccess {
            client: &self.sync_client,
            base: get_scan_base_table_name(&self.job_id),
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
        if entries.is_empty() {
            return Ok(());
        }
        let access = self.native_access().await?;
        let table = if temporary { &access.stage } else { &access.base };
        let generation = self.get_cached_scan_state().await?;
        let client = self.sync_client.clone().with_validation(false);
        let mut insert = client.insert::<NativeScanRecord>(table).await?;
        for entry in entries {
            for view in ObservedView::all(entry) {
                insert.write(&NativeScanRecord::capture(view, generation)?).await?;
            }
        }
        insert.end().await?;
        Ok(())
    }
    pub(crate) async fn compare_native(&mut self, entries: &[Arc<ObservedEntry>]) -> Result<Vec<ObservedScanEvent>> {
        if self.resume_base_has_typed_handles.is_none() {
            let base = get_scan_base_table_name(&self.job_id);
            self.resume_base_has_typed_handles=Some(self.sync_client.query(&format!("SELECT count(file_handle)>0 AND countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle,'54534934'))=0 FROM {base}")).fetch_one().await?);
        }
        self.native_access().await?.compare(entries).await
    }
    pub(crate) async fn delete_native(&self) -> Result<Vec<ScanDeletion>> {
        self.native_access().await?.deleted().await
    }
    pub(crate) async fn merge_native(&self) -> Result<()> {
        let access = self.native_access().await?;
        self.sync_client
            .query(&format!(
                "INSERT INTO {} ({OBSERVED_COLUMNS}) SELECT {OBSERVED_COLUMNS} FROM {}",
                access.base, access.stage
            ))
            .execute()
            .await?;
        Ok(())
    }
    pub(crate) async fn events_native(&self, events: &[ObservedScanEvent], time: i64) -> Result<()> {
        let records = event_records(events, time)?;
        self.write_native_events(records, time).await
    }
    pub(crate) async fn deletions_native(&self, events: &[ScanDeletion], time: i64) -> Result<()> {
        let records = events
            .iter()
            .map(|event| {
                incremental_record(
                    event.item().clone(),
                    event.operation(),
                    event.to.as_ref().map(|_| event.from.relative_path.clone()),
                    time,
                )
            })
            .collect();
        self.write_native_events(records, time).await
    }
    async fn write_native_events(&self, mut records: Vec<IncrementalStorageEntryRecord>, time: i64) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let table = get_incremental_scan_base_table_name(&self.job_id);
        let paths: HashSet<_> = records.iter().map(|record| record.relative_path.clone()).collect();
        let keys = self.write_resume_keys(paths).await?;
        let existing=self.sync_client.query(&format!("SELECT operation_type,relative_path,version_id,create_at FROM {table} FINAL WHERE toUnixTimestamp64Nano(create_at)>=? AND relative_path IN (SELECT relative_path FROM {keys})")).bind(time).fetch_all::<(String,String,String,i64)>().await?;
        let mut next = existing.iter().map(|row| row.3).max().unwrap_or(time);
        let mut times: HashMap<_, _> = existing
            .into_iter()
            .map(|(op, path, version, time)| ((op, path, version), time))
            .collect();
        let mut insert = self.sync_client.insert::<IncrementalStorageEntryRecord>(&table).await?;
        for record in &mut records {
            let key = (
                record.operation_type.clone(),
                record.relative_path.clone(),
                record.version_id.clone(),
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
            insert.write(record).await?;
        }
        insert.end().await?;
        Ok(())
    }
}
