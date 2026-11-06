//! 扫描完成时从幂等结果汇总统计，排除目录检查点重放造成的重复观察。
use super::*;
use crate::scan::model::{ScanEventStatistics, ScanEventSummary, ScanResumeStatistics, ScanResumeSummary};
use std::time::{SystemTime, UNIX_EPOCH};

impl ClickHouseDatabase {
    pub async fn resume_statistics(&self, generation: u8, event_time: Option<i64>) -> Result<ScanResumeStatistics> {
        let base = get_scan_base_table_name(&self.job_id);
        let scope = format!("FROM {base} FINAL WHERE current_state={generation}");
        let regular = "is_regular_file";
        let summary = self.sync_client.query(&format!("SELECT countIf(is_dir) AS dirs, toInt64(sumIf(size,is_dir)) AS dir_bytes, countIf({regular}) AS files, toInt64(sumIf(size,{regular})) AS file_bytes, countIf(NOT is_dir AND is_symlink) AS links, toInt64(sumIf(size,NOT is_dir AND is_symlink)) AS link_bytes, toInt64(sumIf(countMatches(relative_path,'/')+1,is_dir)) AS total_depth, toUInt64(maxIf(countMatches(relative_path,'/')+1,is_dir)) AS max_depth, toInt64(sum(length(name))) AS total_name_length, toUInt64(max(length(name))) AS max_name_length {scope}"))
            .fetch_quorum_one::<ScanResumeSummary>().await?;
        let extensions = self.sync_client.query(&format!("SELECT lower(ifNull(ext,'')), toInt64(sum(size)), count() {scope} AND {regular} AND ext IS NOT NULL GROUP BY lower(ifNull(ext,''))"))
            .fetch_quorum_all::<(String,i64,u64)>().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let ages = self.sync_client.query(&format!("SELECT toUInt8(multiIf(toUnixTimestamp64Nano(mtime)>{now}-86400000000000,0,toUnixTimestamp64Nano(mtime)>{now}-604800000000000,1,toUnixTimestamp64Nano(mtime)>{now}-2592000000000000,2,toUnixTimestamp64Nano(mtime)>{now}-15552000000000000,3,toUnixTimestamp64Nano(mtime)>{now}-31536000000000000,4,5)) AS bucket, toInt64(sum(size)), count() {scope} GROUP BY bucket"))
            .fetch_quorum_all::<(u8,i64,u64)>().await?;
        let sizes = self.sync_client.query(&format!("SELECT toUInt8(multiIf(size<4096,0,size<65536,1,size<131072,2,size<262144,3,size<524288,4,size<1048576,5,size<2097152,6,size<16777216,7,size<67108864,8,size<134217728,9,size<536870912,10,11)) AS bucket, toInt64(sum(size)), count() {scope} AND {regular} GROUP BY bucket"))
            .fetch_quorum_all::<(u8,i64,u64)>().await?;
        let events = if let Some(epoch) = event_time {
            self.resume_event_statistics(epoch).await?
        } else {
            Vec::new()
        };
        Ok(ScanResumeStatistics {
            summary,
            extensions,
            ages,
            sizes,
            events,
        })
    }
    pub async fn resume_event_statistics(&self, epoch: i64) -> Result<ScanEventStatistics> {
        let events_table = get_incremental_scan_base_table_name(&self.job_id);
        self.sync_client.query(&format!("SELECT operation_type,is_dir,is_symlink,is_regular_file,count() AS entries,toInt64(sum(size)) AS bytes FROM {events_table} FINAL WHERE toUnixTimestamp64Nano(create_at)>=? GROUP BY operation_type,is_dir,is_symlink,is_regular_file"))
            .bind(epoch).fetch_quorum_all::<ScanEventSummary>().await.map_err(Into::into)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::ClickHouseConfig;
    use crate::{
        execution::read::tests::server,
        scan::{ScanDatabaseExecution, with_scan_cancellation},
    };
    use clickhouse::Compression;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn final_summary_retries_a_transient_network_error_without_replaying_finalization() {
        // summary 的十个字段均为 64 位；其余三条分布查询返回空集合。
        let (url, server) = server(vec![
            (503, b"Code: 210. NETWORK_ERROR".to_vec()),
            (200, vec![0; 80]),
            (200, vec![]),
            (200, vec![]),
            (200, vec![]),
        ])
        .await;
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: url.clone(),
            database: "fixture".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let client = Client::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(Compression::None);
        let database = ClickHouseDatabase::with_client(&config, "fixture", client).unwrap();
        let execution = ScanDatabaseExecution::new(&config).unwrap();
        let summary = with_scan_cancellation(
            CancellationToken::new(),
            Some(execution.clone()),
            database.resume_statistics(0, None),
        )
        .await
        .unwrap();
        assert_eq!(summary.summary.files, 0);
        assert!(summary.events.is_empty());
        assert!(execution.failure().is_none());
        assert_eq!(server.await.unwrap().len(), 5);
    }
}
