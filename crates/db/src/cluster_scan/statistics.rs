//! 整轮完成后统一校准，实时扫描不查询数据库去重。
use super::*;
use crate::clickhouse::scan_resume_statistics::ScanResumeSummary;

impl ClusterScanStore {
    pub(crate) async fn resume_statistics(
        &self, _generation: u8, event_time: Option<i64>,
    ) -> Result<ScanResumeStatistics> {
        let database = identifier(&self.database)?;
        let epoch = self.epoch.load(Ordering::Relaxed);
        let job = &self.job_id;
        let scope = format!("FROM {database}.base FINAL WHERE job_id='{job}' AND epoch={epoch} AND is_deleted=0");
        let regular = "NOT is_dir AND NOT is_symlink";
        let summary = self.client.query(&format!("SELECT countIf(is_dir) AS dirs, toInt64(sumIf(size,is_dir)) AS dir_bytes, countIf({regular}) AS files, toInt64(sumIf(size,{regular})) AS file_bytes, countIf(NOT is_dir AND is_symlink) AS links, toInt64(sumIf(size,NOT is_dir AND is_symlink)) AS link_bytes, toInt64(sumIf(countMatches(relative_path,'/')+1,is_dir)) AS total_depth, toUInt64(maxIf(countMatches(relative_path,'/')+1,is_dir)) AS max_depth, toInt64(sum(length(name))) AS total_name_length, toUInt64(max(length(name))) AS max_name_length {scope}"))
            .fetch_one::<ScanResumeSummary>().await?;
        let extensions = self.client.query(&format!("SELECT lower(ifNull(ext,'')), toInt64(sum(size)), count() {scope} AND {regular} AND ext IS NOT NULL GROUP BY lower(ifNull(ext,''))"))
            .fetch_all::<(String,i64,u64)>().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DatabaseError::ConversionError(e.to_string()))?
            .as_nanos();
        let ages = self.client.query(&format!("SELECT toUInt8(multiIf(toUnixTimestamp64Nano(mtime)>{now}-86400000000000,0,toUnixTimestamp64Nano(mtime)>{now}-604800000000000,1,toUnixTimestamp64Nano(mtime)>{now}-2592000000000000,2,toUnixTimestamp64Nano(mtime)>{now}-15552000000000000,3,toUnixTimestamp64Nano(mtime)>{now}-31536000000000000,4,5)) AS bucket, toInt64(sum(size)), count() {scope} GROUP BY bucket"))
            .fetch_all::<(u8,i64,u64)>().await?;
        let sizes = self.client.query(&format!("SELECT toUInt8(multiIf(size<4096,0,size<65536,1,size<131072,2,size<262144,3,size<524288,4,size<1048576,5,size<2097152,6,size<16777216,7,size<67108864,8,size<134217728,9,size<536870912,10,11)) AS bucket, toInt64(sum(size)), count() {scope} AND {regular} GROUP BY bucket"))
            .fetch_all::<(u8,i64,u64)>().await?;
        let events = if let Some(epoch) = event_time {
            let events_table = format!("{database}.incremental");
            self.client.query(&format!("SELECT operation_type,is_dir,is_symlink,count(),toInt64(sum(size)) FROM {events_table} FINAL WHERE job_id='{job}' AND epoch={epoch} AND toUnixTimestamp64Nano(create_at)>=? GROUP BY operation_type,is_dir,is_symlink"))
                .bind(epoch).fetch_all::<(String,bool,bool,u64,i64)>().await?
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
}
