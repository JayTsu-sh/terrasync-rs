//! 整轮完成后统一校准，实时扫描不查询数据库去重。
use super::*;
use crate::scan::{ScanEventStatistics, ScanEventSummary, ScanResumeSummary};

impl ClusterScanStore {
    pub(crate) async fn resume_statistics(
        &self, _generation: u8, event_time: Option<i64>,
    ) -> Result<ScanResumeStatistics> {
        let database = identifier(&self.database)?;
        let epoch = self.epoch.load(Ordering::Relaxed);
        let job = &self.job_id;
        let scope = format!("FROM {database}.base FINAL WHERE job_id='{job}' AND epoch={epoch} AND is_deleted=0");
        let regular = "is_regular_file";
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        // 在同一个去重结果上汇总分布，避免对五千万条 base 重复执行四次 FINAL。
        let totals = self.client.query(&format!("WITH toUInt8(multiIf(toUnixTimestamp64Nano(mtime)>{now}-86400000000000,0,toUnixTimestamp64Nano(mtime)>{now}-604800000000000,1,toUnixTimestamp64Nano(mtime)>{now}-2592000000000000,2,toUnixTimestamp64Nano(mtime)>{now}-15552000000000000,3,toUnixTimestamp64Nano(mtime)>{now}-31536000000000000,4,5)) AS age_bucket, toUInt8(multiIf(size<4096,0,size<65536,1,size<131072,2,size<262144,3,size<524288,4,size<1048576,5,size<2097152,6,size<16777216,7,size<67108864,8,size<134217728,9,size<536870912,10,11)) AS size_bucket SELECT countIf(is_dir) AS dirs, toInt64(sumIf(size,is_dir)) AS dir_bytes, countIf({regular}) AS files, toInt64(sumIf(size,{regular})) AS file_bytes, countIf(NOT is_dir AND is_symlink) AS links, toInt64(sumIf(size,NOT is_dir AND is_symlink)) AS link_bytes, toInt64(sumIf(countMatches(relative_path,'/')+1,is_dir)) AS total_depth, toUInt64(maxIf(countMatches(relative_path,'/')+1,is_dir)) AS max_depth, toInt64(sum(length(name))) AS total_name_length, toUInt64(max(length(name))) AS max_name_length, sumMapIf([lower(ifNull(ext,''))],[toInt64(size)],[toUInt64(1)],{regular} AND ext IS NOT NULL) AS extensions, tuple(CAST(range(6),'Array(UInt8)'),sumResample(0,6,1)(toInt64(size),age_bucket),sumResample(0,6,1)(toUInt64(1),age_bucket)) AS ages, tuple(CAST(range(12),'Array(UInt8)'),sumResample(0,12,1)(if({regular},toInt64(size),toInt64(0)),size_bucket),sumResample(0,12,1)(toUInt64({regular}),size_bucket)) AS sizes {scope}"))
            .fetch_quorum_one::<ScanAggregateRow>().await?;
        let events = if let Some(epoch) = event_time {
            self.resume_event_statistics(epoch).await?
        } else {
            Vec::new()
        };
        Ok(totals.statistics(events))
    }
    pub(crate) async fn resume_event_statistics(&self, epoch: i64) -> Result<ScanEventStatistics> {
        let database = identifier(&self.database)?;
        let job = &self.job_id;
        let events_table = format!("{database}.incremental");
        self.client.query(&format!("SELECT operation_type,is_dir,is_symlink,is_regular_file,count() AS entries,toInt64(sum(size)) AS bytes FROM {events_table} FINAL WHERE job_id='{job}' AND epoch={epoch} AND toUnixTimestamp64Nano(create_at)>=? GROUP BY operation_type,is_dir,is_symlink,is_regular_file"))
            .bind(epoch).fetch_quorum_all::<ScanEventSummary>().await
    }
}

type BucketTotals<K> = (Vec<K>, Vec<i64>, Vec<u64>);

#[derive(Deserialize, clickhouse::Row)]
struct ScanAggregateRow {
    dirs: u64,
    dir_bytes: i64,
    files: u64,
    file_bytes: i64,
    links: u64,
    link_bytes: i64,
    total_depth: i64,
    max_depth: u64,
    total_name_length: i64,
    max_name_length: u64,
    extensions: BucketTotals<String>,
    ages: BucketTotals<u8>,
    sizes: BucketTotals<u8>,
}

impl ScanAggregateRow {
    fn statistics(self, events: ScanEventStatistics) -> ScanResumeStatistics {
        ScanResumeStatistics {
            summary: ScanResumeSummary {
                dirs: self.dirs,
                dir_bytes: self.dir_bytes,
                files: self.files,
                file_bytes: self.file_bytes,
                links: self.links,
                link_bytes: self.link_bytes,
                total_depth: self.total_depth,
                max_depth: self.max_depth,
                total_name_length: self.total_name_length,
                max_name_length: self.max_name_length,
            },
            extensions: buckets(self.extensions),
            ages: buckets(self.ages),
            sizes: buckets(self.sizes),
            events,
        }
    }
}

fn buckets<K>((keys, bytes, counts): BucketTotals<K>) -> Vec<(K, i64, u64)> {
    keys.into_iter()
        .zip(bytes)
        .zip(counts)
        .filter(|(_, count)| *count > 0)
        .map(|((key, bytes), count)| (key, bytes, count))
        .collect()
}
