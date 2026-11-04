//! 同一数据库作业的执行互斥，扫描和复制共用注册与文件锁。
use crate::error::{AppError, Result};
use dashmap::{DashMap, mapref::entry::Entry};
use std::{
    fs::{File, OpenOptions, create_dir_all},
    path::Path,
    sync::OnceLock,
};
use utils::{app_config::ClickhouseConfig, sanitize_job_id};

static ACTIVE: OnceLock<DashMap<String, ()>> = OnceLock::new();

/// 同进程按数据库身份和作业互斥；跨进程通过同一 `job_dir` 的文件锁互斥。
pub(crate) struct JobLease {
    key: String,
    _file: File,
}

impl JobLease {
    pub(crate) fn acquire(connection: &ClickhouseConfig, job_id: &str, job_dir: &str) -> Result<Self> {
        let key = blake3::hash(&serde_json::to_vec(&(
            &connection.dsn,
            &connection.database,
            sanitize_job_id(job_id),
        ))?)
        .to_hex()
        .to_string();
        let active = ACTIVE.get_or_init(DashMap::new);
        match active.entry(key.clone()) {
            Entry::Occupied(_) => {
                return Err(AppError::ConfigError(
                    "同一数据库作业已有执行，不能并发扫描或复制".into(),
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(());
            }
        }
        let file = (|| {
            create_dir_all(job_dir)?;
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(Path::new(job_dir).join("job.lock"))?;
            file.try_lock().map_err(|source| AppError::JobExecutionLock {
                job_id: job_id.to_owned(),
                source,
            })?;
            Ok(file)
        })();
        match file {
            Ok(file) => Ok(Self { key, _file: file }),
            Err(error) => {
                active.remove(&key);
                Err(error)
            }
        }
    }
}

impl Drop for JobLease {
    fn drop(&mut self) {
        if let Some(active) = ACTIVE.get() {
            active.remove(&self.key);
        }
    }
}
