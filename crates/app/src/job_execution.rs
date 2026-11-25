//! 扫描和复制按相同的 ClickHouse DSN、数据库名与作业 ID 互斥。
use crate::error::{AppError, Result};
#[cfg(not(target_os = "linux"))]
use std::env::home_dir;
#[cfg(not(target_os = "linux"))]
use std::io::Error;
use std::io::ErrorKind;
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
#[cfg(target_os = "linux")]
use std::os::unix::net::{SocketAddr, UnixListener};
use std::{
    fs::{File, OpenOptions, TryLockError, create_dir_all},
    path::Path,
};
use utils::{app_config::ClickhouseConfig, sanitize_job_id};

/// 数据库作业锁与工作目录无关；目录锁另保护同一目录中的本地恢复文件。
pub(crate) struct JobLease {
    _file: File,
    _identity: IdentityLock,
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
        let lock_error = |source: TryLockError| match source {
            TryLockError::WouldBlock => AppError::JobExecutionBusy {
                job_id: job_id.to_owned(),
            },
            source => AppError::JobExecutionLock {
                job_id: job_id.to_owned(),
                source,
            },
        };
        let identity = lock_job_identity(&key).map_err(lock_error)?;
        create_dir_all(job_dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(Path::new(job_dir).join("job.lock"))?;
        file.try_lock().map_err(lock_error)?;
        Ok(Self {
            _file: file,
            _identity: identity,
        })
    }
}

#[cfg(target_os = "linux")]
type IdentityLock = UnixListener;

#[cfg(target_os = "linux")]
fn lock_job_identity(key: &str) -> std::result::Result<IdentityLock, TryLockError> {
    // 抽象 Unix 地址在同一网络命名空间内唯一，最后一个 FD 关闭时由内核释放。
    let address = SocketAddr::from_abstract_name(format!("terrasync-job-{key}")).map_err(TryLockError::Error)?;
    UnixListener::bind_addr(&address).map_err(|error| match error.kind() {
        ErrorKind::AddrInUse => TryLockError::WouldBlock,
        _ => TryLockError::Error(error),
    })
}

#[cfg(not(target_os = "linux"))]
type IdentityLock = File;

#[cfg(not(target_os = "linux"))]
fn lock_job_identity(key: &str) -> std::result::Result<IdentityLock, TryLockError> {
    // 用户目录固定锁文件位置，保留文件以免删除后两个进程锁住不同对象。
    let home = home_dir().filter(|path| path.is_absolute()).ok_or_else(|| {
        TryLockError::Error(Error::new(
            ErrorKind::NotFound,
            "Cannot resolve home directory for the TerraSync job lock",
        ))
    })?;
    let directory = home.join(".terrasync").join("locks");
    create_dir_all(&directory).map_err(TryLockError::Error)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join(key))
        .map_err(TryLockError::Error)?;
    lock.try_lock()?;
    Ok(lock)
}
