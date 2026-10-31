use crate::{
    error::operation,
    options::BackendConfig as ProductBackendConfig,
    projection::{decode_acl, encode_acl},
    streaming::source_qos,
    *,
};
use data_mover::storage::{
    BackendConfig as NativeBackendConfig, CifsBackendConfig, CopiedAclTarget, HdfsBackendConfig, LocalBackendConfig,
    NfsBackendConfig, S3BackendConfig, StorageRoleFailure,
};
use data_mover::{
    TransferConcurrency,
    metadata::{
        AclTarget, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget, OwnershipTarget,
        TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
    },
    model::*,
    probes::{check_connectivity, probe_server_time},
    storage::{self, CopiedOwnershipTarget, CopiedTimestampTarget, MetadataMutation, Storage},
    transfer::{self, ReadBackVerification, TransferPolicy, TransferRequest},
};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};
use tokio::fs::create_dir_all;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageType {
    Local,
    Nfs,
    Cifs,
    S3,
    Hdfs,
}
pub fn detect_storage_type(path: &str) -> StorageType {
    if path.starts_with("nfs://") || path.starts_with("nfs4://") {
        StorageType::Nfs
    } else if path.starts_with("s3") && path.contains("://") {
        StorageType::S3
    } else if path.starts_with("smb://") {
        StorageType::Cifs
    } else if path.starts_with("hdfs://") {
        StorageType::Hdfs
    } else {
        StorageType::Local
    }
}
#[derive(Clone)]
pub struct ConnectedStorage {
    pub(crate) roles: Storage,
    pub(crate) block_size: u64,
}
/// 变体仅服务产品层策略判断；各操作统一借用连接的角色，无后端复制实现。
#[derive(Clone)]
pub enum StorageEnum {
    Local(ConnectedStorage),
    NFS(ConnectedStorage),
    CIFS(ConnectedStorage),
    S3(ConnectedStorage),
    HDFS(ConnectedStorage),
}
impl std::ops::Deref for StorageEnum {
    type Target = ConnectedStorage;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Local(s) | Self::NFS(s) | Self::CIFS(s) | Self::S3(s) | Self::HDFS(s) => s,
        }
    }
}
pub async fn create_storage(path: &str, options: CreateStorageOptions) -> Result<StorageEnum> {
    let kind = detect_storage_type(path);
    let block_size = options.block_size.unwrap_or(4 * 1024 * 1024);
    if block_size == 0 {
        return Err(StorageError::ConfigError("block size must be positive".into()));
    }
    let config = match kind {
        StorageType::Local => {
            let root = PathBuf::from(path);
            if options.ensure_dir {
                create_dir_all(&root).await?;
            }
            let concurrency = TransferConcurrency::from_env(BackendKind::Local, TransferConcurrency::defaults(4, 2))
                .map_err(StorageError::from)?;
            NativeBackendConfig::Local(LocalBackendConfig {
                root,
                rename_context: None,
                read_concurrency: NonZeroUsize::new(concurrency.read())
                    .ok_or_else(|| operation("zero read concurrency"))?,
                write_concurrency: NonZeroUsize::new(concurrency.write())
                    .ok_or_else(|| operation("zero write concurrency"))?,
                read_chunk_bytes: options.block_size.and_then(|v| NonZeroUsize::new(v as usize)),
                write_chunk_bytes: options.block_size.and_then(|v| NonZeroUsize::new(v as usize)),
            })
        }
        StorageType::Nfs => NativeBackendConfig::Nfs(NfsBackendConfig {
            url: path.into(),
            block_size: options.block_size,
            ensure_dir: options.ensure_dir,
            transfer_concurrency: None,
        }),
        StorageType::S3 => NativeBackendConfig::S3(S3BackendConfig {
            url: path.into(),
            block_size: options.block_size,
            single_put_threshold: None,
        }),
        StorageType::Cifs => {
            let mut config = CifsBackendConfig::from_url(path).map_err(StorageError::from)?;
            config.ensure_dir = options.ensure_dir;
            NativeBackendConfig::Cifs(config)
        }
        StorageType::Hdfs => {
            let ProductBackendConfig::Hdfs(client) = options.backend else {
                return Err(StorageError::ConfigError(
                    "HDFS requires explicit client configuration".into(),
                ));
            };
            NativeBackendConfig::Hdfs(HdfsBackendConfig {
                location: path.into(),
                client,
                block_size: options.block_size,
                ensure_dir: options.ensure_dir,
            })
        }
    };
    let connected = ConnectedStorage {
        roles: storage::connect_backend(config).await.map_err(StorageError::from)?,
        block_size,
    };
    Ok(match kind {
        StorageType::Local => StorageEnum::Local(connected),
        StorageType::Nfs => StorageEnum::NFS(connected),
        StorageType::Cifs => StorageEnum::CIFS(connected),
        StorageType::S3 => StorageEnum::S3(connected),
        StorageType::Hdfs => StorageEnum::HDFS(connected),
    })
}
pub(crate) fn path(value: &Path) -> Result<StoragePath> {
    if value.as_os_str().is_empty() {
        Ok(StoragePath::root())
    } else {
        StoragePath::new(
            value
                .to_str()
                .ok_or_else(|| StorageError::InvalidPath("non-UTF-8 storage path".into()))?,
        )
        .map_err(Into::into)
    }
}
pub(crate) fn timestamp(value: i64) -> StorageTimestamp {
    StorageTimestamp::new(i128::from(value), TimePrecision::Nanoseconds)
        .unwrap_or_else(|_| unreachable!("nanoseconds are always aligned"))
}
impl ConnectedStorage {
    pub fn validate_transfer_mode(&self, mode: TransferMode) -> Result<()> {
        let destination = self.roles.staged_destination()?;
        let reason = match mode {
            TransferMode::Direct if !destination.supports_direct() => {
                Some("direct writes are unavailable; select --resume or --in-place")
            }
            TransferMode::Checkpoint if !destination.resumable_at_destination() => {
                Some("destination checkpoints are unavailable; select --in-place or a supported backend")
            }
            _ => None,
        };
        if let Some(reason) = reason {
            return Err(StorageError::UnsupportedTransferMode {
                backend: self.roles.kind(),
                mode,
                reason,
            });
        }
        Ok(())
    }

    pub fn validate_stream_transfer_mode(&self, mode: TransferMode) -> Result<()> {
        self.validate_transfer_mode(mode)?;
        if mode == TransferMode::Direct && !self.roles.staged_destination()?.supports_sparse_patch() {
            return Err(StorageError::UnsupportedTransferMode {
                backend: self.roles.kind(),
                mode,
                reason: "remote Direct requires sparse patch support; use --resume or --in-place",
            });
        }
        Ok(())
    }

    pub fn storage(&self) -> &Storage {
        &self.roles
    }
    pub fn block_size(&self) -> u64 {
        self.block_size
    }
    pub fn has_real_directory_objects(&self) -> bool {
        self.roles.kind() != BackendKind::S3
    }
    pub async fn is_bucket_versioned(&self) -> Result<bool> {
        Ok(self.roles.staged_destination()?.keeps_history().await?)
    }
    pub async fn check_connectivity(&self) -> Result<()> {
        check_connectivity(&self.roles).await.map_err(StorageError::from)
    }
    pub async fn probe_server_time(&self) -> Result<Option<i64>> {
        Ok(probe_server_time(&self.roles)
            .await
            .map_err(StorageError::from)?
            .map(|v| (v.unix_nanos() / 1_000_000_000) as i64))
    }
    pub async fn get_metadata(&self, value: &Path) -> Result<EntryEnum> {
        let descriptor = self.roles.namespace()?.stat(&path(value)?).await?;
        self.project_descriptor(descriptor, true).await
    }
    pub async fn create_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        storage::create_directory_all(&self.roles, &path(entry.get_relative_path())?)
            .await
            .map_err(StorageError::from)
    }
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self.roles.namespace()?.rename(&path(from)?, &path(to)?).await?)
    }
    pub async fn delete_file(&self, entry: &EntryEnum) -> Result<()> {
        Ok(self
            .roles
            .namespace()?
            .delete(&path(entry.get_relative_path())?)
            .await?)
    }
    pub async fn read_symlink(&self, entry: &EntryEnum) -> Result<PathBuf> {
        let target = self
            .roles
            .namespace()?
            .read_link(&path(entry.get_relative_path())?)
            .await?;
        #[cfg(unix)]
        {
            Ok(std::ffi::OsString::from_vec(target.as_bytes().to_vec()).into())
        }
        #[cfg(windows)]
        {
            if target.encoding() == SymlinkTargetEncoding::WindowsWide {
                let wide: Vec<_> = target
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|v| u16::from_le_bytes([v[0], v[1]]))
                    .collect();
                Ok(std::ffi::OsString::from_wide(&wide).into())
            } else {
                Ok(String::from_utf8(target.as_bytes().to_vec())
                    .map_err(StorageError::from)?
                    .into())
            }
        }
    }
    pub async fn create_symlink(&self, entry: &EntryEnum, target: &Path) -> Result<()> {
        let name = path(entry.get_relative_path())?;
        if let Some(parent) = entry.get_relative_path().parent() {
            storage::create_directory_all(&self.roles, &path(parent)?)
                .await
                .map_err(StorageError::from)?;
        }
        // 旧产品支持用链接替换已有目录项；角色接口显式要求调用方删除。
        match self.roles.namespace()?.stat(&name).await {
            Ok(_) => self.roles.namespace()?.delete(&name).await?,
            Err(StorageRoleFailure::Entry(e)) if e.class() == FailureClass::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        #[cfg(unix)]
        let bytes = { target.as_os_str().as_bytes().to_vec() };
        #[cfg(windows)]
        let bytes = {
            target
                .as_os_str()
                .encode_wide()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>()
        };
        let encoding = if cfg!(windows) {
            SymlinkTargetEncoding::WindowsWide
        } else {
            SymlinkTargetEncoding::UnixBytes
        };
        let target = SymlinkTarget::new(encoding, bytes)?;
        Ok(self
            .roles
            .namespace()?
            .create_symlink(storage::CreateSymlinkRequest {
                path: name,
                target,
                uid: entry.get_uid(),
                gid: entry.get_gid(),
                accessed: Some(timestamp(entry.get_atime())),
                modified: Some(timestamp(entry.get_mtime())),
            })
            .await?)
    }
    pub async fn set_metadata(
        &self, value: &Path, atime: Option<i64>, mtime: Option<i64>, uid: Option<u32>, gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        let destination = self.roles.staged_destination()?;
        let capabilities = destination
            .copied_metadata_target()
            .ok_or_else(|| operation("destination does not advertise copied metadata support"))?;
        let metadata = self.roles.metadata()?;
        let p = path(value)?;
        let cancel = CancellationToken::new();
        // 旧 NAS mode 包含文件类型位；角色层的修改接口只接受权限位。
        let mode = mode.map(|value| value & 0o7777);
        if let (CopiedOwnershipTarget::Numeric, Some(uid), Some(gid), Some(mode)) =
            (capabilities.ownership, uid, gid, mode)
            && destination.may_set_owner(uid, gid)
        {
            metadata
                .apply(
                    &p,
                    MetadataMutation::NumericOwnership(OwnershipMode { uid, gid, mode }),
                    cancel.clone(),
                )
                .await?;
        } else if let Some(mode) = mode
            && capabilities.ownership != CopiedOwnershipTarget::Unsupported
        {
            metadata.apply(&p, MetadataMutation::Mode(mode), cancel.clone()).await?;
        }
        if let CopiedTimestampTarget::Stored(precision) = capabilities.timestamps
            && (atime.is_some() || mtime.is_some())
        {
            let aligned = |value: i64| {
                let quantum = precision.quantum_nanos();
                StorageTimestamp::new(i128::from(value).div_euclid(quantum) * quantum, precision)
                    .unwrap_or_else(|_| unreachable!("quantized timestamp is aligned"))
            };
            metadata
                .apply(
                    &p,
                    MetadataMutation::Timestamps(TimestampMetadata {
                        accessed: atime.map(aligned),
                        modified: mtime.map(aligned),
                        created: None,
                    }),
                    cancel,
                )
                .await?;
        }
        Ok(())
    }
    pub async fn set_entry_metadata(&self, entry: &EntryEnum) -> Result<()> {
        if self.roles.kind() == BackendKind::S3 {
            return Ok(());
        }
        self.set_metadata(
            entry.get_relative_path(),
            Some(entry.get_atime()),
            Some(entry.get_mtime()),
            entry.get_uid(),
            entry.get_gid(),
            entry.get_mode(),
        )
        .await
    }
    pub async fn get_acl_bytes(&self, value: &Path) -> Result<Option<Vec<u8>>> {
        let facts = self
            .roles
            .metadata()?
            .observe(
                &path(value)?,
                ObservationPlan::default().with_acl(ObservationMode::BestEffort),
            )
            .await?;
        facts.acl().value().map(encode_acl).transpose()
    }
    pub async fn set_acl_bytes(&self, value: &Path, bytes: &[u8]) -> Result<()> {
        let acl = decode_acl(bytes)?;
        self.apply_acl(value, acl).await
    }
}
impl StorageEnum {
    pub async fn copy_file(from: &Self, to: &Self, entry: &EntryEnum, options: CopyOptions) -> Result<()> {
        to.validate_transfer_mode(options.transfer_mode)?;
        let p = path(entry.get_relative_path())?;
        if entry.get_is_delete_marker() {
            transfer::write_delete_marker(to.roles.clone(), p, options.cancel.unwrap_or_default()).await?;
            return Ok(());
        }
        let mut request = TransferRequest::new(
            from.roles.clone(),
            p,
            to.roles.clone(),
            options.cancel.unwrap_or_default(),
        )
        .with_transfer_policy(options.transfer_mode.policy());
        if let Some(version) = entry.get_version_id() {
            request = request.with_source_version(SourceVersion::Id(version.into()));
        }
        request = request.with_read_back_verification(if options.enable_integrity_check {
            ReadBackVerification::Enabled
        } else {
            ReadBackVerification::Disabled
        });
        if let Some(qos) = options.qos {
            request = request.with_source_qos(source_qos(&qos, from.block_size)?);
        }
        let outcome = if options.is_source_reserved {
            transfer::transfer(request).await?
        } else {
            transfer::move_file(request).await?
        };
        if let Some(counter) = options.bytes_counter {
            counter.fetch_add(outcome.transferred_bytes, Ordering::Relaxed);
        }
        Ok(())
    }
    pub async fn copy_file_resumable(
        from: &Self, to: &Self, entry: &EntryEnum, options: CopyOptions, _resume: ResumeContext,
    ) -> Result<()> {
        to.validate_transfer_mode(TransferMode::Checkpoint)?;
        // 恢复权威在目标端；不再信任应用的 .part 路径或本地 offset 记录。
        let mut request = TransferRequest::new(
            from.roles.clone(),
            path(entry.get_relative_path())?,
            to.roles.clone(),
            options.cancel.unwrap_or_default(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed);
        if let Some(version) = entry.get_version_id() {
            request = request.with_source_version(SourceVersion::Id(version.into()));
        }
        request = request.with_read_back_verification(if options.enable_integrity_check {
            ReadBackVerification::Enabled
        } else {
            ReadBackVerification::Disabled
        });
        if let Some(qos) = options.qos {
            request = request.with_source_qos(source_qos(&qos, from.block_size)?);
        }
        let outcome = if options.is_source_reserved {
            transfer::transfer(request).await?
        } else {
            transfer::move_file(request).await?
        };
        tracing::debug!(
            transferred_bytes = outcome.transferred_bytes,
            reused_bytes = outcome.reused_bytes,
            recovery = ?outcome.recovery,
            "checkpointed transfer completed"
        );
        if let Some(counter) = options.bytes_counter {
            counter.fetch_add(outcome.transferred_bytes, Ordering::Relaxed);
        }
        Ok(())
    }
    pub async fn copy_acl(from: &Self, to: &Self, value: &Path) -> Result<()> {
        if let Some(bytes) = from.get_acl_bytes(value).await? {
            to.set_acl_bytes(value, &bytes).await?;
        }
        Ok(())
    }
    pub async fn copy_xattr(from: &Self, to: &Self, value: &Path) -> Result<()> {
        let p = path(value)?;
        let facts = from
            .roles
            .metadata()?
            .observe(&p, ObservationPlan::default().with_xattrs(ObservationMode::BestEffort))
            .await?;
        if let Some(attrs) = facts.xattrs().value() {
            to.roles
                .metadata()?
                .apply(&p, MetadataMutation::Xattrs(attrs.clone()), CancellationToken::new())
                .await?;
        }
        Ok(())
    }
}

impl ConnectedStorage {
    /// 在当前目标的 ACL 能力下先编译映射，不能把另一协议的原生字节直接写入。
    pub(crate) async fn apply_acl(&self, value: &Path, acl: AclMetadata) -> Result<()> {
        let target = self.roles.staged_destination()?.copied_metadata_target();
        let acl_target = match target.map(|v| v.acl) {
            Some(CopiedAclTarget::Encoding(encoding)) => AclTarget::Encoding(encoding),
            _ => AclTarget::Unsupported,
        };
        let observations = MetadataObservations::new(
            MetadataObservation::Value {
                value: acl,
                provenance: MetadataProvenance::AdditionalCall,
            },
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
        )?;
        let plan = compile_copied_metadata_plan(
            &MetadataPlanRequest {
                observations: &observations,
                target: MetadataTarget {
                    acl: acl_target,
                    xattrs: ValueTarget::NotApplicable,
                    tags: ValueTarget::NotApplicable,
                    ownership_mode: OwnershipTarget::NotApplicable,
                    timestamps: TimestampTargetCapability::NotApplicable,
                },
                policies: MetadataPolicies::default().with_acl(MetadataPolicy::BestEffort),
                principal_mapper: None,
            },
            None,
            false,
            true,
        )
        .map_err(StorageError::from)?;
        plan.apply(&*self.roles.metadata()?, &path(value)?, CancellationToken::new())
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
