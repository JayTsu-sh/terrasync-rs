//! 暖缓存返回旧描述时，目录阶段仍核对当前路径，而非信任元数据回读描述。
use std::{
    error::Error as StdError,
    os::unix::fs::MetadataExt as _,
    path::PathBuf,
    sync::{Mutex, atomic::AtomicBool},
    time::SystemTime,
};

use super::*;
use crate::copy_root::CopyRootMetadata;
use crate::receiver::LocalCopyCompletion;
use data_mover::{
    metadata::MetadataVerificationError,
    model::{EntryKind, ObservedEntry, PermissionFacts, SourceVersion, StoragePath},
    storage::NamespaceReadPolicy,
};

const COPIED_MODIFIED_NANOS: u64 = 1_700_000_123_987_654_321;
const REPLACEMENT_MODIFIED_NANOS: u64 = 1_800_000_456_123_456_789;

#[derive(Clone)]
struct StatWitness {
    ordinal: usize,
    descriptor: SourceDescriptor,
}

#[derive(Clone)]
struct ReplacementWitness {
    old_inode: u64,
    new_inode: u64,
    old_mode: u32,
    new_mode: u32,
    old_modified: SystemTime,
    new_modified: SystemTime,
}

fn replace_directory(physical: &Path, held: &Path) -> ReplacementWitness {
    let old = std::fs::metadata(physical).unwrap();
    std::fs::rename(physical, held).unwrap();
    std::fs::create_dir(physical).unwrap();
    std::fs::set_permissions(physical, Permissions::from_mode(0o711)).unwrap();
    File::open(physical)
        .unwrap()
        .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_nanos(REPLACEMENT_MODIFIED_NANOS)))
        .unwrap();
    let new = std::fs::metadata(physical).unwrap();
    ReplacementWitness {
        old_inode: old.ino(),
        new_inode: new.ino(),
        old_mode: old.permissions().mode() & 0o7777,
        new_mode: new.permissions().mode() & 0o7777,
        old_modified: old.modified().unwrap(),
        new_modified: new.modified().unwrap(),
    }
}

struct CachedNamespace {
    inner: Arc<dyn Namespace>,
    path: StoragePath,
    cached: SourceDescriptor,
    physical: PathBuf,
    held: PathBuf,
    replace_at_fresh: Arc<AtomicUsize>,
    fresh_ordinal: Arc<AtomicUsize>,
    witnesses: Arc<Mutex<Vec<StatWitness>>>,
    mutation: Arc<Mutex<Option<ReplacementWitness>>>,
    calls: Arc<Mutex<Vec<bool>>>,
}

#[async_trait]
impl Namespace for CachedNamespace {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        let ordinal = if path == &self.path {
            self.calls.lock().unwrap().push(fresh);
            if !fresh {
                return Ok(self.cached.clone());
            }
            let ordinal = self.fresh_ordinal.fetch_add(1, Ordering::SeqCst) + 1;
            if self.replace_at_fresh.load(Ordering::SeqCst) == ordinal {
                *self.mutation.lock().unwrap() = Some(replace_directory(&self.physical, &self.held));
            }
            Some(ordinal)
        } else {
            None
        };
        // 这里只模拟NFS canonical-root guard的Conflict契约；Local真实根仍绑定capability FD。
        if fresh && path.as_str().is_empty() && self.held.exists() {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Observe,
                    FailureClass::Conflict,
                    Transience::Permanent,
                    "configured root name no longer matches the held root",
                )
                .unwrap(),
            ));
        }
        let descriptor = self.inner.stat(path, fresh).await?;
        if let Some(ordinal) = ordinal {
            self.witnesses.lock().unwrap().push(StatWitness {
                ordinal,
                descriptor: descriptor.clone(),
            });
        }
        Ok(descriptor)
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.inner.create_directory(path).await
    }
}

struct CacheControl {
    replace_at_fresh: Arc<AtomicUsize>,
    fresh_ordinal: Arc<AtomicUsize>,
    witnesses: Arc<Mutex<Vec<StatWitness>>>,
    mutation: Arc<Mutex<Option<ReplacementWitness>>>,
    calls: Arc<Mutex<Vec<bool>>>,
    cached: SourceDescriptor,
    physical: PathBuf,
    held: PathBuf,
}

async fn cache_namespace(
    context: &mut StorageContext, path: StoragePath, physical: PathBuf,
) -> TestResult<CacheControl> {
    let cached = context.roles.namespace()?.stat(&path, true).await?;
    let replace_at_fresh = Arc::new(AtomicUsize::new(0));
    let fresh_ordinal = Arc::new(AtomicUsize::new(0));
    let witnesses = Arc::new(Mutex::new(Vec::new()));
    let mutation = Arc::new(Mutex::new(None));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let held = physical.with_file_name("held");
    let namespace = CachedNamespace {
        inner: context.roles.namespace()?,
        path,
        cached: cached.clone(),
        physical: physical.clone(),
        held: held.clone(),
        replace_at_fresh: replace_at_fresh.clone(),
        fresh_ordinal: fresh_ordinal.clone(),
        witnesses: witnesses.clone(),
        mutation: mutation.clone(),
        calls: calls.clone(),
    };
    context.roles = StorageBuilder::new(context.roles.identity().clone(), context.roles.capabilities().clone())
        .read_source(context.roles.read_source()?)
        .staged_destination(context.roles.staged_destination()?)
        .metadata(context.roles.metadata()?)
        .namespace(Arc::new(namespace))
        .build()?;
    Ok(CacheControl {
        replace_at_fresh,
        fresh_ordinal,
        witnesses,
        mutation,
        calls,
        cached,
        physical,
        held,
    })
}

struct OpaqueReadControl {
    reads: Arc<AtomicUsize>,
    bound: Arc<Mutex<Vec<SourceIdentity>>>,
    namespace_calls_at_read: Arc<Mutex<Vec<Vec<bool>>>>,
}

struct OpaqueMetadata {
    inner: Arc<dyn Metadata>,
    physical: PathBuf,
    held: PathBuf,
    replace: AtomicBool,
    mutation: Arc<Mutex<Option<ReplacementWitness>>>,
    calls: Arc<Mutex<Vec<bool>>>,
    reads: Arc<AtomicUsize>,
    bound: Arc<Mutex<Vec<SourceIdentity>>>,
    namespace_calls_at_read: Arc<Mutex<Vec<Vec<bool>>>>,
}

#[async_trait]
impl Metadata for OpaqueMetadata {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        self.inner.copied_metadata_observation_plan()
    }
    fn observe_listed(
        &self, _: &SourceDescriptor, _: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        None
    }
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.inner.observe(path, plan).await
    }
    async fn observe_copy_bound_version(
        &self, path: &StoragePath, expected: &SourceIdentity, version: &SourceVersion, plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        self.namespace_calls_at_read
            .lock()
            .unwrap()
            .push(self.calls.lock().unwrap().clone());
        self.bound.lock().unwrap().push(expected.clone());
        let observed = self
            .inner
            .observe_copy_bound_version(path, expected, version, plan)
            .await?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        // 真正的绑定回读已取得旧目录事实；在该 future 返回前替换当前名字。
        if self.replace.swap(false, Ordering::SeqCst) {
            *self.mutation.lock().unwrap() = Some(replace_directory(&self.physical, &self.held));
        }
        Ok(observed)
    }
    async fn permissions(&self, descriptor: &SourceDescriptor) -> Result<PermissionFacts, StorageRoleFailure> {
        self.inner.permissions(descriptor).await
    }
    async fn apply(
        &self, path: &StoragePath, value: MetadataMutation, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.inner.apply(path, value, cancel).await
    }
    fn supports_bound_directory_metadata(&self) -> bool {
        self.inner.supports_bound_directory_metadata()
    }
    async fn apply_bound_directory(
        &self, path: &StoragePath, expected: EntryIdentityKey, mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        self.inner
            .apply_bound_directory(path, expected, mutations, cancel)
            .await
    }
}

fn opaque_verification(context: &mut StorageContext, cache: &CacheControl) -> TestResult<OpaqueReadControl> {
    let reads = Arc::new(AtomicUsize::new(0));
    let bound = Arc::new(Mutex::new(Vec::new()));
    let namespace_calls_at_read = Arc::new(Mutex::new(Vec::new()));
    let metadata = OpaqueMetadata {
        inner: context.roles.metadata()?,
        physical: cache.physical.clone(),
        held: cache.held.clone(),
        replace: AtomicBool::new(true),
        mutation: cache.mutation.clone(),
        calls: cache.calls.clone(),
        reads: reads.clone(),
        bound: bound.clone(),
        namespace_calls_at_read: namespace_calls_at_read.clone(),
    };
    context.roles = StorageBuilder::new(context.roles.identity().clone(), context.roles.capabilities().clone())
        .read_source(context.roles.read_source()?)
        .staged_destination(context.roles.staged_destination()?)
        .namespace(context.roles.namespace()?)
        .metadata(Arc::new(metadata))
        .build()?;
    Ok(OpaqueReadControl {
        reads,
        bound,
        namespace_calls_at_read,
    })
}

async fn metadata_directory(executor: &LocalCopyExecutor, source: &Path) -> TestResult<Arc<ObservedEntry>> {
    let physical = source.join("dir");
    std::fs::set_permissions(&physical, Permissions::from_mode(0o751))?;
    File::open(&physical)?
        .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_nanos(COPIED_MODIFIED_NANOS)))?;
    Ok(Arc::new(
        observe_path_with_plan(
            executor.source.storage(),
            Path::new("dir"),
            options().observation_plan.unwrap(),
        )
        .await?,
    ))
}

fn assert_replacement_preserved(cache: &CacheControl) -> TestResult {
    let witness = cache
        .mutation
        .lock()
        .unwrap()
        .clone()
        .ok_or("replacement did not occur")?;
    let held = std::fs::metadata(&cache.held)?;
    let current = std::fs::metadata(&cache.physical)?;
    assert_ne!(witness.old_inode, witness.new_inode);
    assert_eq!(held.ino(), witness.old_inode);
    assert_eq!(current.ino(), witness.new_inode);
    assert_eq!(witness.old_mode, 0o751);
    assert_eq!(witness.new_mode, 0o711);
    assert_eq!(held.permissions().mode() & 0o7777, witness.old_mode);
    assert_eq!(current.permissions().mode() & 0o7777, witness.new_mode);
    assert_eq!(
        witness.old_modified,
        UNIX_EPOCH + Duration::from_nanos(COPIED_MODIFIED_NANOS)
    );
    assert_eq!(
        witness.new_modified,
        UNIX_EPOCH + Duration::from_nanos(REPLACEMENT_MODIFIED_NANOS)
    );
    assert_eq!(held.modified()?, witness.old_modified);
    assert_eq!(current.modified()?, witness.new_modified);
    assert_eq!(std::fs::read_dir(&cache.physical)?.count(), 0);
    Ok(())
}

async fn assert_rejected_once(executor: &mut LocalCopyExecutor, completion: LocalCopyCompletion) -> TestResult {
    assert!(!completion.phase_proof.is_ready());
    assert!(
        matches!(&completion.result, Err(error) if matches!(error.storage_error(),
        Some(StorageError::TargetPublicationChanged { source: Some(source), .. })
            if matches!(source.as_ref(), StorageError::MetadataVerification(
                MetadataVerificationError::VersionChanged { path }) if path.as_str() == "dir")))
    );
    let error = completion
        .result
        .as_ref()
        .err()
        .ok_or("expected anchored replacement failure")?;
    let mut cause: &(dyn StdError + 'static) = error;
    loop {
        if let Some(source) = cause.downcast_ref::<Box<StorageError>>() {
            assert!(matches!(
                source.as_ref(),
                StorageError::MetadataVerification(MetadataVerificationError::VersionChanged { path })
                    if path.as_str() == "dir"
            ));
            break;
        }
        cause = cause
            .source()
            .ok_or("AppError source chain lost the typed native verification cause")?;
    }
    let mut events = executor.broadcaster.subscribe();
    assert!(
        executor
            .publish(completion)
            .await
            .as_ref()
            .is_err_and(AppError::is_reported_copy_failure)
    );
    assert!(
        matches!(events.try_recv(), Ok(StorageEntryMessage::CopyVersionFailed { first_in_entry: true, reason, .. })
        if reason.contains("目标发布状态") && reason.contains("dir"))
    );
    assert!(
        events.try_recv().is_err(),
        "no New or second entry failure may be published"
    );
    Ok(())
}

async fn bare_directory(executor: &LocalCopyExecutor) -> TestResult<Arc<ObservedEntry>> {
    let path = StoragePath::new("dir")?;
    let descriptor = executor.source.storage().namespace()?.stat(&path, false).await?;
    Ok(Arc::new(ObservedEntry::new(
        path,
        EntryKind::Directory,
        None,
        None,
        descriptor.source_identity,
    )?))
}

#[tokio::test]
async fn prepare_captures_current_name_instead_of_warm_cached_identity() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::create_dir(target.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let entry = bare_directory(&executor).await?;
    let cache = cache_namespace(
        Arc::make_mut(&mut executor.destination),
        StoragePath::new("dir")?,
        target.path().join("dir"),
    )
    .await?;
    std::fs::rename(target.path().join("dir"), target.path().join("held"))?;
    std::fs::create_dir(target.path().join("dir"))?;
    let current = executor
        .destination
        .storage()
        .namespace()?
        .stat(entry.path(), true)
        .await?;
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([1; 32], LocalCopyPhase::Prepare)),
        })
        .await;
    assert!(completion.phase_proof.is_ready());
    let LocalCopyOutcome::Prepared(identity) = completion.result? else {
        unreachable!()
    };
    assert_ne!(identity, cache.cached.source_identity.identity_key());
    assert_eq!(identity, current.source_identity.identity_key());
    // 第一次是测试读取；Prepare 用同一次 fresh stat 确认目录已存在并取得当前身份。
    assert_eq!(*cache.calls.lock().unwrap(), vec![true, true]);
    Ok(())
}

struct ReplaceBeforeMetadata {
    inner: Arc<dyn Metadata>,
    physical: PathBuf,
    held: PathBuf,
    replacement: Arc<Mutex<Option<ReplacementWitness>>>,
}

impl ReplaceBeforeMetadata {
    fn replace_once(&self) {
        let mut replacement = self.replacement.lock().unwrap();
        if replacement.is_none() {
            *replacement = Some(replace_directory(&self.physical, &self.held));
        }
    }
}

#[async_trait]
impl Metadata for ReplaceBeforeMetadata {
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.inner.observe(path, plan).await
    }

    async fn apply(
        &self, path: &StoragePath, mutation: MetadataMutation, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.replace_once();
        self.inner.apply(path, mutation, cancel).await
    }

    fn supports_bound_directory_metadata(&self) -> bool {
        self.inner.supports_bound_directory_metadata()
    }

    async fn apply_bound_directory(
        &self, path: &StoragePath, identity: EntryIdentityKey, mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        self.replace_once();
        self.inner
            .apply_bound_directory(path, identity, mutations, cancel)
            .await
    }
}

#[tokio::test]
async fn replacement_between_prepare_and_metadata_write_leaves_both_directories_unchanged() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::create_dir(target.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let entry = metadata_directory(&executor, source.path()).await?;
    let prepared = executor
        .execute(LocalCopyWork {
            entry: entry.clone(),
            control: Some(([1; 32], LocalCopyPhase::Prepare)),
        })
        .await;
    let LocalCopyOutcome::Prepared(identity) = prepared.result? else {
        unreachable!()
    };
    let destination = Arc::make_mut(&mut executor.destination);
    let cache = cache_namespace(destination, StoragePath::new("dir")?, target.path().join("dir")).await?;
    destination.roles = StorageBuilder::new(
        destination.roles.identity().clone(),
        destination.roles.capabilities().clone(),
    )
    .read_source(destination.roles.read_source()?)
    .staged_destination(destination.roles.staged_destination()?)
    .namespace(destination.roles.namespace()?)
    .metadata(Arc::new(ReplaceBeforeMetadata {
        inner: destination.roles.metadata()?,
        physical: cache.physical.clone(),
        held: cache.held.clone(),
        replacement: cache.mutation.clone(),
    }))
    .build()?;
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    assert!(!completion.phase_proof.is_ready());
    assert!(matches!(
        completion.result.as_ref().err().and_then(AppError::storage_error),
        Some(StorageError::TargetPublicationChanged { source: Some(source), .. })
            if matches!(source.as_ref(), StorageError::MetadataApplication(error)
                if matches!(error.storage_error(), Some(StorageRoleFailure::Entry(error))
                    if error.class() == FailureClass::Conflict))
    ));
    let mut events = executor.broadcaster.subscribe();
    assert!(
        executor
            .publish(completion)
            .await
            .as_ref()
            .is_err_and(AppError::is_reported_copy_failure)
    );
    assert!(matches!(
        events.try_recv(),
        Ok(StorageEntryMessage::CopyVersionFailed {
            first_in_entry: true,
            ..
        })
    ));
    assert!(events.try_recv().is_err());
    assert!(cache.calls.lock().unwrap().is_empty());
    let replacement = cache
        .mutation
        .lock()
        .unwrap()
        .clone()
        .ok_or("replacement did not occur at write")?;
    assert_ne!(replacement.old_inode, replacement.new_inode);
    for (path, inode, mode, modified) in [
        (
            &cache.held,
            replacement.old_inode,
            replacement.old_mode,
            replacement.old_modified,
        ),
        (
            &cache.physical,
            replacement.new_inode,
            replacement.new_mode,
            replacement.new_modified,
        ),
    ] {
        let metadata = std::fs::metadata(path)?;
        assert_eq!(metadata.ino(), inode);
        assert_eq!(metadata.permissions().mode() & 0o7777, mode);
        assert_eq!(metadata.modified()?, modified);
    }
    assert_eq!(replacement.new_mode, 0o711);
    assert_eq!(
        replacement.new_modified,
        UNIX_EPOCH + Duration::from_nanos(REPLACEMENT_MODIFIED_NANOS)
    );
    Ok(())
}

#[tokio::test]
async fn finalize_rechecks_current_name_after_cached_metadata_verification() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::create_dir(target.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let entry = metadata_directory(&executor, source.path()).await?;
    let cache = cache_namespace(
        Arc::make_mut(&mut executor.destination),
        StoragePath::new("dir")?,
        target.path().join("dir"),
    )
    .await?;
    let identity = cache.cached.source_identity.identity_key();
    // 写入身份在 Metadata 内核验；命名空间只执行写后 fresh。
    cache.replace_at_fresh.store(1, Ordering::SeqCst);
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    assert!(
        target.path().join("held").is_dir(),
        "replacement occurs at the actual final fresh read"
    );
    assert_eq!(*cache.calls.lock().unwrap(), vec![true]);
    let witnesses = cache.witnesses.lock().unwrap().clone();
    assert_eq!(witnesses.len(), 1);
    assert_ne!(witnesses[0].descriptor.source_identity.identity_key(), identity);
    assert_rejected_once(&mut executor, completion).await?;
    assert_replacement_preserved(&cache)?;
    assert!(source.path().join("dir").is_dir());
    Ok(())
}

#[tokio::test]
async fn root_rechecks_current_name_after_bound_metadata_write() -> TestResult {
    let source = tempfile::tempdir()?;
    let parent = tempfile::tempdir()?;
    let target = parent.path().join("target");
    std::fs::create_dir(&target)?;
    let mut executor = executor(source.path(), &target, CancellationToken::new()).await?;
    let cache = cache_namespace(
        Arc::make_mut(&mut executor.destination),
        StoragePath::root(),
        target.clone(),
    )
    .await?;
    let root = CopyRootMetadata::new(
        executor.source.clone(),
        executor.destination.clone(),
        true,
        ObservationPlan::default(),
        NamespaceReadPolicy {
            operation_timeout: Duration::from_millis(500),
            recovery_window: Duration::from_millis(100),
            retry_interval: Duration::from_millis(10),
        },
        CancellationToken::new(),
        false,
    );
    root.capture().await?;
    cache.calls.lock().unwrap().clear();
    let final_ordinal = cache.fresh_ordinal.load(Ordering::SeqCst) + 1;
    cache.replace_at_fresh.store(final_ordinal, Ordering::SeqCst);
    assert!(root.finish(&executor.destination).await.is_err());
    assert!(parent.path().join("held").is_dir());
    assert_eq!(*cache.calls.lock().unwrap(), vec![true]);
    assert_eq!(std::fs::metadata(&target)?.permissions().mode() & 0o7777, 0o711);
    Ok(())
}

#[tokio::test]
async fn unchanged_directory_finishes_with_one_fresh_path_lookup_and_ordinary_observation_stays_cached() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let entry = bare_directory(&executor).await?;
    std::fs::create_dir(target.path().join("dir"))?;
    std::fs::set_permissions(target.path().join("dir"), Permissions::from_mode(0o700))?;
    let cache = cache_namespace(
        Arc::make_mut(&mut executor.destination),
        StoragePath::new("dir")?,
        target.path().join("dir"),
    )
    .await?;
    let prepared = executor
        .execute(LocalCopyWork {
            entry: entry.clone(),
            control: Some(([1; 32], LocalCopyPhase::Prepare)),
        })
        .await;
    let LocalCopyOutcome::Prepared(identity) = prepared.result? else {
        unreachable!()
    };
    cache.calls.lock().unwrap().clear();
    cache.witnesses.lock().unwrap().clear();
    let final_ordinal = cache.fresh_ordinal.load(Ordering::SeqCst) + 1;
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    assert!(completion.phase_proof.is_ready());
    assert!(completion.result.is_ok());
    assert_eq!(*cache.calls.lock().unwrap(), vec![true]);
    let witnesses = cache.witnesses.lock().unwrap().clone();
    assert_eq!(
        witnesses.iter().map(|value| value.ordinal).collect::<Vec<_>>(),
        vec![final_ordinal]
    );
    assert!(
        witnesses
            .iter()
            .all(|value| value.descriptor.kind == EntryKind::Directory
                && value.descriptor.source_identity.identity_key() == identity)
    );
    cache.calls.lock().unwrap().clear();
    std::fs::set_permissions(target.path().join("dir"), Permissions::from_mode(0o711))?;
    let ordinary = observe_path_with_plan(
        executor.destination.storage(),
        Path::new("dir"),
        ObservationPlan::default(),
    )
    .await?;
    assert_eq!(*cache.calls.lock().unwrap(), vec![false]);
    assert_eq!(
        ordinary.source_identity().identity_key(),
        cache.cached.source_identity.identity_key()
    );
    assert_eq!(ordinary.permissions(), cache.cached.permissions.as_deref());
    assert_ne!(
        ordinary.permissions().and_then(|value| value.mode),
        Some(std::fs::metadata(target.path().join("dir"))?.permissions().mode() & 0o7777)
    );
    Ok(())
}

#[tokio::test]
async fn opaque_bound_verification_still_checks_the_actual_last_fresh_after_replacement() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::create_dir(target.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let entry = metadata_directory(&executor, source.path()).await?;
    let cache = cache_namespace(
        Arc::make_mut(&mut executor.destination),
        StoragePath::new("dir")?,
        target.path().join("dir"),
    )
    .await?;
    let identity = cache.cached.source_identity.identity_key();
    let opaque = opaque_verification(Arc::make_mut(&mut executor.destination), &cache)?;
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    assert_eq!(opaque.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        opaque.bound.lock().unwrap().as_slice(),
        &[cache.cached.source_identity.clone()]
    );
    assert_eq!(*opaque.namespace_calls_at_read.lock().unwrap(), vec![vec![true]]);
    assert_eq!(*cache.calls.lock().unwrap(), vec![true, true]);
    let witnesses = cache.witnesses.lock().unwrap().clone();
    assert_eq!(
        witnesses.iter().map(|value| value.ordinal).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(witnesses[0].descriptor.source_identity.identity_key(), identity);
    assert_ne!(witnesses[1].descriptor.source_identity.identity_key(), identity);
    assert_rejected_once(&mut executor, completion).await?;
    assert_replacement_preserved(&cache)?;
    assert!(source.path().join("dir").is_dir());
    Ok(())
}
