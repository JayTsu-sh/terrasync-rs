//! 根目录原值在普通条目之外收尾；空根也不能遗漏。
use std::{
    fs::{File, FileTimes, Permissions},
    os::unix::fs::PermissionsExt,
    time::{Duration, UNIX_EPOCH},
};

use task_model::SourceCleanupPolicy;
use utils::app_config::AppConfig;

use super::tests::job;
use crate::{error::Result, orchestrator::SyncOrchestrator};

#[tokio::test]
#[ignore = "runs an isolated production orchestrator with a dedicated global configuration"]
async fn real_no_database_copy_preserves_source_root_metadata_for_empty_and_nonempty_roots() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    AppConfig::override_with(|config| config.database.enabled = false)?;
    for empty in [true, false] {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        let target = root.path().join("target");
        std::fs::create_dir(&source)?;
        std::fs::create_dir(&target)?;
        std::fs::set_permissions(&source, Permissions::from_mode(0o750))?;
        std::fs::set_permissions(&target, Permissions::from_mode(0o777))?;
        if !empty {
            std::fs::write(source.join("payload"), b"root metadata payload")?;
        }
        let expected = UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
        File::open(&source)?.set_times(FileTimes::new().set_modified(expected))?;
        let mut command = job(root.path());
        command.job_id = if empty { "root_empty" } else { "root_nonempty" }.into();
        command.src_path = source.to_string_lossy().into_owned();
        command.dest_path = target.to_string_lossy().into_owned();
        command.source_cleanup = SourceCleanupPolicy::Retain;
        SyncOrchestrator::new_local(command).run().await?;
        assert_eq!(
            std::fs::metadata(&target)?.modified()?,
            expected,
            "root mtime, empty={empty}"
        );
        assert_eq!(
            std::fs::metadata(&target)?.permissions().mode() & 0o7777,
            0o750,
            "root mode, empty={empty}"
        );
        if !empty {
            assert_eq!(std::fs::read(source.join("payload"))?, b"root metadata payload");
            assert_eq!(std::fs::read(target.join("payload"))?, b"root metadata payload");
        }
    }
    Ok(())
}
