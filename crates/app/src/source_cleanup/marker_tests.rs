//! 标记只核验已选事实；不访问当前隐藏路径或不存在的数据／元数据接口。
#![allow(clippy::unwrap_used)]
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use data_mover::model::{
    BackendKind, HistoryOrder, ObjectHistory, RecordKind, StoragePath, StorageTimestamp, TimePrecision,
};
use data_mover::storage::{ByteStream, ReadRequest, ReadSource, StorageBuilder};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::error::AppError;
use crate::observed_fixtures::observation;
use crate::storage_factory::{CreateStorageOptions, create_storage};

struct MarkerReader(SourceDescriptor);

#[async_trait]
impl ReadSource for MarkerReader {
    async fn describe(&self, _: &StoragePath) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        panic!("marker verification must not describe the hidden current path")
    }
    async fn describe_version(
        &self, _: &StoragePath, version: &SourceVersion,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        assert_eq!(version, &self.0.version);
        Ok(self.0.clone())
    }
    async fn read(&self, _: ReadRequest) -> std::result::Result<ByteStream, StorageRoleFailure> {
        panic!("a marker has no readable data")
    }
}

#[tokio::test]
async fn marker_identity_and_mutable_null_time_are_required_without_reading_content() -> Result<()> {
    let root = tempfile::tempdir()?;
    // payload 没有本地实体；错误进入普通 metadata 读取会失败。
    let mut source = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new());
    for (id, seconds, success) in [
        (Some("v1"), None, true),
        (None, Some(2), true),
        (None, Some(3), false),
        (None, None, false),
    ] {
        let entry = observation(
            BackendKind::S3,
            "payload".into(),
            EntryKind::File,
            0,
            [0, 2_777_000_000, 0],
            None,
            None,
            None,
            Some((id.map(str::to_owned), true, true)),
        );
        let actual = VersionRecord::new(
            id.map(str::to_owned),
            false,
            RecordKind::DeleteMarker {
                modified: seconds
                    .map(|seconds| StorageTimestamp::new(seconds * 1_000_000_000, TimePrecision::Seconds).unwrap()),
            },
            entry.source_identity().clone(),
            None,
        )
        .unwrap();
        let mut descriptor = SourceDescriptor::new(
            entry.path().clone(),
            EntryKind::File,
            None,
            entry.source_identity().clone(),
        );
        descriptor.version = SourceVersion::Id(id.unwrap_or("null").into());
        descriptor.listing.listing_only = true;
        descriptor.listing.history = Some(Box::new(
            ObjectHistory::segment_of(vec![actual], HistoryOrder::Server, 0, false, None).unwrap(),
        ));
        source.roles = StorageBuilder::new(source.roles.identity().clone(), source.roles.capabilities().clone())
            .namespace(source.roles.namespace()?)
            .metadata(source.roles.metadata()?)
            .staged_destination(source.roles.staged_destination()?)
            .read_source(Arc::new(MarkerReader(descriptor)))
            .build()
            .unwrap();
        let result = verify_source_version(
            &source,
            ObservedView {
                entry: &entry,
                version_index: Some(0),
            },
            &check,
        )
        .await;
        assert_eq!(
            result.is_ok(),
            success,
            "id={id:?} seconds={seconds:?} result={result:?}"
        );
        if !success {
            assert!(matches!(
                result,
                Err(AppError::StorageError(StorageError::HistoryCleanupUnconfirmed { .. }))
            ));
        }
    }
    Ok(())
}
