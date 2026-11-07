#![allow(clippy::unwrap_used)]

use super::*;
use crate::observed_fixtures::{directory_marker, observation};

fn object(path: &str, size: u64, latest: bool, tags: bool) -> ObservedEntry {
    observation(
        BackendKind::S3,
        path.into(),
        EntryKind::File,
        size,
        [123; 3],
        None,
        None,
        tags.then(Vec::new),
        Some((Some(if latest { "latest" } else { "old" }.into()), latest, false)),
    )
}

#[test]
fn omitted_content_object_output_does_not_require_tags_but_keeps_history_checks() {
    let entry = object("folder/", 3, true, false);
    let validation = ObservationValidation::new(
        true,
        MetadataScope {
            directory_object_output: false,
            ..Default::default()
        },
    );
    assert!(validation.validate(&entry).is_ok());
    assert!(
        ObservationValidation::new(true, MetadataScope::default())
            .validate(&entry)
            .is_err()
    );
    let whole = ObjectHistory::whole(
        vec![
            object("folder/", 3, false, false).history().unwrap().records()[0].clone(),
            entry.history().unwrap().records()[0].clone(),
        ],
        HistoryOrder::Server,
    )
    .unwrap();
    let segments = entry.with_history(Some(Box::new(whole))).unwrap().into_segments(1);
    let mut ordered = HistorySequence::default();
    for segment in &segments {
        ordered.check(segment, validation).unwrap();
    }
    assert!(ordered.is_complete());
    assert!(HistorySequence::default().check(&segments[1], validation).is_err());
}

#[test]
fn omission_does_not_hide_missing_metadata_for_regular_files_empty_markers_or_unknown_sizes() {
    let validation = ObservationValidation::new(
        true,
        MetadataScope {
            directory_object_output: false,
            ..Default::default()
        },
    );
    for entry in [object("file", 3, true, false), object("empty/", 0, true, false)] {
        assert!(validation.validate(&entry).is_err());
    }
    let content = object("unknown/", 3, true, false);
    let unknown = ObservedEntry::new(
        content.path().clone(),
        EntryKind::File,
        None,
        content.modified(),
        content.source_identity().clone(),
    )
    .unwrap();
    assert!(validation.validate(&unknown).is_err());
    assert!(validation.validate(&directory_marker("empty", Vec::new())).is_ok());
}

#[test]
fn latest_only_requires_latest_tags_but_ignores_omitted_history_and_delete_markers() {
    let latest = object("file", 3, true, true);
    let history = ObjectHistory::whole(
        vec![
            object("file", 3, false, false).history().unwrap().records()[0].clone(),
            latest.history().unwrap().records()[0].clone(),
        ],
        HistoryOrder::Server,
    )
    .unwrap();
    let entry = latest.with_history(Some(Box::new(history))).unwrap();
    assert!(validate_required_tags(&entry, true).is_ok());
    assert!(validate_required_tags(&entry, false).is_err());
    assert!(validate_required_tags(&object("file", 3, true, false), true).is_err());
    let deleted = observation(
        BackendKind::S3,
        "deleted".into(),
        EntryKind::File,
        0,
        [123; 3],
        None,
        None,
        None,
        Some((Some("marker".into()), true, true)),
    );
    assert!(validate_required_tags(&deleted, true).is_ok());
}
