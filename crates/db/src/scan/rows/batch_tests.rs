use super::*;
use data_mover::model::{BackendIdentity, IdentityStrength, SourceIdentity, StoragePath};

fn entry(facts: usize) -> Result<Arc<ObservedEntry>> {
    let backend = BackendIdentity::new(BackendKind::Nfs, "request-packing")?;
    Ok(Arc::new(
        ObservedEntry::new(
            StoragePath::new("file")?,
            EntryKind::File,
            Some(1),
            None,
            SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"identity")?,
        )?
        .with_backend_fact_bytes(vec![17; facts])?,
    ))
}

#[test]
fn a_small_caller_batch_does_not_create_an_insert_request_per_thousand_rows() -> Result<()> {
    let observation = entry(32)?;
    let input = vec![observation; 5_000];
    let requests = NativeWriteBatches::new(&input, 1).collect::<Result<Vec<_>>>()?;
    assert_eq!(requests.len(), 1, "小型调用批次不应被拆成多个独立 HTTP 提交");
    assert_eq!(requests[0].len(), input.len());
    Ok(())
}

#[test]
fn large_facts_still_split_without_losing_rows_or_changing_snapshot_bytes() -> Result<()> {
    let observation = entry(64 * 1024)?;
    let expected = observation.try_encode_snapshot()?;
    let input = vec![observation; 300];
    let requests = NativeWriteBatches::new(&input, 1).collect::<Result<Vec<_>>>()?;
    assert!(requests.len() > 1, "字节预算仍需约束大事实载荷");
    assert_eq!(requests.iter().map(Vec::len).sum::<usize>(), input.len());
    for request in requests {
        assert!(!request.is_empty());
        for row in request {
            assert_eq!(row.entry_snapshot.as_bytes(), expected.as_bytes());
            assert_eq!(row.relative_path, "file");
        }
    }
    Ok(())
}
