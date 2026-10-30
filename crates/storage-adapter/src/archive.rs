use crate::connected::path;
use crate::streaming::source_qos;
use crate::*;
use data_mover::tar_pack::{self, TarRequest};
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};
impl StorageEnum {
    pub async fn pack_files_to_tar(
        source: &Self, destination: &Self, entries: &[Arc<EntryEnum>], final_path: &Path, _mtime: i64,
        options: TarPackOptions,
    ) -> Result<u64> {
        let mut observations = Vec::with_capacity(entries.len());
        for entry in entries {
            observations.push(source.observe(entry.get_relative_path()).await?);
        }
        let request = TarRequest {
            source: source.roles.clone(),
            destination: destination.roles.clone(),
            entries: observations,
            final_path: path(final_path)?,
            cancel: Default::default(),
            source_qos: options
                .qos
                .as_ref()
                .map(|q| source_qos(q, source.block_size))
                .transpose()?,
        };
        let outcome = tar_pack::pack(request).await?;
        if let Some(counter) = options.bytes_counter {
            counter.fetch_add(outcome.transferred_bytes, Ordering::Relaxed);
        }
        Ok(outcome.transferred_bytes)
    }
}
