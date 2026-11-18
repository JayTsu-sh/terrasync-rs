//! 事实键对应目标的版本槽；源版本原值仍保存在标量列和原生 snapshot。
use std::sync::Arc;

use data_mover::model::{ObservedEntry, VersionRecord};
use task_model::CopyCompletion;
use task_model::observation::{ObservedItem, ObservedView, ObservedViews};

use super::ClickHouseDatabase;

#[derive(Clone, Copy)]
pub(super) enum CatalogueVersions {
    All,
    Latest,
}

impl CatalogueVersions {
    pub fn includes(self, view: ObservedView<'_>) -> bool {
        matches!(self, Self::All) || view.version().is_none_or(VersionRecord::is_latest)
    }

    pub fn key(self, view: ObservedView<'_>) -> &str {
        match self {
            Self::All => view.version_id(),
            Self::Latest => "",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum CatalogueInputs<'a> {
    Observed(&'a [Arc<ObservedEntry>]),
    Selected(&'a [ObservedItem]),
    Copied(&'a [CopyCompletion]),
}

pub(super) struct CatalogueViews<'a> {
    inputs: CatalogueInputs<'a>,
    versions: CatalogueVersions,
    position: usize,
    current: Option<ObservedViews<'a>>,
}
impl<'a> CatalogueViews<'a> {
    pub(super) fn new(inputs: CatalogueInputs<'a>, versions: CatalogueVersions) -> Self {
        Self {
            inputs,
            versions,
            position: 0,
            current: None,
        }
    }
}
impl<'a> Iterator for CatalogueViews<'a> {
    type Item = (ObservedView<'a>, Option<&'a CopyCompletion>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = match &mut self.inputs {
                CatalogueInputs::Observed(records) => loop {
                    if let Some(view) = self.current.as_mut().and_then(Iterator::next) {
                        break (view, None);
                    }
                    let entry = records.get(self.position)?;
                    self.position += 1;
                    self.current = Some(ObservedView::all(entry));
                },
                CatalogueInputs::Selected(records) => {
                    let item = records.get(self.position)?;
                    self.position += 1;
                    (item.view(), None)
                }
                CatalogueInputs::Copied(records) => {
                    let completion = records.get(self.position)?;
                    self.position += 1;
                    (completion.item.view(), Some(completion))
                }
            };
            if self.versions.includes(item.0) {
                return Some(item);
            }
        }
    }
}

impl ClickHouseDatabase {
    pub(super) async fn catalogue_versions(&self) -> CatalogueVersions {
        let lifecycle = self.generation_lifecycle.lock().await;
        if lifecycle
            .copy_generation
            .as_ref()
            .is_some_and(|generation| !generation.request.binding.target_history)
        {
            CatalogueVersions::Latest
        } else {
            CatalogueVersions::All
        }
    }
}
