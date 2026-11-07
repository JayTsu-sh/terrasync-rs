//! 事实键对应目标的版本槽；源版本原值仍保存在标量列和原生 snapshot。
use data_mover::model::VersionRecord;
use task_model::observation::ObservedView;

use super::ClickHouseDatabase;

#[derive(Clone, Copy)]
pub(super) enum CatalogueVersions {
    All,
    Latest,
}

impl CatalogueVersions {
    pub fn select<'a>(self, views: Box<dyn Iterator<Item = ObservedView<'a>> + Send + 'a>) -> CatalogueViews<'a> {
        CatalogueViews { views, versions: self }
    }
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

pub(super) struct CatalogueViews<'a> {
    views: Box<dyn Iterator<Item = ObservedView<'a>> + Send + 'a>,
    versions: CatalogueVersions,
}

impl<'a> Iterator for CatalogueViews<'a> {
    type Item = ObservedView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.views.by_ref().find(|view| self.versions.includes(*view))
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
