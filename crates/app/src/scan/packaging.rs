use super::source::WalkOptions;
use chrono::NaiveDate;
use data_mover::DslTraversalFilter;
use data_mover::model::{EntryKind, MetadataObservations, MetadataRequirements};
use data_mover::traversal::{TraversalCandidate, TraversalDecision, TraversalFilter};
/// 打包选择属于产品层；遍历仍由上游按页执行，在选中目录处停止下钻。
#[derive(Debug)]
pub(super) struct PackageFilter {
    dsl: DslTraversalFilter,
    enabled: bool,
    extra_depth: usize,
    maximum: Option<usize>,
}
impl PackageFilter {
    pub(super) fn new(options: &WalkOptions) -> Self {
        Self {
            dsl: DslTraversalFilter::new(options.match_expressions.clone(), options.exclude_expressions.clone()),
            enabled: options.packaged && options.match_expressions.is_some(),
            extra_depth: options.package_depth,
            maximum: options.depth.filter(|v| *v > 0),
        }
    }
    fn target_depth(&self, value: &str, is_dir: bool) -> Option<usize> {
        if !self.enabled {
            return None;
        }
        let parts: Vec<_> = value.split('/').filter(|v| !v.is_empty()).collect();
        let directories = parts.len().saturating_sub(usize::from(!is_dir));
        for index in 0..directories {
            if !date_name(parts[index]) {
                continue;
            }
            let ancestor = parts[..=index].join("/");
            let metadata = MetadataObservations::default();
            let candidate = TraversalCandidate {
                path: &ancestor,
                name: parts[index],
                kind: EntryKind::Directory,
                size: None,
                modified: None,
                metadata: &metadata,
            };
            if self.dsl.decide(&candidate).emit {
                let target = index + 1 + self.extra_depth;
                return Some(target);
            }
        }
        None
    }
    pub(super) fn packaged_path(&self, value: &str) -> bool {
        self.target_depth(value, true) == Some(value.split('/').filter(|v| !v.is_empty()).count())
    }
}
impl TraversalFilter for PackageFilter {
    fn resume_key(&self) -> Option<Vec<u8>> {
        let mut key = b"terrasync-package-filter-v1\0".to_vec();
        key.extend(self.dsl.resume_key()?);
        key.push(u8::from(self.enabled));
        key.extend_from_slice(&(self.extra_depth as u64).to_le_bytes());
        key.push(u8::from(self.maximum.is_some()));
        key.extend_from_slice(&(self.maximum.unwrap_or_default() as u64).to_le_bytes());
        Some(key)
    }
    fn required_metadata(&self) -> MetadataRequirements {
        self.dsl.required_metadata()
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let mut decision = self.dsl.decide(candidate);
        let is_dir = candidate.kind == EntryKind::Directory;
        if let Some(target) = self.target_depth(candidate.path, is_dir) {
            let depth = candidate.path.split('/').filter(|v| !v.is_empty()).count();
            if self.maximum.is_some_and(|maximum| target > maximum) {
                decision.emit = false;
                decision.descend = false;
            } else if depth == target && is_dir {
                decision.descend = false;
            } else {
                decision.emit = false;
            }
        }
        // 必须在每层评估打包深度；普通 DSL 可以提前放行整个子树。
        decision.filter_children = true;
        decision
    }
}
fn date_name(name: &str) -> bool {
    name.char_indices().any(|(start, _)| {
        [(10, "%Y-%m-%d"), (8, "%Y%m%d"), (6, "%y%m%d")]
            .into_iter()
            .any(|(length, format)| {
                name.get(start..start + length)
                    .is_some_and(|value| NaiveDate::parse_from_str(value, format).is_ok())
            })
    })
}
