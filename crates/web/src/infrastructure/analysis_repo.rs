use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

use crate::domain::analysis::{AnalysisMetric, AnalysisNode, AnalysisRepository};
use crate::domain::repository::ConfigRepository;
use crate::error::{Result, WebError};
use crate::infrastructure::db::sanitize_job_id;
use crate::infrastructure::task_runner::{ChConfig, load_ch_config};

pub struct ClickHouseAnalysisRepo {
    config_repo: Arc<dyn ConfigRepository>,
}

impl ClickHouseAnalysisRepo {
    pub fn new(config_repo: Arc<dyn ConfigRepository>) -> Self {
        Self { config_repo }
    }
}

#[async_trait]
impl AnalysisRepository for ClickHouseAnalysisRepo {
    async fn children(
        &self, task_id: &str, path: &str, metric: AnalysisMetric, limit: u16,
    ) -> Result<Vec<AnalysisNode>> {
        let config = load_ch_config(self.config_repo.as_ref())
            .await?
            .ok_or_else(|| WebError::AnalysisUnavailable("请先配置 ClickHouse，再读取扫描明细".to_string()))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(analysis_request_failed)?;
        let cluster_mode = config.cluster.is_some();
        let table = if cluster_mode {
            "base".to_string()
        } else {
            format!("base_{}", sanitize_job_id(task_id))
        };
        // 先读表结构，兼容仅有 legacy 列的旧扫描表。表名由任务 ID 派生，路径单独绑定。
        let columns_sql = "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = {scan_table:String} FORMAT JSONEachRow";
        let columns_body = query(&client, &config, columns_sql, &[("param_scan_table", table.as_str())]).await?;
        let columns: HashSet<String> = columns_body
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<ColumnRow>(line).map(|row| row.name))
            .collect::<std::result::Result<_, _>>()?;
        if columns.is_empty() {
            return Err(WebError::AnalysisUnavailable(
                "当前扫描明细尚未生成或已被清理，请重新执行扫描".to_string(),
            ));
        }
        let observed = ["entry_kind", "size_observed", "modified_unix_nanos", "backend_kind"]
            .iter()
            .all(|name| columns.contains(*name));
        let sql = if cluster_mode {
            children_query_scoped(
                &table,
                observed,
                metric,
                limit,
                "WHERE job_id={job_id:String} AND is_deleted=0",
            )
        } else {
            children_query(&table, observed, metric, limit)
        };
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        let job_id = sanitize_job_id(task_id);
        let body = query(
            &client,
            &config,
            &sql,
            &[("param_prefix", prefix.as_str()), ("param_job_id", job_id.as_str())],
        )
        .await?;
        decode_nodes(&body)
    }
}

#[derive(Deserialize)]
struct ColumnRow {
    name: String,
}

async fn query(client: &Client, config: &ChConfig, sql: &str, parameters: &[(&str, &str)]) -> Result<String> {
    let mut request = client
        .post(config.dsn.trim_end_matches('/'))
        .basic_auth(&config.username, Some(&config.password))
        .header("X-ClickHouse-Database", &config.database)
        .body(sql.to_string())
        .build()
        .map_err(analysis_request_failed)?;
    request
        .url_mut()
        .query_pairs_mut()
        .extend_pairs(parameters.iter().copied())
        .extend_pairs([
            ("output_format_json_quote_64bit_integers", "0"),
            ("max_execution_time", "25"),
            ("max_result_rows", "501"),
            ("max_result_bytes", "8388608"),
        ]);
    let response = client.execute(request).await.map_err(analysis_request_failed)?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        warn!(%status, error = %body, "Storage analysis query failed");
        // ClickHouse 原始错误可能包含路径和内部配置，只在服务端记录。
        return Err(WebError::AnalysisUnavailable(
            "无法读取扫描明细，请检查数据库连接或重新执行扫描".to_string(),
        ));
    }
    response.text().await.map_err(analysis_request_failed)
}

fn analysis_request_failed(source: reqwest::Error) -> WebError {
    WebError::ClickHouseRequestFailed {
        message: "无法读取扫描明细，请检查数据库连接或重新执行扫描",
        source,
    }
}

/// 仅从数据库取 limit + 1 个汇总桶；长尾在数据库内合并，保持容量/数量守恒。
fn children_query(table: &str, observed: bool, metric: AnalysisMetric, limit: u16) -> String {
    children_query_scoped(table, observed, metric, limit, "")
}

fn children_query_scoped(table: &str, observed: bool, metric: AnalysisMetric, limit: u16, scope: &str) -> String {
    let path = if observed {
        r"if(lower(storage_type) = 's3' OR backend_kind = 's3', relative_path, replaceAll(relative_path, '\\', '/'))"
    } else {
        r"if(lower(storage_type) = 's3', relative_path, replaceAll(relative_path, '\\', '/'))"
    };
    let kind = if observed {
        "if(entry_kind != '', entry_kind, if(is_dir, 'directory', if(is_symlink, 'symlink', 'file')))"
    } else {
        "if(is_dir, 'directory', if(is_symlink, 'symlink', 'file'))"
    };
    let bytes = if observed {
        "if(entry_kind = '' OR size_observed, size, toUInt64(0))"
    } else {
        "size"
    };
    let modified = if observed {
        "if(entry_kind != '', toInt64(intDiv(modified_unix_nanos, 1000000000)), if(mtime = toDateTime64(0, 9), NULL, toInt64(toUnixTimestamp(mtime))))"
    } else {
        "if(mtime = toDateTime64(0, 9), NULL, toInt64(toUnixTimestamp(mtime)))"
    };
    let order = match metric {
        AnalysisMetric::Size => "bytes DESC, file_count DESC",
        AnalysisMetric::Count => "file_count DESC, bytes DESC",
    };
    format!(
        r"
WITH normalized AS (
    SELECT {path} AS entry_path, {kind} AS entry_type,
           {bytes} AS logical_bytes, {modified} AS modified_secs
    FROM {table} FINAL {scope}
), scoped AS (
    SELECT *, substring(entry_path, length({{prefix:String}}) + 1) AS suffix
    FROM normalized
    WHERE startsWith(entry_path, {{prefix:String}})
      AND entry_path != {{prefix:String}} AND entry_path != ''
      AND entry_type IN ('file', 'directory')
), grouped AS (
    SELECT concat({{prefix:String}}, splitByChar('/', suffix)[1]) AS child_path,
           splitByChar('/', suffix)[1] AS child_name,
           if(position(suffix, '/') > 0 OR entry_type = 'directory', 'directory', 'file') AS child_kind,
           sumIf(logical_bytes, entry_type = 'file') AS bytes,
           countIf(entry_type = 'file') AS file_count,
           countIf(entry_type = 'directory') AS directory_count,
           max(modified_secs) AS modified_secs
    FROM scoped
    GROUP BY child_path, child_name, child_kind
), ranked AS (
    SELECT *, row_number() OVER (ORDER BY {order}, child_path, child_kind) AS rank
    FROM grouped
)
SELECT if(rank <= {limit}, child_path, '') AS path,
       if(rank <= {limit}, child_name, '其他') AS name,
       if(rank <= {limit}, child_kind, 'other') AS kind,
       sum(bytes) AS bytes, sum(file_count) AS file_count,
       sum(directory_count) AS directory_count, max(modified_secs) AS modified_secs
FROM ranked
GROUP BY path, name, kind
ORDER BY kind = 'other', {order}, path
FORMAT JSONEachRow
"
    )
}

#[derive(Deserialize)]
struct NodeRow {
    path: String,
    name: String,
    kind: String,
    bytes: u64,
    file_count: u64,
    directory_count: u64,
    modified_secs: Option<i64>,
}

fn decode_nodes(body: &str) -> Result<Vec<AnalysisNode>> {
    body.lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let row: NodeRow = serde_json::from_str(line)?;
            Ok(AnalysisNode {
                path: row.path,
                name: row.name,
                kind: row.kind,
                bytes: row.bytes,
                file_count: row.file_count,
                directory_count: row.directory_count,
                modified_at: row
                    .modified_secs
                    .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
                    .map(|time| time.to_rfc3339()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use std::process::Command;

    use super::{children_query, decode_nodes, query};
    use crate::domain::analysis::AnalysisMetric;
    use crate::error::WebError;
    use crate::infrastructure::task_runner::ChConfig;

    #[tokio::test]
    async fn invalid_query_url_preserves_typed_http_failure() {
        let config = ChConfig {
            cluster: None,
            dsn: "http://[invalid-private-host".into(),
            username: "private-user".into(),
            password: "private-password".into(),
            database: "default".into(),
            batch_size: 1_000,
        };
        let result = query(&Client::new(), &config, "SELECT 1", &[]).await;
        assert!(matches!(result, Err(WebError::ClickHouseRequestFailed { .. })));
    }

    #[test]
    fn keeps_bound_path_parameters_and_limits_both_schema_variants() {
        let legacy = children_query("base_safe", false, AnalysisMetric::Size, 120);
        assert!(!legacy.contains("entry_kind"));
        assert!(legacy.contains("startsWith(entry_path, {prefix:String})"));
        assert!(legacy.contains("sumIf(logical_bytes, entry_type = 'file')"));
        assert!(legacy.contains("rank <= 120"));
        let observed = children_query("base_safe", true, AnalysisMetric::Count, 8);
        assert!(observed.contains("size_observed"));
        assert!(observed.contains("ORDER BY file_count DESC, bytes DESC"));
        assert!(observed.contains("GROUP BY path, name, kind"));
    }

    #[test]
    fn decodes_empty_and_unknown_age_without_inventing_dates() {
        assert!(matches!(decode_nodes(""), Ok(nodes) if nodes.is_empty()));
        let body = r#"{"path":"a","name":"a","kind":"directory","bytes":100,"file_count":2,"directory_count":1,"modified_secs":null}
{"path":"","name":"其他","kind":"other","bytes":20,"file_count":4,"directory_count":0,"modified_secs":1700000000}"#;
        let nodes = decode_nodes(body).unwrap_or_default();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes.iter().map(|node| node.bytes).sum::<u64>(), 120);
        assert_eq!(nodes.iter().map(|node| node.file_count).sum::<u64>(), 6);
        assert_eq!(nodes[0].modified_at, None);
        assert_eq!(nodes[1].modified_at.as_deref(), Some("2023-11-14T22:13:20+00:00"));
    }

    /// 可将官方 clickhouse local 或测试包装脚本放在 CLICKHOUSE_LOCAL 指定的位置。
    #[test]
    #[ignore = "requires CLICKHOUSE_LOCAL executable; runs real ClickHouse SQL with synthetic data"]
    #[allow(clippy::unwrap_used)]
    fn clickhouse_aggregates_real_paths_and_conserves_overflow() {
        let executable = std::env::var("CLICKHOUSE_LOCAL").unwrap();
        let fixture = r"
CREATE TABLE base_fixture (
    relative_path String, size UInt64, storage_type String DEFAULT 'local',
    is_dir Bool DEFAULT 0, is_symlink Bool DEFAULT 0, mtime DateTime64(9) DEFAULT 0,
    entry_kind String DEFAULT '', size_observed Bool DEFAULT 0,
    modified_unix_nanos Nullable(Int128) DEFAULT NULL, backend_kind String DEFAULT ''
) ENGINE = ReplacingMergeTree ORDER BY relative_path;
INSERT INTO base_fixture (relative_path,size,is_dir) VALUES
    ('alpha',9999,1),('alpha/large.bin',100,0),('alpha/large.bin',200,0),
    ('alpha/zero',0,0),('alpha/nested',8888,1),('alpha/nested/tiny',3,0),
    ('alpha2',9999,1),('alpha2/big',900,0),('root.txt',50,0),('空間/資料.txt',7,0),
    ('win\\nested\\file.txt',9,0);
INSERT INTO base_fixture (relative_path,size,storage_type) VALUES ('obj/a/key',11,'s3');
INSERT INTO base_fixture (relative_path,size,is_symlink) VALUES ('link',50,1);
";
        for observed in [false, true] {
            for (prefix, metric, bytes, files) in [
                ("", AnalysisMetric::Size, 1180, 8),
                ("alpha/", AnalysisMetric::Size, 203, 3),
                ("", AnalysisMetric::Count, 1180, 8),
                ("空間/", AnalysisMetric::Size, 7, 1),
                ("win/", AnalysisMetric::Size, 9, 1),
                ("obj/", AnalysisMetric::Size, 11, 1),
            ] {
                let sql = children_query("base_fixture", observed, metric, 2);
                let output = Command::new(&executable)
                    .args([
                        "--multiquery",
                        "--query",
                        &format!("{fixture}{sql}"),
                        "--param_prefix",
                        prefix,
                        "--output_format_json_quote_64bit_integers",
                        "0",
                    ])
                    .env_remove("LD_LIBRARY_PATH")
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "status={:?}, stderr={}, stdout={}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr),
                    String::from_utf8_lossy(&output.stdout)
                );
                let nodes = decode_nodes(&String::from_utf8(output.stdout).unwrap()).unwrap();
                assert!(nodes.len() <= 3);
                assert_eq!(nodes.iter().map(|node| node.bytes).sum::<u64>(), bytes);
                assert_eq!(nodes.iter().map(|node| node.file_count).sum::<u64>(), files);
                if prefix.is_empty() {
                    assert!(nodes.iter().any(|node| node.kind == "other" && node.path.is_empty()));
                }
                if prefix == "alpha/" {
                    assert!(
                        nodes
                            .iter()
                            .all(|node| node.path.starts_with(prefix) || node.kind == "other")
                    );
                }
            }
        }
        // 中立投影不依赖 legacy 的 is_dir/mtime：未知 size 不能把默认数值当成已观测容量。
        let schema = fixture.split("INSERT INTO").next().unwrap();
        let neutral_fixture = r"
INSERT INTO base_fixture (relative_path,size,entry_kind,size_observed,modified_unix_nanos,backend_kind) VALUES
    ('neutral',99999,'directory',1,NULL,'s3'),
    ('neutral/file.bin',17,'file',1,1700000000000000000,'s3'),
    ('neutral/unknown.bin',999,'file',0,NULL,'s3'),
    ('neutral/nested',8888,'directory',1,NULL,'s3'),
    ('neutral/nested/child.bin',3,'file',1,1700000000000000000,'s3'),
    ('neutral/link',900,'symlink',1,NULL,'s3');
";
        let sql = children_query("base_fixture", true, AnalysisMetric::Size, 120);
        let output = Command::new(&executable)
            .args([
                "--multiquery",
                "--query",
                &format!("{schema}{neutral_fixture}{sql}"),
                "--param_prefix",
                "neutral/",
                "--output_format_json_quote_64bit_integers",
                "0",
            ])
            .env_remove("LD_LIBRARY_PATH")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "status={:?}, {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let nodes = decode_nodes(&String::from_utf8(output.stdout).unwrap()).unwrap();
        assert_eq!(nodes.iter().map(|node| node.bytes).sum::<u64>(), 20);
        assert_eq!(nodes.iter().map(|node| node.file_count).sum::<u64>(), 3);
        assert_eq!(nodes.iter().map(|node| node.directory_count).sum::<u64>(), 1);
        let unknown = nodes.iter().find(|node| node.path == "neutral/unknown.bin").unwrap();
        assert_eq!(unknown.bytes, 0);
        assert_eq!(unknown.modified_at, None);
        let file = nodes.iter().find(|node| node.path == "neutral/file.bin").unwrap();
        assert_eq!(file.modified_at.as_deref(), Some("2023-11-14T22:13:20+00:00"));
    }
}
