//! 共用 SQL 标识符及持久化字段。
use crate::{DatabaseError, Result};

pub(crate) fn identifier(value: &str) -> Result<String> {
    if value.is_empty() || !value.bytes().all(|ch| ch.is_ascii_alphanumeric() || ch == b'_') {
        return Err(DatabaseError::ConfigError(
            "ClickHouse cluster/database identifiers must contain only letters, digits and underscores".into(),
        ));
    }
    Ok(format!("`{value}`"))
}

pub(crate) mod entry;

pub(crate) mod columns;

pub(crate) mod records;

pub(crate) mod identity;
