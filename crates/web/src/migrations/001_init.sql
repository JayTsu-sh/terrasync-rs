-- 端点管理
CREATE TABLE IF NOT EXISTS endpoints (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    endpoint_type TEXT NOT NULL CHECK(endpoint_type IN ('local', 'nfs_v3', 's3', 'cifs')),
    config_json   TEXT NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 迁移路径
CREATE TABLE IF NOT EXISTS migration_paths (
    id          TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    sub_path    TEXT NOT NULL,
    description TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(endpoint_id, sub_path)
);

-- 迁移任务
CREATE TABLE IF NOT EXISTS migration_tasks (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    task_type          TEXT NOT NULL CHECK(task_type IN ('scan', 'sync', 'integrity_check')),
    source_endpoint_id TEXT NOT NULL REFERENCES endpoints(id),
    source_path_id     TEXT REFERENCES migration_paths(id),
    dest_endpoint_id   TEXT REFERENCES endpoints(id),
    dest_path_id       TEXT REFERENCES migration_paths(id),
    config_json        TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    started_at         TEXT,
    finished_at        TEXT,
    error_message      TEXT
);

-- 任务实时进度（ProgressReport 整体存入 report_json）
CREATE TABLE IF NOT EXISTS task_progress (
    task_id     TEXT PRIMARY KEY REFERENCES migration_tasks(id) ON DELETE CASCADE,
    report_json TEXT NOT NULL DEFAULT '{}',
    is_final    INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 任务执行历史
CREATE TABLE IF NOT EXISTS task_executions (
    id             TEXT PRIMARY KEY,
    task_id        TEXT NOT NULL REFERENCES migration_tasks(id) ON DELETE CASCADE,
    execution_type TEXT NOT NULL CHECK(execution_type IN ('full', 'incremental')),
    status         TEXT NOT NULL DEFAULT 'running' CHECK(status IN ('running', 'completed', 'failed', 'cancelled')),
    started_at     TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at    TEXT,
    stats_json     TEXT,
    error_message  TEXT
);
CREATE INDEX IF NOT EXISTS idx_executions_task_id ON task_executions(task_id);

-- 任务执行日志
CREATE TABLE IF NOT EXISTS task_logs (
    id           TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES task_executions(id) ON DELETE CASCADE,
    level        TEXT NOT NULL CHECK(level IN ('info', 'warn', 'error')),
    message      TEXT NOT NULL,
    file_path    TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_logs_execution_id ON task_logs(execution_id);

-- 系统配置持久化
CREATE TABLE IF NOT EXISTS system_config (
    key        TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
