"""
env.py — .env 加载、校验、默认值
所有 e2e skill 的 run.py 通过 import 使用本模块。
"""

import sys
from pathlib import Path

_DEFAULTS = {
    "TERRASYNC_BINARY": "./target/debug/terrasync",
    "TERRASYNC_CONFIG": "examples/config.toml",
    "SSH_USER": "root",
    "NFS_V3_EXPORT": "/export/nfs",
    "NFS_V4_EXPORT": "/export/nfs4",
    "CLICKHOUSE_DATABASE": "default",
    "HARNESS_OUTPUT_DIR": "",
}


def load(env_dict=None):
    """Load config from .env + env_dict override. Returns merged dict."""
    config = dict(_DEFAULTS)

    # 搜索顺序：skill 目录的 .env → harness-run 目录的 .env
    this_file = Path(__file__)
    candidates = [
        this_file.parent.parent.parent / "harness-run" / ".env",  # harness-run/.env
    ]
    # 如果调用方是 skill 下的 run.py，再加 skill/.env
    for candidate in candidates:
        if candidate.exists():
            _parse(candidate, config)
            break

    # 从 S3_*_HOST (ip:port) 派生 S3_*_IP（端口不覆盖，保留脚本默认值 39000）
    for prefix in ("S3_SOURCE", "S3_DEST"):
        host_key = f"{prefix}_HOST"
        ip_key = f"{prefix}_IP"
        if host_key in config and ip_key not in config:
            host = config[host_key]
            ip = host.split(":")[0] if ":" in host else host
            config.setdefault(ip_key, ip)

    if env_dict:
        config.update({k: v for k, v in env_dict.items() if v is not None})
    return config


def require(config, *keys):
    """如缺少必需 key 则打印错误并退出。"""
    missing = [k for k in keys if not config.get(k)]
    if missing:
        sys.exit(
            f"ERROR: missing required env vars: {', '.join(missing)}\n"
            "Copy .env.example to .env and fill in the values."
        )


def _parse(path, out):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k:
                    out[k] = v
