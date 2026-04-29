#!/usr/bin/env python3
"""CIFS e2e-test-cifs-incremental-sync e2e 测试 — 待 Samba 配置后激活。"""
import subprocess, sys, time
from pathlib import Path
_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult

def run(env=None):
    start = time.monotonic()
    r = subprocess.run(["smbclient", "--version"], capture_output=True)
    if r.returncode != 0:
        return {"passed": False, "metrics": {"elapsed_sec": 0},
                "assertions": [{"name": "smbclient", "passed": False,
                                "message": "✗ smbclient not found — configure Samba on 192.168.50.173/192.168.50.23 first"}]}
    # TODO: implement full test once Samba is configured
    return {"passed": False, "metrics": {"elapsed_sec": 0},
            "assertions": [{"name": "cifs_not_configured", "passed": False,
                            "message": "✗ CIFS Samba not configured — implement after Samba setup"}]}

if __name__ == "__main__":
    result = run()
    for a in result["assertions"]: print(a["message"])
    sys.exit(0 if result["passed"] else 1)
