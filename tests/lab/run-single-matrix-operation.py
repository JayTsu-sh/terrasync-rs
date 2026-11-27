#!/usr/bin/env python3
"""Bound one matrix operation and reap its workers before releasing the run locks."""

import ctypes
import os
import signal
import subprocess
import sys
import time


PR_SET_CHILD_SUBREAPER = 36


def run(parent, seconds, grace, command):
    # Linux 子进程回收器接管提前退出的操作进程留下的 worker。
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")

    cancelled = False

    def cancel(*_):
        nonlocal cancelled
        cancelled = True

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        # HDFS 的生产阶段清理沿用主分支的信号屏蔽。
        if signal.getsignal(sig) != signal.SIG_IGN:
            signal.signal(sig, cancel)
    if cancelled or os.getppid() != parent:
        return 143

    # 本进程继承锁。操作及其普通 worker 使用单独的进程组，且无需继承锁。
    child = subprocess.Popen(command, start_new_session=True)
    deadline = time.monotonic() + seconds
    forced_status = None
    kill_at = None

    def signal_workers(sig):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            pass

    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            result = child.returncode
            return forced_status or (128 - result if result < 0 else result)
        if pid:
            if pid == child.pid:
                child.returncode = os.waitstatus_to_exitcode(status)
            continue

        now = time.monotonic()
        if forced_status is None:
            if cancelled or os.getppid() != parent:
                forced_status = 143
            elif now >= deadline:
                forced_status = 124
            elif child.returncode is not None:
                forced_status = 1
            if forced_status is not None:
                signal_workers(signal.SIGTERM)
                kill_at = now + grace
        elif now >= kill_at:
            signal_workers(signal.SIGKILL)
            kill_at = float("inf")
            if forced_status == 124:
                forced_status = 137
        time.sleep(0.025)


if __name__ == "__main__":
    parent, duration, grace, *command = sys.argv[1:]
    sys.exit(run(int(parent), int(duration.removesuffix("s")), int(grace), command))
