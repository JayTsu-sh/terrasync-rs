"""Run the copy result matrix with an owned NFS 4.1 source and an isolated ClickHouse."""

from contextlib import contextmanager
from importlib.util import module_from_spec, spec_from_file_location
import os
from pathlib import Path
from select import select
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import uuid


def forward(left, right):
    with right:
        peers = {left: right, right: left}
        while peers:
            ready, _, _ = select(peers, [], [], 30)
            if not ready:
                return
            for stream in ready:
                data = stream.recv(65536)
                peer = peers[stream]
                if data:
                    peer.sendall(data)
                else:
                    peer.shutdown(socket.SHUT_WR)
                    del peers[stream]


@contextmanager
def running(server):
    with server:
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join()


@contextmanager
def nfs_tunnel():
    path = os.environ.get("LAB_COPY_RESULT_NFS_SOCKET")
    if path is None:
        yield None
        return

    class GuestRelay(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                target = socket.socket(socket.AF_UNIX)
                with target:
                    target.connect(path)
                    forward(self.request, target)
            except OSError:
                pass

    with running(socketserver.ThreadingTCPServer(("127.0.0.1", 0), GuestRelay)) as server:
        yield server.server_address[1]


def main():
    root = Path(__file__).resolve().parent
    spec = spec_from_file_location("copy_result_nfs_lab", root / "run-source-ready-nfs.py")
    lab = module_from_spec(spec)
    spec.loader.exec_module(lab)
    prefix = f"nightly-copy-result-{uuid.uuid4().hex}"
    remote = f"/srv/nfs/v4/ci/{prefix}"
    lab.guest(f"from pathlib import Path; p=Path({remote!r}); p.mkdir(mode=0o777); p.chmod(0o777)")
    try:
        with tempfile.TemporaryDirectory(prefix="ts-mig-nfs-") as directory:
            view = Path(directory) / "source"
            view.mkdir()
            unix_path = str(Path(directory) / "nfs.sock")

            class HostRelay(socketserver.BaseRequestHandler):
                def handle(self):
                    try:
                        forward(self.request, socket.create_connection((lab.SERVER, 2049), timeout=5))
                    except OSError:
                        pass

            # 内核挂载只供夹具增删改和独立核对。实际迁移使用直接 NFS 4.1 客户端。
            subprocess.run(["mount", "-t", "nfs", "-o", "vers=4.1,proto=tcp,soft,timeo=20,retrans=2,noresvport",
                            f"{lab.SERVER}:{remote}", str(view)], check=True, timeout=20)
            try:
                with running(socketserver.ThreadingUnixStreamServer(unix_path, HostRelay)):
                    environment = dict(os.environ, LAB_COPY_RESULT_NFS_SOCKET=unix_path,
                                       LAB_COPY_RESULT_NFS_VIEW=str(view), LAB_COPY_RESULT_NFS_PREFIX=prefix)
                    subprocess.run([sys.executable, str(root / "run-notification-archive-local.py"),
                                    "--copy-generation-result", *sys.argv[1:]], env=environment, check=True)
            finally:
                subprocess.run(["umount", str(view)], check=True, timeout=15)
    finally:
        lab.guest(f"from pathlib import Path; import shutil; p=Path({remote!r}); "
                  "assert p.name.startswith('nightly-copy-result-'); shutil.rmtree(p)")


if __name__ == "__main__":
    main()
