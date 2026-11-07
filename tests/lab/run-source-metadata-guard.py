#!/usr/bin/env python3
"""Run against a prefix owned by a prepare-run.sh / cleanup-run.sh lifecycle.

The TCP relay only affects this test's NFS connections. It changes no server,
firewall, SSH configuration or unrelated export content.
"""
import http.server
import os
from pathlib import Path
import selectors
import socket
import socketserver
import subprocess
import threading


def run():
    run_id = os.environ["LAB_SOURCE_GUARD_RUN_ID"]
    if not run_id.startswith("nightly-source-metadata-guard-") or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for char in run_id):
        raise ValueError("owned prepared source guard prefix required")
    state = {"mode": "normal", "sockets": set(), "drops": 0}
    lock = threading.Lock()
    stopped = threading.Event()

    def disconnect():
        with lock:
            sockets = list(state["sockets"])
        for stream in sockets:
            try:
                stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    class Relay(socketserver.BaseRequestHandler):
        def handle(self):
            upstream = None
            try:
                upstream = socket.create_connection((os.environ.get("LAB_SOURCE_DATA", "10.10.1.12"), 2049), timeout=5)
                upstream.settimeout(None)
                with lock:
                    state["sockets"].update((self.request, upstream))
                with selectors.DefaultSelector() as ready:
                    ready.register(self.request, selectors.EVENT_READ, upstream)
                    ready.register(upstream, selectors.EVENT_READ, self.request)
                    while not stopped.is_set():
                        for selected, _ in ready.select(0.1):
                            data = selected.fileobj.recv(65536)
                            if not data:
                                return
                            with lock:
                                mode = state["mode"]
                                if mode == "flash":
                                    state["mode"] = "normal"
                                    state["drops"] += 1
                            if mode in ("disconnect", "flash"):
                                return
                            if mode in ("stall", "cancel"):
                                continue
                            selected.data.sendall(data)
            except (OSError, ConnectionError):
                pass
            finally:
                with lock:
                    state["sockets"].discard(self.request)
                    state["sockets"].discard(upstream)
                if upstream:
                    upstream.close()

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            mode = self.path.lstrip("/")
            if mode not in ("normal", "flash", "disconnect", "stall", "cancel"):
                self.send_error(400)
                return
            with lock:
                previous = state["mode"]
                state["mode"] = mode
            if mode == "disconnect" or mode == "normal" and previous in ("disconnect", "stall", "cancel"):
                disconnect()
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True
        allow_reuse_address = True

    relay = Server(("127.0.0.1", 0), Relay)
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Control)
    control.daemon_threads = True
    for server in (relay, control):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    environment = dict(os.environ,
        LAB_SOURCE_GUARD_SERVER_ROOT=f"/srv/nfs/v4/ci/{run_id}",
        LAB_SOURCE_GUARD_NFS_URL=f"nfs://127.0.0.1:{relay.server_address[1]}/srv/nfs/v4:/ci/{run_id}?version=4.1&noresvport=true&uid=1000&gid=1000",
        LAB_SOURCE_GUARD_CONTROL=f"127.0.0.1:{control.server_port}")
    try:
        subprocess.run(["cargo", "test", "-p", "app", "--lib", "source_cleanup::real_tests::", "--locked", "--", "--ignored", "--nocapture", "--test-threads=1"],
            cwd=Path(__file__).resolve().parents[2], env=environment, check=True, timeout=240)
        assert state["drops"] >= 1, "flash was not injected"
        print("independent NFS relay flash_injected=true fault_scope=owned_connections server_configuration_unchanged=true", flush=True)
    finally:
        stopped.set()
        disconnect()
        for server in (control, relay):
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    run()
