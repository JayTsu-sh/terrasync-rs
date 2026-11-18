#!/usr/bin/env python3
"""Exercise NFS ready-window cleanup on owned prefixes and private localhost services.

Uses the existing VM 104 exports without changing exports, services or firewalls.
ClickHouse containers bind fresh localhost-only ports. The NFS relay affects only
the test's v4.1 source connections and suppresses REMOVE for the combined-failure case.
"""
import http.client
import http.server
import importlib.util
import json
import os
from pathlib import Path
import random
import shlex
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import urllib.parse
import uuid


ROOT = Path(__file__).resolve().parents[2]
SSH = ["ssh", "-i", os.environ.get("LAB_PVE_SSH_KEY", "/root/.ssh/id_ed25519_proxmox"),
       "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "root@10.131.9.20"]
SERVER = "10.10.1.13"
BODY = b"ready data"


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def guest(script):
    command = shlex.join(["qm", "guest", "exec", "104", "--timeout", "20", "--", "python3", "-c", script])
    output = subprocess.check_output(SSH + [command], text=True, timeout=30)
    result = json.loads(output)
    if not result.get("exited") or result.get("exitcode"):
        raise RuntimeError(result)
    return result.get("out-data", "")


def docker(*arguments):
    return subprocess.check_output(["timeout", "30", "docker", *arguments], text=True).strip()


def ports(count):
    chosen = []
    while len(chosen) < count:
        port = random.randrange(20000, 30000)
        if port in chosen:
            continue
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
        chosen.append(port)
    return chosen


def main():
    lab = module("owned_notification_lab", ROOT / "tests/lab/run-notification-archive-local.py")
    rpc = module("owned_nfs_rpc", Path(os.environ.get("LAB_DATA_MOVER_ROOT", ROOT.parent / "data-mover-rs")) / "tests/lab/nfs_fault_rpc.py")
    run = "nightly-ready-window-" + uuid.uuid4().hex[:16]
    assert run.startswith("nightly-ready-window-") and run.replace("-", "").isalnum()
    keys = [(deployment, dialect, name) for deployment in ("single", "cluster")
            for dialect in ("v3", "v4") for name in (("quick", "full", "dual") if dialect == "v4" else ("quick", "full"))]
    prepared = False
    containers = []
    stopped = threading.Event()
    guard = threading.Lock()
    active = [None]
    states = {}
    reports = []
    sockets = set()
    services = []
    export_roots = [f"/srv/nfs/{dialect}/ci/{run}" for dialect in ("v3", "v4")]
    try:
        script = f"""from pathlib import Path
import os
roots={export_roots!r}; keys={keys!r}
assert all(Path(p).parent.is_dir() and not Path(p).exists() for p in roots)
for p in roots: Path(p).mkdir(mode=0o777); os.chmod(p,0o777)
for deployment,dialect,name in keys:
 p=Path('/srv/nfs')/dialect/'ci'/{run!r}/deployment/(dialect+'_'+name)
 for role in ['source','target']:
  q=p/role; q.mkdir(parents=True); os.chmod(q,0o777)
 for name in ['a','b']:
  q=p/'source'/name; q.write_bytes({BODY!r}); q.chmod(0o644)
print('owned NFS prefixes prepared')
"""
        print(guest(script), flush=True)
        prepared = True
        with tempfile.TemporaryDirectory(prefix="ts-nfs-ready-") as directory:
            selected = dict(zip((8123, 8124, 9000, 9001, 9009, 9010, 9181, 9234), ports(8)))
            for replica in (1, 2):
                content = lab.configuration(replica)
                for old, new in selected.items():
                    content = content.replace(f">{old}<", f">{new}<")
                config = Path(directory) / f"replica-{replica}.xml"
                config.write_text(content)
                container = docker("run", "-d", "--pull", "never", "--name", f"ts-nfs-ready-r{replica}-{uuid.uuid4().hex[:10]}",
                                   "--network", "host", "--memory=1536m", "--cpus=2",
                                   "--tmpfs", "/var/lib/clickhouse:rw,size=384m,mode=1777",
                                   "--tmpfs", "/var/log/clickhouse-server:rw,size=32m,mode=1777",
                                   "--label", "codex.task=source-ready-nfs", "-e", "CLICKHOUSE_SKIP_USER_SETUP=1",
                                   "--mount", f"type=bind,src={config},dst=/etc/clickhouse-server/config.d/notifications.xml,readonly",
                                   os.environ.get("NOTIFICATION_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:26.3-alpine"))
                containers.append(container)
            for port in (selected[8123], selected[8124]):
                for _ in range(100):
                    try:
                        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                        connection.request("GET", "/ping")
                        response = connection.getresponse()
                        if response.status == 200:
                            break
                    except OSError:
                        pass
                    finally:
                        connection.close()
                    time.sleep(0.2)
                else:
                    raise RuntimeError("owned ClickHouse readiness timeout")

            class NfsRelay(socketserver.BaseRequestHandler):
                def handle(self):
                    upstream = socket.create_connection((SERVER, 2049), timeout=3)
                    with guard:
                        sockets.update((self.request, upstream))
                    self.request.settimeout(0.2)
                    upstream.settimeout(0.2)
                    done = threading.Event()

                    def replies():
                        try:
                            while not done.is_set() and not stopped.is_set():
                                try:
                                    data = upstream.recv(65536)
                                except socket.timeout:
                                    continue
                                if not data:
                                    break
                                self.request.sendall(data)
                        except OSError:
                            pass
                        finally:
                            done.set()

                    thread = threading.Thread(target=replies, daemon=True)
                    thread.start()
                    decoder = rpc.RpcRequests()
                    try:
                        while not done.is_set() and not stopped.is_set():
                            try:
                                data = self.request.recv(65536)
                            except socket.timeout:
                                continue
                            if not data:
                                break
                            for wire, tag, xid, message in decoder.feed(data):
                                with guard:
                                    key = active[0]
                                    suppress = key is not None and key[2] == "dual" and tag == "remove" and message == 0
                                    if suppress:
                                        states[key]["remove_seen"].set()
                                        states[key]["suppressed"] += 1
                                if not suppress:
                                    upstream.sendall(wire)
                    except OSError:
                        pass
                    finally:
                        done.set()
                        for stream in (self.request, upstream):
                            try:
                                stream.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                            stream.close()
                        thread.join(timeout=1)
                        with guard:
                            sockets.difference_update((self.request, upstream))

            class NfsServer(socketserver.ThreadingTCPServer):
                daemon_threads = True

            nfs = NfsServer(("127.0.0.1", 0), NfsRelay)
            services.append(nfs)
            threading.Thread(target=nfs.serve_forever, daemon=True).start()

            def inspect(key, bodies=False):
                deployment, dialect, name = key
                path = f"/srv/nfs/{dialect}/ci/{run}/{deployment}/{dialect}_{name}"
                script = f"""from pathlib import Path
import json
p=Path({path!r})
result={{'source':[n for n in ['a','b'] if (p/'source'/n).exists()]}}
if {bodies!r}:
 result['target_ok']=all((p/'target'/n).read_bytes()=={BODY!r} for n in ['a','b'])
 result['metadata_ok']=all((p/'target'/n).stat().st_mode&0o7777==0o644 for n in ['a','b'])
print(json.dumps(result))
"""
                return json.loads(guest(script))

            class HttpRelay(http.server.BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass

                def do_GET(self):
                    parts = self.path.strip("/").split("/")
                    assert len(parts) == 4 and parts[0] in ("arm", "finish"), parts
                    key = tuple(parts[1:])
                    if parts[0] == "arm":
                        with guard:
                            active[0] = key
                            states[key] = {"released": False, "suppressed": 0, "remove_seen": threading.Event(), "error": None, "observed": False}
                    else:
                        state = states[key]
                        assert state["error"] is None and state["observed"] and state["released"], state
                        result = inspect(key, True)
                        assert result["target_ok"] and result["metadata_ok"], result
                        assert result["source"] == (["a", "b"] if key[2] == "dual" else []), result
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()

                def read_body(self):
                    if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                        return self.rfile.read(int(self.headers.get("Content-Length", "0")))
                    chunks = []
                    while True:
                        count = int(self.rfile.readline().split(b";", 1)[0], 16)
                        if not count:
                            while self.rfile.readline() not in (b"\r\n", b""):
                                pass
                            return b"".join(chunks)
                        chunks.append(self.rfile.read(count))
                        assert self.rfile.read(2) == b"\r\n"

                def do_POST(self):
                    body = self.read_body()
                    if self.path == "/callback":
                        reports.append(json.loads(body))
                        self.send_response(204)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    sql = urllib.parse.unquote_plus(self.path).upper()
                    with guard:
                        key = active[0]
                        state = states.get(key)
                        own = key is not None and ("BASE_NFREADY_" + "_".join(key)).upper() in sql
                        held = own and "INSERT INTO" in sql and "FORMAT ROWBINARY" in sql and not state["released"]
                    if held:
                        try:
                            if key[2] == "dual":
                                assert state["remove_seen"].wait(8), "source REMOVE must begin before fact receipt"
                                result = inspect(key, True)
                                assert result["source"] == ["a", "b"] and result["target_ok"], result
                            else:
                                deadline = time.monotonic() + 8
                                while inspect(key)["source"]:
                                    assert time.monotonic() < deadline, "confirmed sources must be absent before fact receipt"
                                result = inspect(key, True)
                                assert result["target_ok"] and result["metadata_ok"], result
                            state["observed"] = True
                            print(f"independent NFS before-receipt key={key} source={result['source']} targets_ready=true fact_forwarded=false suppressed={state['suppressed']}", flush=True)
                        except BaseException as error:
                            state["error"] = repr(error)
                            self.send_error(500, "owned NFS readiness proof failed")
                            return
                        finally:
                            state["released"] = True
                        if key[2] == "dual":
                            self.send_error(403, "owned NFS fact rejection")
                            return
                    connection = http.client.HTTPConnection("127.0.0.1", selected[8123], timeout=15)
                    try:
                        headers = {name: value for name, value in self.headers.items()
                                   if name.lower() not in ("host", "connection", "content-length", "transfer-encoding")}
                        headers["Content-Length"] = str(len(body))
                        connection.request("POST", self.path, body=body, headers=headers)
                        response = connection.getresponse()
                        payload = response.read()
                        self.send_response(response.status)
                        for name, value in response.getheaders():
                            if name.lower() not in ("connection", "content-length", "transfer-encoding"):
                                self.send_header(name, value)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                    finally:
                        connection.close()

            relay = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HttpRelay)
            relay.daemon_threads = True
            services.append(relay)
            threading.Thread(target=relay.serve_forever, daemon=True).start()
            address = f"127.0.0.1:{relay.server_port}"
            environment = dict(os.environ, LAB_READY_NFS_RUN=run, LAB_READY_NFS_CONTROL=address,
                               LAB_READY_NFS_CALLBACK=f"http://{address}/callback", LAB_READY_NFS_DSN=f"http://{address}",
                               LAB_READY_NFS_PROXY_PORT=str(nfs.server_address[1]))
            subprocess.run(["cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                            "copy_execution::ready_cleanup_nfs_tests::real_nfs_full_move_ready_window_and_combined_failure",
                            "--", "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                           cwd=ROOT, env=environment, check=True, timeout=300)
            assert len(states) == 10 and all(state["observed"] and state["error"] is None for state in states.values()), states
            summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
            assert len(summaries) == 10 and all(report["diagnostics_complete"] for report in summaries), summaries
            failed = [report for report in summaries if report["migration"]["phase"] == "failed"]
            assert len(failed) == 2, failed
            events = {event["event_id"]: event for report in reports
                      for event in (report.get("diagnostics") or {}).get("events", [])}
            unknown = [event for event in events.values() if event.get("retention", {}).get("reason") == "source_deletion_unconfirmed"]
            assert len(unknown) == 2 and all(event["retention"]["source_presence"] == "unknown" and event["path"] in ("a", "b") for event in unknown), unknown
            assert sum("owned NFS fact rejection" in event["message"] for event in events.values()) >= 2, events
            print("independent NFS ready-window deployments=2 protocols=v3,v4.1 normal_moves=8 combined_failure=2 source_unknown_and_database_causes_delivered=true final_reports_saved=true", flush=True)
    finally:
        stopped.set()
        with guard:
            for stream in list(sockets):
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                stream.close()
        for service in reversed(services):
            service.shutdown()
            service.server_close()
        for container in reversed(containers):
            docker("rm", "-f", "-v", container)
        if prepared:
            print(guest(f"import shutil; from pathlib import Path; roots={export_roots!r}; [shutil.rmtree(p) for p in roots if Path(p).exists()]; print('owned NFS prefixes cleaned')"), flush=True)


if __name__ == "__main__":
    main()
