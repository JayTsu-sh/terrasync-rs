#!/usr/bin/env python3
"""只在独占 NFS 前缀和私有 ClickHouse 连接注入目录收尾取消故障。"""
import argparse
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import socketserver
import subprocess
import struct
import tempfile
import threading
import time
import urllib.parse
import uuid

from importlib.util import module_from_spec, spec_from_file_location

ROOT = Path(__file__).resolve().parents[2]


def module(name, path):
    spec = spec_from_file_location(name, path)
    loaded = module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def root_mode_write(rpc, payload):
    """识别本夹具根的 0750 SETATTR；普通文件均为 0640，不靠请求计数猜阶段。"""
    header = rpc._compound_header(payload)
    if header is None or header[0] != 'setattr': return False
    offset = header[1]
    def number():
        nonlocal offset
        value, = struct.unpack_from('>I', payload, offset)
        offset += 4
        return value
    try:
        minor, count = number(), number()
        if (minor, count, number()) != (1, 3, 53): return False
        offset += 32  # SEQUENCE4args
        if number() != 22: return False  # PUTFH
        length = number()
        if length > 128: return False
        offset += (length + 3) // 4 * 4
        if number() != 34: return False  # SETATTR
        offset += 16  # anonymous stateid
        count = number()
        if count != 2: return False
        word0, word1 = number(), number()
        if word0 & ~(1 << 4) or not word1 & (1 << 1): return False
        length = number()
        if offset + length > len(payload): return False
        if word0 & (1 << 4): offset += 8
        return number() == 0o750
    except struct.error:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-metadata", action="store_true")
    parser.add_argument("--source-metadata-failure", action="store_true")
    args = parser.parse_args()
    root_only = args.root_metadata
    source_capture = args.source_metadata_failure
    assert not source_capture or root_only
    ready = module("owned_ready_nfs", ROOT / "tests/lab/run-source-ready-nfs.py")
    lab = module("owned_notification_nfs", ROOT / "tests/lab/run-notification-archive-local.py")
    rpc = module("owned_directory_rpc", Path(os.environ.get("LAB_DATA_MOVER_ROOT", ROOT.parent / "data-mover-rs")) / "tests/lab/nfs_fault_rpc.py")
    run = "nightly-directory-cancel-" + uuid.uuid4().hex[:16]
    remote = f"/srv/nfs/v4/ci/{run}"
    keys = [(deployment, mode) for deployment in ("single", "cluster") for mode in ("full", "incremental")]
    guard = threading.Lock()
    stop = threading.Event()
    active = [None]
    states = {}
    reports = []
    sockets = set()
    services = []
    containers = []
    prepared = False
    try:
        prepared = True
        print(ready.guest(f"""from pathlib import Path
import os
p=Path({remote!r});assert p.parent.is_dir() and not p.exists()
for deployment,mode in {keys!r}:
 for role in ['source','target']:
  root=p/deployment/mode/role;root.mkdir(parents=True);os.chmod(root,0o777)
  if role=='source':
   os.chmod(root,0o750)
   for i in range(3):
    if {root_only!r}: d=root;f=root/('file'+str(i))
    else: d=root/('d'+str(i));d.mkdir();os.chmod(d,0o750);f=d/'payload'
    f.write_bytes((deployment+'/'+mode+'/'+str(i)).encode());os.chmod(f,0o640)
    os.utime(d,ns=(1700000000000000000,1700000000000000000))
   os.utime(root,ns=(1700000000000000000,1700000000000000000))
print('owned directory cancellation NFS prefixes prepared')
"""), flush=True)
        prepared = True
        with tempfile.TemporaryDirectory(prefix="ts-directory-cancel-") as directory:
            selected = dict(zip((8123, 8124, 9000, 9001, 9009, 9010, 9181, 9234), ready.ports(8)))
            for replica in (1, 2):
                config = lab.configuration(replica)
                for old, new in selected.items():
                    config = config.replace(f">{old}<", f">{new}<")
                path = Path(directory) / f"replica-{replica}.xml"
                path.write_text(config)
                containers.append(ready.docker("run", "-d", "--pull", "never", "--name", f"ts-directory-cancel-r{replica}-{uuid.uuid4().hex[:8]}",
                    "--network", "host", "--memory=1536m", "--cpus=2", "--tmpfs", "/var/lib/clickhouse:rw,size=384m,mode=1777",
                    "--tmpfs", "/var/log/clickhouse-server:rw,size=32m,mode=1777", "--label", "codex.task=directory-cancel-nfs",
                    "-e", "CLICKHOUSE_SKIP_USER_SETUP=1", "--mount", f"type=bind,src={path},dst=/etc/clickhouse-server/config.d/notifications.xml,readonly",
                    os.environ.get("NOTIFICATION_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:26.3-alpine")))
            for port in (selected[8123], selected[8124]):
                for _ in range(100):
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                    try:
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

            def inspect(key, recovered):
                path = f"{remote}/{key[0]}/{key[1]}"
                script = f"""from pathlib import Path
import os,json,stat
p=Path({path!r});recovered={recovered!r}
def body(path):
 fd=os.open(path,os.O_RDONLY|os.O_NOATIME)
 try:return os.read(fd,4096)
 finally:os.close(fd)
def file(role,i):
 return p/role/('file'+str(i)) if {root_only!r} else p/role/('d'+str(i))/'payload'
assert all(body(file('target',i))==({(key[0]+'/'+key[1]+'/')!r}+str(i)).encode() for i in range(3))
if recovered:
 assert list((p/'source').iterdir())==[]
 dirs=[p/'target'] if {root_only!r} else [p/'target'/('d'+str(i)) for i in range(3)]
 assert all(d.stat().st_mtime_ns==1700000002123456789 for d in dirs)
 assert all(stat.S_IMODE(d.stat().st_mode)==0o750 for d in dirs)
else:
 assert all(body(file('source',i))==({(key[0]+'/'+key[1]+'/')!r}+str(i)).encode() for i in range(3))
 if not {root_only!r}: assert any((p/'target'/('d'+str(i))).stat().st_mtime_ns!=1700000002123456789 for i in range(3))
print(json.dumps({{'target_payloads':3,'source_payloads':0 if recovered else 3,'recovered':recovered}}))
"""
                return json.loads(ready.guest(script))

            class NfsRelay(socketserver.BaseRequestHandler):
                def handle(self):
                    upstream = socket.create_connection((ready.SERVER, 2049), timeout=3)
                    self.request.settimeout(0.2)
                    upstream.settimeout(0.2)
                    done = threading.Event()
                    source_side = getattr(self.server, "source_side", False)
                    denied_xids = set()
                    with guard:
                        sockets.update((self.request, upstream))

                    def replies():
                        decoder = rpc.RpcRequests()
                        try:
                            while not stop.is_set() and not done.is_set():
                                try:
                                    data = upstream.recv(65536)
                                except socket.timeout:
                                    continue
                                if not data:
                                    break
                                if source_side:
                                    for wire, _, xid, _, payload in decoder.feed(data, include_payload=True):
                                        with guard:
                                            denied = xid in denied_xids
                                            if denied: denied_xids.remove(xid)
                                        self.request.sendall(rpc._deny_acl_reply(payload) if denied else wire)
                                else:
                                    self.request.sendall(data)
                        except OSError:
                            pass
                        finally:
                            done.set()
                    thread = threading.Thread(target=replies, daemon=True)
                    thread.start()
                    decoder = rpc.RpcRequests()
                    try:
                        while not stop.is_set() and not done.is_set():
                            try:
                                data = self.request.recv(65536)
                            except socket.timeout:
                                continue
                            if not data:
                                break
                            for wire, tag, xid, _, payload in decoder.feed(data, include_payload=True):
                                with guard:
                                    state = states.get(active[0])
                                    if source_side and state is not None and tag == 'getacl' and state['acl_denied'] == 0:
                                        state['acl_denied'] += 1
                                        denied_xids.add(xid)
                                    if root_only and not source_side and not source_capture and state is not None and state['fault_at'] is None and root_mode_write(rpc, payload):
                                        state['armed'] = True
                                    suppress = not source_side and not source_capture and state is not None and state['armed'] and tag in ('setattr', 'setacl')
                                    if suppress:
                                        state['suppressed'] += 1
                                        if state['fault_at'] is None:
                                            state['fault_at'] = time.monotonic()
                                            state['database_fault'] = True
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
            nfs = NfsServer(('127.0.0.1', 0), NfsRelay)
            services.append(nfs)
            threading.Thread(target=nfs.serve_forever, daemon=True).start()

            source_nfs = NfsServer(('127.0.0.1', 0), NfsRelay)
            source_nfs.source_side = True
            services.append(source_nfs)
            threading.Thread(target=source_nfs.serve_forever, daemon=True).start()

            class Relay(http.server.BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass

                def empty(self):
                    self.send_response(204)
                    self.send_header('Content-Length', '0')
                    self.end_headers()

                def do_GET(self):
                    action, deployment, mode = self.path.strip('/').split('/')
                    key = (deployment, mode)
                    assert key in keys
                    if action == 'arm':
                        print(ready.guest(f"from pathlib import Path;import os;p=Path({remote!r})/{deployment!r}/{mode!r}/'source';[os.utime(d,ns=(1700000002123456789,1700000002123456789)) for d in ([p] if {root_only!r} else [p/('d'+str(i)) for i in range(3)])]"), flush=True)
                        with guard:
                            active[0] = key
                            states[key] = dict(armed=False, database_fault=False, fault_at=None, stopped_at=None,
                                               suppressed=0, rejected=0, killed=0, released_by=None, recovered=False, acl_denied=0)
                    elif action == 'failed':
                        state = states[key]
                        if source_capture: assert state['acl_denied'] == 1 and state['suppressed'] == 0 and state['rejected'] == 0, state
                        else: assert state['suppressed'] > 0 and state['rejected'] > 0 and state['released_by'] in ('kill', 'terminal_generation'), state
                        result = inspect(key, False)
                        with guard:
                            state['armed'] = False
                            state['database_fault'] = False
                        print(f"independent directory failure key={key} state={state} inspection={result}", flush=True)
                    elif action == 'recovered':
                        result = inspect(key, True)
                        states[key]['recovered'] = True
                        active[0] = None
                        print(f"independent directory recovery key={key} inspection={result}", flush=True)
                    else:
                        raise ValueError(action)
                    self.empty()

                def body(self):
                    if self.headers.get('Transfer-Encoding', '').lower() != 'chunked':
                        return self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    chunks = []
                    while True:
                        count = int(self.rfile.readline().split(b';', 1)[0], 16)
                        if not count:
                            while self.rfile.readline() not in (b'\r\n', b''):
                                pass
                            return b''.join(chunks)
                        chunks.append(self.rfile.read(count))
                        assert self.rfile.read(2) == b'\r\n'

                def do_POST(self):
                    body = self.body()
                    if self.path == '/callback':
                        reports.append(json.loads(body))
                        self.empty()
                        return
                    sql = urllib.parse.unquote_plus(self.path).upper() + body[:4096].decode(errors='replace').upper()
                    with guard:
                        key = active[0]
                        state = states.get(key)
                        if state:
                            own_table = ('BASE_DIRCANCEL_' + '_'.join(key)).upper() in sql
                            if own_table and state['fault_at'] is None and 'SELECT ' in sql and 'IS_DIR = TRUE' in sql and 'COPY_EXECUTION_ID' in sql:
                                state['armed'] = True
                            if 'KILL QUERY' in sql:
                                state['killed'] += 1
                                state['database_fault'] = False
                                state['stopped_at'] = time.monotonic()
                                state['released_by'] = 'kill'
                            # 元数据调用可能先于取消管理请求退出；独立终态记录使用无执行标记的连接。
                            # 只在观测到本 job 已离开执行域的代次收尾请求时恢复数据连接，不依赖固定等待。
                            parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                            terminal = ('GENERATIONS_DIRCANCEL_' + '_'.join(key)).upper() in sql and 'log_comment' not in parameters
                            if state['database_fault'] and terminal:
                                state['database_fault'] = False
                                state['stopped_at'] = time.monotonic()
                                state['released_by'] = 'terminal_generation'
                            reject = state['database_fault'] and 'NOTIFICATIONS_' not in sql
                            if reject:
                                state['rejected'] += 1
                        else:
                            reject = False
                    if reject:
                        self.close_connection = True
                        return
                    connection = http.client.HTTPConnection('127.0.0.1', selected[8123], timeout=15)
                    try:
                        headers = {name: value for name, value in self.headers.items() if name.lower() not in ('host', 'connection', 'content-length', 'transfer-encoding')}
                        headers['Content-Length'] = str(len(body))
                        connection.request('POST', self.path, body=body, headers=headers)
                        response = connection.getresponse()
                        payload = response.read()
                        self.send_response(response.status)
                        for name, value in response.getheaders():
                            if name.lower() not in ('connection', 'content-length', 'transfer-encoding'):
                                self.send_header(name, value)
                        self.send_header('Content-Length', str(len(payload)))
                        self.end_headers()
                        try:
                            self.wfile.write(payload)
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                    finally:
                        connection.close()

            relay = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Relay)
            relay.daemon_threads = True
            services.append(relay)
            threading.Thread(target=relay.serve_forever, daemon=True).start()
            environment = dict(os.environ, LAB_DIRECTORY_CANCEL_RUN=run,
                LAB_DIRECTORY_CANCEL_CONTROL=f'127.0.0.1:{relay.server_port}', LAB_DIRECTORY_CANCEL_NFS_PORT=str(nfs.server_address[1]), LAB_DIRECTORY_CANCEL_SOURCE_PORT=str(source_nfs.server_address[1]))
            subprocess.run(['cargo', 'test', '--offline', '--locked', '-p', 'app', '--lib',
                ('copy_execution::directory_cancel_tests::real_root_metadata_acquisition_failure_copies_data_and_preserves_sources' if source_capture else 'copy_execution::directory_cancel_tests::real_root_metadata_cancellation_keeps_sources_and_recovers' if root_only else 'copy_execution::directory_cancel_tests::real_directory_metadata_cancellation_keeps_sources_and_recovers'),
                '--', '--ignored', '--exact', '--nocapture', '--test-threads=1'], cwd=ROOT, env=environment, check=True, timeout=300)
            assert len(states) == 4 and all(state['recovered'] for state in states.values()), states
            summaries = [r for r in reports if r['is_final'] and not r.get('diagnostics')]
            assert len(summaries) == 10 and all(r['diagnostics_complete'] for r in summaries), summaries
            failures = [r for r in summaries if r['migration']['phase'] == 'failed']
            assert len(failures) == 4
            for report in failures:
                attempt = report['callback_identity']['attempt_id']
                events = {event['event_id']: event for r in reports if r['callback_identity']['attempt_id'] == attempt
                    for event in (r.get('diagnostics') or {}).get('events', [])}
                entries = [e for e in events.values() if e['code'] == 'Copy']
                if root_only:
                    assert not entries, entries
                    phase = 'root_source_metadata_capture' if source_capture else 'root_metadata_finalize'
                    errors = [e for e in events.values() if phase in e['message']]
                    assert len(errors) == 1 and 'backend=Nfs' in errors[0]['message'], events
                    if source_capture: assert 'Acl' in errors[0]['message'] and 'Permission' in errors[0]['message'], events
                    else: assert 'unconfirmed' in errors[0]['message'], events
                else:
                    assert len(entries) == 3 and {e.get('path') for e in entries} == {'d0', 'd1', 'd2'}, entries
                    assert all('directory_metadata_finalize' in e['message'] and 'backend=Nfs' in e['message']
                               and 'unconfirmed' in e['message'] and 'cancel' in e['message'].lower() for e in entries), entries
                assert report['detail']['error_count'] == (0 if root_only else 3) and report['task_error_count'] == 1, report
                pages = [r['diagnostics'] for r in reports if r['callback_identity']['attempt_id'] == attempt and r.get('diagnostics')]
                assert all(page['omitted_count'] == 0 for page in pages), pages
                if report['migration']['plan']['mode'] == 'incremental':
                    assert report['detail']['transferred_bytes'] == 0, report
            print(f'independent callback entry_errors={0 if root_only else 12} task_errors=4 root_metadata={root_only} unknown_write_causes_complete=true no_omissions=true incremental_data_replay=0', flush=True)
            print('independent directory cancellation deployments=2 failed_full=2 failed_incremental=2 bootstrap=2 recovery_full=4 source_retained=true callback_204=true final_reports_saved=true', flush=True)
    finally:
        print('owned directory fault final states=' + json.dumps({str(key): value for key, value in states.items()}, sort_keys=True), flush=True)
        if prepared:
            print(ready.guest(f"from pathlib import Path;import json;p=Path({remote!r});print(json.dumps({{str(f.relative_to(p)):(f.stat().st_atime_ns,f.stat().st_mtime_ns) for f in p.rglob('payload')}}))"), flush=True)
        stop.set()
        with guard:
            streams = list(sockets)
        for stream in streams:
            try:
                stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            stream.close()
        for service in reversed(services):
            service.shutdown()
            service.server_close()
        for container in reversed(containers):
            ready.docker('rm', '-f', '-v', container)
        if prepared:
            print(ready.guest(f"from pathlib import Path;import shutil;p=Path({remote!r});assert p.name.startswith('nightly-directory-cancel-') and p.parent==Path('/srv/nfs/v4/ci');shutil.rmtree(p) if p.exists() else None;print('owned directory cancellation prefix cleaned')"), flush=True)


if __name__ == '__main__':
    main()
