#!/usr/bin/env python3
"""真实跨主机 QUIC Direct 取消：接收／原生校验中断、业务失败、资源释放及同目标重试。

先构建 terrasync release 和 app 的 direct_cancel_real 测试二进制。所有远端数据均在
prepare-run/cleanup-run 管理的独立运行目录，原始日志和凭据不进入源码仓库。
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shlex
import shutil
import subprocess
import time


WORKER = r'''
import base64, hashlib, json, os, pathlib, socket, subprocess, sys, time
c = json.loads(sys.argv[1])
case = pathlib.Path(c['case'])
case.mkdir(parents=True, exist_ok=True)
(case / 'worker.pid').write_text(str(os.getpid()))
target = case / 'target'
target.mkdir(parents=True, exist_ok=True)
(case / 'tmp').mkdir(exist_ok=True)
(case / 'receiver').mkdir(exist_ok=True)
file = target / 'file'
def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()
if c['reset']:
    with file.open('wb') as stream: stream.truncate(c['size'])
    file.chmod(0o600)
    os.utime(file, ns=(1670000000000000000, 1670000000000000000))
before = file.stat()
old_hash = digest(file)
config = case / 'config.toml'
config.write_text('[database]\nenabled = false\n[scan]\nconcurrency = 1\ninclude_tags = false\n')
cert = case / ('server-' + c['phase'] + '.crt')
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
    probe.bind((c['data_address'], 0))
    port = probe.getsockname()[1]
env = {k: v for k, v in os.environ.items() if not k.lower().endswith('_proxy')}
env.update(TMPDIR=str(case / 'tmp'), CARGO_MANIFEST_DIR=str(case / 'receiver' / 'fake'))
log_path = case / ('receiver-' + c['phase'] + '.log')
with log_path.open('w') as log:
    process = subprocess.Popen([c['binary'], '-c', str(config), 'serve', '--listen',
        c['data_address'] + ':' + str(port), '--tls-cert-out', str(cert), '--token',
        c['token'], str(target)], cwd=case / 'receiver', env=env, stdout=log, stderr=log)
    (case / 'receiver.pid').write_text(str(process.pid))
    try:
        deadline = time.monotonic() + 240
        while not cert.exists():
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('Receiver did not become ready')
            time.sleep(.01)
        print(json.dumps({'ready': True, 'address': c['data_address'] + ':' + str(port),
            'cert': base64.b64encode(cert.read_bytes()).decode()}), flush=True)
        injected = None
        position = None
        while process.poll() is None:
            if time.monotonic() > deadline: raise RuntimeError('Receiver deadline exceeded')
            if c['phase'] != 'control' and injected is None:
                for fd in pathlib.Path('/proc/' + str(process.pid) + '/fd').iterdir():
                    try:
                        name = os.readlink(fd)
                        info = pathlib.Path('/proc/' + str(process.pid) + '/fdinfo/' + fd.name).read_text()
                        pos = int(next(x.split(':', 1)[1] for x in info.splitlines() if x.startswith('pos:')))
                    except (OSError, StopIteration): continue
                    receiving = name.startswith(str(case / 'tmp')) and name.endswith(' (deleted)') and pos > 0
                    verifying = name == str(file) and pos >= 1024 * 1024
                    if (c['phase'] == 'receive' and receiving) or (c['phase'] == 'verify' and verifying):
                        injected = time.monotonic()
                        position = pos
                        print(json.dumps({'phase': c['phase'], 'position': pos}), flush=True)
                        break
            time.sleep(.002)
        elapsed = None if injected is None else time.monotonic() - injected
        log.flush()
        text = log_path.read_text(errors='replace')
        for extra in (case / 'receiver' / 'logs').rglob('*.log'):
            text += '\n' + extra.read_text(errors='replace')
        after = file.stat()
        report = {'phase': c['phase'], 'exit_code': process.returncode,
            'phase_observed': c['phase'] == 'control' or injected is not None,
            'native_position_at_injection': position, 'close_to_exit_seconds': elapsed,
            'process_reaped': not pathlib.Path('/proc/' + str(process.pid)).exists(),
            'inode_preserved': (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino),
            'before_sha256': old_hash, 'target_sha256': digest(file),
            'size': after.st_size, 'mode': after.st_mode & 0o777,
            'target_entries': sorted(x.name for x in target.iterdir()),
            'temporary_entries': sorted(x.name for x in (case / 'tmp').iterdir()),
            'completed': '[Receiver Remote] Completed' in text,
            'transport_closed_reported': 'Transport closed during file list/data phase' in text,
            'sparse_patch_completed': 'Direct sparse patch completed' in text}
        print(json.dumps(report), flush=True)
    finally:
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
'''


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def environment():
    return {k: v for k, v in os.environ.items() if not k.lower().endswith('_proxy')}


def ssh_args(args):
    return ['ssh', '-i', str(args.ssh_key), '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            '-o', 'IdentitiesOnly=yes', args.ssh_user + '@' + args.receiver_management]


def json_line(worker, driver=None):
    deadline = time.monotonic() + 260
    while not select.select([worker.stdout], [], [], .05)[0]:
        if driver is not None and driver.poll() is not None:
            raise RuntimeError('QUIC driver exited before the requested phase')
        if time.monotonic() > deadline:
            raise TimeoutError('remote evidence deadline exceeded')
    line = worker.stdout.readline()
    if not line:
        raise RuntimeError('remote worker exited before evidence was delivered')
    return json.loads(line)


def run_case(args, phase, retry, source, expected_hash, token, report):
    label = phase + ('-retry' if retry else '')
    case = '/var/lib/terrasync-ci/' + args.run_id + '/direct-cancel/' + phase
    remote = {'case': case, 'size': args.size, 'reset': not retry,
              'phase': 'control' if retry else phase, 'token': token,
              'binary': '/var/lib/terrasync-ci/' + args.run_id + '/terrasync',
              'data_address': args.receiver_address}
    with (args.work_dir / (label + '-ssh.log')).open('w') as log:
        worker = subprocess.Popen(ssh_args(args) + [shlex.join(['python3', '-c', WORKER, json.dumps(remote)])],
                                  text=True, stdout=subprocess.PIPE, stderr=log)
        driver = None
        try:
            ready = json_line(worker)
            cert = args.work_dir / (label + '.crt')
            cert.write_bytes(base64.b64decode(ready['cert']))
            env = environment()
            if retry:
                config = args.work_dir / 'config.toml'
                env['CARGO_MANIFEST_DIR'] = str(args.work_dir / (label + '-sender') / 'fake')
                completed = subprocess.run([str(args.binary), '-c', str(config), 'sync', '--id', label,
                    '--remote', ready['address'], '--tls-server-cert', str(cert), '--token', token,
                    '--enable-integrity-check', str(source.parent), case + '/target'],
                    env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=240)
                text = completed.stdout.replace(token, '<redacted>')
                (args.work_dir / (label + '-sender.log')).write_text(text)
                assert completed.returncode == 0, 'retry Sender failed'
            else:
                trigger = args.work_dir / (label + '.trigger')
                trigger.unlink(missing_ok=True)
                env.update(TERRASYNC_CANCEL_ADDRESS=ready['address'], TERRASYNC_CANCEL_CERT=str(cert),
                    TERRASYNC_CANCEL_TOKEN_FILE=str(args.work_dir / 'token'),
                    TERRASYNC_CANCEL_SOURCE=str(source), TERRASYNC_CANCEL_TRIGGER=str(trigger))
                with (args.work_dir / (label + '-sender.log')).open('w') as sender_log:
                    driver = subprocess.Popen([str(args.sender_test_binary), '--ignored', '--exact',
                        'close_real_quic_during_direct_transfer', '--nocapture'], env=env,
                        stdout=sender_log, stderr=subprocess.STDOUT)
                    observed = json_line(worker, driver)
                    assert observed['phase'] == phase
                    trigger.touch()
                    assert driver.wait(timeout=15) == 0, 'QUIC cancellation driver failed'
            evidence = json_line(worker)
            assert worker.wait(timeout=10) == 0
            assert evidence['phase_observed'] and evidence['process_reaped'] and evidence['inode_preserved']
            assert evidence['size'] == args.size and evidence['target_entries'] == ['file']
            assert evidence['temporary_entries'] == []
            if retry:
                assert evidence['exit_code'] == 0, 'retry Receiver did not exit successfully'
                assert evidence['completed'] and evidence['target_sha256'] == expected_hash
                assert evidence['mode'] == 0o640
            else:
                assert evidence['exit_code'] != 0, 'cancelled Receiver must report a failing exit status'
                assert not evidence['completed'] and not evidence['sparse_patch_completed']
                assert evidence['transport_closed_reported'] and evidence['close_to_exit_seconds'] < 3
                assert evidence['mode'] == 0o600
                wanted = evidence['before_sha256'] if phase == 'receive' else expected_hash
                assert evidence['target_sha256'] == wanted
            evidence.update(label=label, passed=True)
            report['cases'].append(evidence)
            print(json.dumps({'case': label, 'passed': True,
                              'close_to_exit_seconds': evidence['close_to_exit_seconds']}), flush=True)
        finally:
            cleanup_code = '''import os, pathlib, signal, sys
case = pathlib.Path(sys.argv[1])
for name in ('receiver.pid', 'worker.pid'):
    try:
        pid = int((case / name).read_text())
        command = pathlib.Path('/proc/' + str(pid) + '/cmdline').read_bytes()
        if str(case).encode() in command: os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError): pass
'''
            try:
                subprocess.run(ssh_args(args) + [shlex.join(['python3', '-c', cleanup_code, case])],
                               stdout=log, stderr=log, timeout=20, check=False)
            finally:
                for process in (driver, worker):
                    if process is not None and process.poll() is None:
                        process.terminate()
                        try: process.wait(timeout=5)
                        except subprocess.TimeoutExpired: process.kill(); process.wait()


def main(args):
    if not re.fullmatch(r'(nightly|release)-[a-zA-Z0-9-]+', args.run_id):
        raise ValueError('invalid isolated run id')
    if args.size < 64 * 1024 * 1024:
        raise ValueError('fixture must be at least 64 MiB')
    args.work_dir.mkdir(parents=True, exist_ok=False)
    token = os.urandom(24).hex()
    token_file = args.work_dir / 'token'
    token_file.touch(mode=0o600)
    token_file.write_text(token)
    fixture = args.work_dir / 'source'
    fixture.mkdir()
    source = fixture / 'file'
    with source.open('wb') as stream:
        stream.truncate(args.size)
        stream.seek(args.size // 2)
        stream.write(hashlib.shake_256(b'direct-cancel-real').digest(16 * 1024 * 1024))
    source.chmod(0o640)
    os.utime(source, ns=(1670000001000000000, 1670000001000000000))
    (args.work_dir / 'config.toml').write_text('[database]\nenabled = false\n[scan]\nconcurrency = 1\ninclude_tags = false\n')
    report = {'run_id': args.run_id, 'binary_sha256': digest(args.binary), 'cases': [], 'passed': False}
    lab_env = dict(environment(), LAB_SSH_KEY=str(args.ssh_key), LAB_SSH_USER=args.ssh_user)
    lab = Path(__file__).resolve().parent
    with (args.work_dir / 'lifecycle.log').open('w') as lifecycle:
        try:
            subprocess.run([str(lab / 'prepare-run.sh'), args.run_id], env=lab_env,
                           stdout=lifecycle, stderr=lifecycle, check=True, timeout=120)
            subprocess.run(['scp', '-i', str(args.ssh_key), '-o', 'IdentitiesOnly=yes',
                str(args.binary), args.ssh_user + '@' + args.receiver_management +
                ':/var/lib/terrasync-ci/' + args.run_id + '/terrasync'],
                stdout=lifecycle, stderr=lifecycle, check=True, timeout=120)
            expected = digest(source)
            for phase in ('receive', 'verify'):
                run_case(args, phase, False, source, expected, token, report)
                run_case(args, phase, True, source, expected, token, report)
            report['passed'] = True
        finally:
            report['cleanup_passed'] = False
            try:
                cleanup = subprocess.run([str(lab / 'cleanup-run.sh'), args.run_id], env=lab_env,
                    stdout=lifecycle, stderr=lifecycle, timeout=120)
                report['cleanup_passed'] = cleanup.returncode == 0
            except (OSError, subprocess.TimeoutExpired) as error:
                report['cleanup_error'] = type(error).__name__
            finally:
                report['passed'] = report['passed'] and report['cleanup_passed']
                token_file.unlink(missing_ok=True)
                shutil.rmtree(fixture)
                (args.work_dir / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
            if not report['cleanup_passed']:
                raise RuntimeError('lab cleanup failed; see lifecycle.log and report.json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--sender-test-binary', required=True, type=Path)
    parser.add_argument('--work-dir', required=True, type=Path)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--ssh-key', required=True, type=Path)
    parser.add_argument('--ssh-user', default='ci-runner')
    parser.add_argument('--receiver-management', default='10.131.9.13')
    parser.add_argument('--receiver-address', default='10.10.1.13')
    parser.add_argument('--size', type=int, default=2 * 1024**3)
    main(parser.parse_args())
