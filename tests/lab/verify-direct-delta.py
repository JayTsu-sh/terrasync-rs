#!/usr/bin/env python3
"""Real QUIC Direct-delta verification: 20 GiB basis, 10 MiB changes by default.

Sparse zero-filled fixtures avoid allocating two complete large files. The report
measures actual backend patch bytes, inode preservation, full SHA-256 equality,
and allocated destination blocks. Timing is a sparse-file/cache-dependent sample.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def verify(args):
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    fixture = work / ('direct-delta-' + str(time.time_ns()))
    fixture.mkdir()
    source, target = fixture / 'source', fixture / 'target'
    source.mkdir()
    target.mkdir()
    for root in (source, target):
        with (root / 'file').open('wb') as stream:
            stream.truncate(args.size)
        os.utime(root / 'file', ns=(1670000000000000000, 1670000000000000000))
    with (source / 'file').open('r+b') as stream:
        stream.seek(args.size // 2)
        stream.write(hashlib.shake_256(b'terrasync-direct-delta-fixture').digest(args.changed))
    os.utime(source / 'file', ns=(1670000001000000000, 1670000001000000000))
    original = (target / 'file').stat()
    config = fixture / 'config.toml'
    config.write_text('[database]\nenabled = false\n[scan]\nconcurrency = 1\ninclude_tags = false\n')
    for name in ('sender', 'receiver'):
        (fixture / name).mkdir()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    address = '127.0.0.1:' + str(port)
    cert = fixture / 'server.crt'
    token = os.urandom(24).hex()
    binary = str(args.binary.resolve(strict=True))
    env = {key: value for key, value in os.environ.items() if not key.lower().endswith('_proxy')}
    env['TMPDIR'] = str(work)
    receiver_env = dict(env, CARGO_MANIFEST_DIR=str(fixture / 'receiver' / 'fake'))
    sender_env = dict(env, CARGO_MANIFEST_DIR=str(fixture / 'sender' / 'fake'))
    receiver_log = (work / 'receiver-process.log').open('w')
    sender_log = (work / 'sender-process.log').open('w')
    receiver = None
    report = {'logical_size': args.size, 'changed_source_bytes': args.changed,
              'fixture': 'sparse zero-filled basis with deterministic changed bytes',
              'binary_sha256': digest(args.binary), 'passed': False}
    try:
        receiver = subprocess.Popen([binary, '-c', str(config), 'serve', '--listen', address,
                                     '--tls-cert-out', str(cert), '--token', token, str(target)],
                                    cwd=fixture / 'receiver', env=receiver_env,
                                    stdout=receiver_log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while not cert.exists():
            if receiver.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('Receiver did not become ready; see receiver-process.log')
            time.sleep(0.05)
        start = time.monotonic()
        sender = subprocess.run([binary, '-c', str(config), 'sync', '--id', 'direct_delta',
                                 '--remote', address, '--tls-server-cert', str(cert), '--token', token,
                                 '--enable-integrity-check', str(source), str(target)],
                                cwd=fixture / 'sender', env=sender_env, stdout=sender_log,
                                stderr=subprocess.STDOUT, timeout=args.timeout)
        report['sync_elapsed_seconds'] = time.monotonic() - start
        assert sender.returncode == 0, 'Sender failed; see sender-process.log'
        assert receiver.wait(timeout=30) == 0, 'Receiver failed; see receiver-process.log'
        receiver_log.flush()
        log = (work / 'receiver-process.log').read_text()
        for file in (fixture / 'receiver' / 'logs').rglob('*.log'):
            log += '\n' + file.read_text(errors='replace')
        log = re.sub(r'\x1b\[[0-9;]*m', '', log)
        matches = re.findall(r'written_bytes=(\d+)', log)
        assert matches, 'Missing backend sparse-patch write evidence'
        report['backend_written_bytes'] = int(matches[-1])
        assert args.changed <= report['backend_written_bytes'] <= args.changed + 2 * 128 * 1024
        after = (target / 'file').stat()
        report['inode_preserved'] = original.st_dev == after.st_dev and original.st_ino == after.st_ino
        report['destination_allocated_bytes'] = after.st_blocks * 512
        assert report['inode_preserved'] and after.st_size == args.size
        assert report['destination_allocated_bytes'] < max(args.changed * 4, args.size // 4)
        report['source_sha256'] = digest(source / 'file')
        report['target_sha256'] = digest(target / 'file')
        assert report['source_sha256'] == report['target_sha256']
        report['passed'] = True
    finally:
        if receiver is not None and receiver.poll() is None:
            receiver.terminate()
            try:
                receiver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                receiver.kill()
                receiver.wait()
        receiver_log.close()
        sender_log.close()
        # Keep logs outside the source tree; fixtures and credentials are disposable.
        for name in ('sender', 'receiver'):
            logs = fixture / name / 'logs'
            if logs.exists():
                shutil.copytree(logs, work / (name + '-logs'), dirs_exist_ok=True)
        shutil.rmtree(fixture)
        report['cleaned'] = not fixture.exists()
        (work / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--size', type=int, default=20 * 1024**3)
    parser.add_argument('--changed', type=int, default=10 * 1024**2)
    parser.add_argument('--timeout', type=int, default=1200)
    args = parser.parse_args()
    if args.changed <= 0 or args.size < args.changed * 4:
        parser.error('--size must be at least four times the positive --changed size')
    verify(args)
