"""Real unversioned S3 source cleanup, isolated services and independent final reads."""
import http.client
import http.server
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET

import boto3
from botocore.config import Config


HISTORY_DATA_MODES = ("historydata", "historytags", "historysource", "historymissing", "historygap", "historydisconnect", "historystall")
HISTORY_MARKER_MODES = ("historylatestmarker", "historymarkermissing", "historymarkerreplace", "historymarkerstall", "historymarkersource")
HISTORY_HIDDEN_MODES = (*HISTORY_MARKER_MODES, "historymarkerunchanged")
HISTORY_PLAIN_MODES = (*HISTORY_DATA_MODES, "historyunchanged")

BODY = b"ordinary object data"
NESTED = b"nested object data"
SLASH = b"nonempty directory object"


def run(repository, environment, *, versioned_source=False):
    database_upstream = urllib.parse.urlsplit(environment["LAB_COPY_GENERATION_DSN"])
    client = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", aws_access_key_id="migration-test",
                          aws_secret_access_key="migration-test-isolated-only", region_name="us-east-1",
                          config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3))
    for _ in range(60):
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(0.25)
    else:
        raise RuntimeError("owned S3 readiness timeout")
    buckets = {role: f"objclean-{role}-{uuid.uuid4().hex[:12]}" for role in ("source", "latest", "history")}
    for role, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if role == "history" or (role == "source" and versioned_source):
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        client.put_object(Bucket=bucket, Key="sentinel", Body=b"retain sentinel")
    guard = threading.Lock()
    active = [None]
    states = {}
    reports = []
    stopped = threading.Event()
    with tempfile.TemporaryDirectory(prefix="ts-object-cleanup-") as directory:
        root = Path(directory)

        def inspect(prefix, mode):
            state = states[prefix]
            target = state["target"]
            keys = state["keys"]
            remaining = {item["Key"][len(prefix) + 1:]: item for item in client.list_objects_v2(
                Bucket=buckets["source"], Prefix=prefix + "/").get("Contents", [])}
            source_removed = mode == "normal" or (mode == "lostack" and not versioned_source)
            expected_remaining = ({"slash/"} if target == "local" and state["initial"] == "normal" else set()) if source_removed else set(keys)
            if mode == "missingcurrent" or state["initial"] in HISTORY_HIDDEN_MODES:
                expected_remaining = set()
            assert set(remaining) == expected_remaining, (prefix, mode, remaining, expected_remaining)
            if versioned_source:
                history = client.list_object_versions(Bucket=buckets["source"], Prefix=prefix + "/")
                records = history.get("Versions", []) + history.get("DeleteMarkers", [])
                for key in keys:
                    actual = {item["VersionId"] for item in records if item["Key"] == f"{prefix}/{key}"}
                    expected = set(state["source_versions"][key]) if key in expected_remaining or mode == "missingcurrent" or (state["initial"] in HISTORY_HIDDEN_MODES and mode != "normal") else set()
                    if mode in ("lostack", "partial", "malformed"):
                        expected -= {version for deleted_key, version in state["deleted_versions"] if deleted_key == f"{prefix}/{key}"}
                    assert actual == expected, (prefix, mode, key, actual, expected)
                if mode not in ("normal", "lostack", "partial", "malformed"):
                    assert not state["deleted_versions"], (prefix, state)
                for key in keys:
                    deleted = [version for deleted_key, version in state["deleted_versions"] if deleted_key == f"{prefix}/{key}"]
                    if mode == "normal" and key not in expected_remaining:
                        assert deleted and deleted[-1] == state["latest_versions"][key], (prefix, key, deleted)
                    if mode == "lostack":
                        assert deleted and deleted[0] != state["latest_versions"][key], (prefix, key, deleted)
            for key, body in keys.items():
                if key in remaining:
                    assert client.get_object(Bucket=buckets["source"], Key=f"{prefix}/{key}")["Body"].read() == state["source_payloads"][key]
                if mode == "normal":
                    body = state["source_payloads"][key]
                elif mode == "unverified":
                    body = bytes([body[0] ^ 1]) + body[1:]
                if target == "local":
                    if key != "slash/":
                        assert (root / prefix / "target" / key).read_bytes() == body
                elif mode in ("data", "verify", "cleanupunknown"):
                    assert not client.list_objects_v2(Bucket=buckets[target], Prefix=f"{prefix}/{key}").get("Contents"), (prefix, key)
                else:
                    selector = {"VersionId": state["target_data_versions"][-1]} if state["initial"] in HISTORY_HIDDEN_MODES else {}
                    assert client.get_object(Bucket=buckets[target], Key=f"{prefix}/{key}", **selector)["Body"].read() == body
                    if mode == "normal":
                        assert client.get_object_tagging(Bucket=buckets[target], Key=f"{prefix}/{key}", **selector)["TagSet"] == state["source_tags_expected"]
            if target == "local":
                assert state["source_tags"] == 0, (prefix, "Local target must not request source tags", state)
            if target == "history":
                history = client.list_object_versions(Bucket=buckets[target], Prefix=prefix + "/")
                markers = 2 if state["initial"] in HISTORY_HIDDEN_MODES else len(keys) if versioned_source and state["initial"] not in HISTORY_PLAIN_MODES else 0
                if state["initial"] == "historymarkermissing": markers -= 1
                assert len(history.get("DeleteMarkers", [])) == markers, (prefix, history)
                assert len(history.get("Versions", [])) == len(keys) * (4 if state["initial"] == "historymarkerreplace" else 2 if state["initial"] == "historymissing" else 3 if versioned_source else 1), (prefix, history)
                assert {record["Key"] for record in history["Versions"]} == {prefix + "/" + key for key in keys}
                if versioned_source:
                    assert [client.get_object(Bucket=buckets[target], Key=record["Key"], VersionId=record["VersionId"])["Body"].read()
                            for record in history["Versions"]] == ([b"replacement", BODY, b"old-1", b"old-0"] if state["initial"] == "historymarkerreplace" else [BODY, b"old-1"] if state["initial"] == "historymissing" else [BODY, b"old-1", b"old-0"]), history
            if state["initial"] == "lostack":
                expected_deletes = (len(state["source_versions"]["payload"]) if mode == "normal"
                                    else len(state["batch_requests"][0])) if versioned_source else 1
                assert state["forwarded_deletes"] == expected_deletes
            if mode in ("data", "metadata", "verify", "cleanup403", "cleanupunknown"):
                assert state["forwarded_deletes"] == 0
            if state["initial"] in ("verify", "cleanup403", "cleanupunknown"):
                assert state["target_deletes"] == 1, (prefix, state)
            if state["initial"] in ("unchanged", "unchangedbad", "historyunchanged", "historymarkerunchanged"):
                expected_writes = 3 if state["initial"] in ("historyunchanged", "historymarkerunchanged") else 2 if state["initial"] == "unchangedbad" and mode == "normal" else 1
                assert state["target_data_writes"] == expected_writes, (prefix, mode, state)
            if state["initial"] == "large":
                assert state["source_tags"] == 2 and state["source_data_reads"] == 0 and state["target_data_writes"] == 1, (prefix, state)
                assert len(state["source_versions"]["payload"]) == 256 and state["forwarded_deletes"] == 256, (prefix, state)
            if state["initial"] in ("large", "batchmany"):
                assert state["batch_requests"] and all(1 <= len(batch) <= 100 for batch in state["batch_requests"]), state
            if mode in ("partial", "malformed", "batchmany"):
                state["failed_deleted_versions"] = list(state["deleted_versions"])
                state["failed_requested_versions"] = [item for batch in state["batch_requests"] for item in batch]
                assert len(state["batch_requests"]) == 1, (prefix, state)
                assert all(version != state["latest_versions"]["payload"] for _, version in state["deleted_versions"]), state
            state["inspections"] += 1
            print(f"independent object cleanup prefix={prefix} mode={mode} source_remaining={sorted(remaining)} target_data_verified=true fault_to_final_report_ms={state.get('fault_to_final_report_ms')}", flush=True)

        class Relay(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.path.startswith(("/arm/", "/recover/", "/inspect/", "/damage/")):
                    action, prefix, mode = self.path.strip("/").split("/")
                    if action == "arm":
                        target = prefix.split("-")[1]
                        keys = {"payload": BODY}
                        if mode == "normal" and (target != "latest" or versioned_source):
                            keys.update({"tree/file": NESTED, "slash/": SLASH})
                        source_versions = {}
                        latest_versions = {}
                        for key, body in keys.items():
                            if versioned_source and mode == "batchmany":
                                for _ in range(100):
                                    client.put_object(Bucket=buckets["source"], Key=f"{prefix}/{key}", Body=b"old-0", Tagging="proof=old-0")
                            elif versioned_source:
                                for _ in range(85 if mode == "large" else 1):
                                    client.put_object(Bucket=buckets["source"], Key=f"{prefix}/{key}", Body=b"old-0", Tagging="proof=old-0")
                                    if mode not in HISTORY_PLAIN_MODES:
                                        client.delete_object(Bucket=buckets["source"], Key=f"{prefix}/{key}")
                                    client.put_object(Bucket=buckets["source"], Key=f"{prefix}/{key}", Body=b"old-1", Tagging="proof=old-1")
                            if versioned_source and mode == "partial":
                                client.put_object(Bucket=buckets["source"], Key=f"{prefix}/{key}", Body=b"accepted-old", Tagging="proof=accepted-old")
                            created = client.put_object(Bucket=buckets["source"], Key=f"{prefix}/{key}", Body=body, Tagging="proof=raw")
                            if mode in HISTORY_HIDDEN_MODES:
                                created = client.delete_object(Bucket=buckets["source"], Key=f"{prefix}/{key}")
                            latest_versions[key] = created.get("VersionId", "null")
                            if versioned_source:
                                history_pages = list(client.get_paginator("list_object_versions").paginate(Bucket=buckets["source"], Prefix=f"{prefix}/{key}"))
                                history = history_pages[0]
                                source_data_versions = [item["VersionId"] for page in history_pages for item in page.get("Versions", []) if item["Key"] == f"{prefix}/{key}"]
                                source_versions[key] = {item["VersionId"] for page in history_pages for item in page.get("Versions", []) + page.get("DeleteMarkers", []) if item["Key"] == f"{prefix}/{key}"}
                                if key.endswith("/"):
                                    # 当前 RustFS 将非空尾斜杠对象保存在可变 null 槽，不虚构四个历史。
                                    assert source_versions[key] == {"null"}, (prefix, key, history)
                                else:
                                    assert len(source_versions[key]) == (256 if mode == "large" else 101 if mode == "batchmany" else 5 if mode == "partial" else 3 if mode in HISTORY_PLAIN_MODES else 5 if mode in HISTORY_HIDDEN_MODES else 4), (prefix, key, history)
                        with guard:
                            assert prefix not in states
                            states[prefix] = dict(initial=mode, mode=mode, target=target, keys=keys,
                                                  forwarded_deletes=0, target_deletes=0, source_tags=0, affected=0,
                                                  first_fault=None, inspections=0, fact_released=False,
                                                  deleted=threading.Event(), source_versions=source_versions, deleted_versions=[], batch_requests=[],
                                                  source_payloads=dict(keys), source_tags_expected=[{"Key": "proof", "Value": "raw"}],
                                                  cleanup_seen=False, target_data_writes=0, source_data_reads=0, latest_versions=latest_versions,
                                                  target_data_versions=[], target_marker_versions=[], history_fault=False,
                                                  source_data_versions=source_data_versions if versioned_source else [],
                                                  source_marker_versions=[item["VersionId"] for page in history_pages for item in page.get("DeleteMarkers", []) if item["Key"] == f"{prefix}/payload"] if versioned_source else [])
                            active[0] = prefix
                    elif action == "recover":
                        with guard:
                            states[prefix]["mode"] = "normal"
                    elif action == "damage":
                        damaged = bytes([BODY[0] ^ 1]) + BODY[1:]
                        client.put_object(Bucket=buckets["latest"], Key=f"{prefix}/payload", Body=damaged, Tagging="proof=raw")
                        with guard:
                            states[prefix]["affected"] += 1
                            states[prefix]["first_fault"] = time.monotonic()
                    else:
                        inspect(prefix, mode)
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.relay()

            def do_POST(self):
                if self.path == "/callback":
                    report = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    reports.append(report)
                    if report["is_final"] and not report.get("diagnostics") and report["migration"]["phase"] == "failed":
                        prefix = report["job_id"].removeprefix("objclean_").replace("_", "-")
                        with guard:
                            state = states[prefix]
                            if state["first_fault"] is not None:
                                state["fault_to_final_report_ms"] = round((time.monotonic() - state["first_fault"]) * 1000)
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.relay()

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

            def relay(self):
                parsed = urllib.parse.urlsplit(self.path)
                path = urllib.parse.unquote(parsed.path)
                query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                parts = path.lstrip("/").split("/", 1)
                database = parts[0] not in buckets.values()
                source = parts[0] == buckets["source"]
                body = self.read_body()
                with guard:
                    prefix = active[0]
                    state = states.get(prefix)
                    mode = state["mode"] if state else "normal"
                    bulk = source and self.command == "POST" and "delete" in query
                    deleting = source and (self.command == "DELETE" or bulk)
                    if source and self.command == "GET" and "tagging" in query:
                        state["source_tags"] += 1
                    if source and self.command == "GET" and "tagging" not in query and path.endswith("/" + prefix + "/payload"):
                        state["source_data_reads"] += 1
                    if mode in HISTORY_DATA_MODES and mode != "historydata" and not state["history_fault"]:
                        historical_target_head = not database and not source and self.command == "HEAD" and query.get("versionId", [None])[0] in state["target_data_versions"][:1] and len(state["target_data_versions"]) == 3
                        sql = query.get("query", [""])[0] or (body.decode("utf-8", errors="replace") if database else "")
                        historical_read = database and "HISTORY_RECORDS" in sql.upper() and "COPY_PATH_CONFIRMED" in sql.upper() and "SELECT" in sql.upper()
                        if (mode == "historygap" and historical_read) or (mode != "historygap" and historical_target_head):
                            state["history_fault"] = True
                            state["affected"] += 1
                            state["first_fault"] = time.monotonic()
                            key = f"{prefix}/payload"
                            if mode == "historygap":
                                changed = sql.replace(" ORDER BY", " AND source_version_id != '" + state["source_data_versions"][1] + "' ORDER BY")
                                assert changed != sql, "history query ordering clause missing"
                                if "query" in query:
                                    query["query"] = [changed]
                                    self.path = urllib.parse.urlunsplit(("", "", parsed.path, urllib.parse.urlencode(query, doseq=True), ""))
                                else:
                                    body = changed.encode()
                                print("owned historical catalogue middle-row gap injected", flush=True)
                            elif mode == "historymissing":
                                client.delete_object(Bucket=buckets["history"], Key=key, VersionId=state["target_data_versions"][0])
                            elif mode in ("historytags", "historysource"):
                                bucket = buckets["source"] if mode == "historysource" else buckets["history"]
                                version = state["source_data_versions"][-1] if mode == "historysource" else state["target_data_versions"][0]
                                client.put_object_tagging(Bucket=bucket, Key=key, VersionId=version, Tagging={"TagSet": [{"Key": "proof", "Value": "changed"}]})
                    if mode in HISTORY_MARKER_MODES[1:] and not state["history_fault"]:
                        wanted = state["target_marker_versions"][-1:] if mode == "historymarkerreplace" else state["target_marker_versions"][:1]
                        ready = len(state["target_data_versions"]) == 3 and len(state["target_marker_versions"]) == 2
                        if ready and not database and not source and self.command == "HEAD" and query.get("versionId", [None])[0] in wanted:
                            state["history_fault"] = True
                            state["affected"] += 1
                            state["first_fault"] = time.monotonic()
                            key = f"{prefix}/payload"
                            if mode == "historymarkermissing":
                                client.delete_object(Bucket=buckets["history"], Key=key, VersionId=wanted[0])
                            elif mode == "historymarkerreplace":
                                client.put_object(Bucket=buckets["history"], Key=key, Body=b"replacement")
                            elif mode == "historymarkersource":
                                records = client.list_object_versions(Bucket=buckets["source"], Prefix=key)["DeleteMarkers"]
                                version = next(record["VersionId"] for record in records if not record["IsLatest"])
                                client.delete_object(Bucket=buckets["source"], Key=key, VersionId=version)
                                state["source_versions"]["payload"].remove(version)
                    mutating_observation = versioned_source and source and self.command == "HEAD" and "versionId" not in query and path.endswith("/" + prefix + "/payload") and mode in ("missingcurrent", "sourcechanged", "metaread", "metachanged", "contradictory") and not state["cleanup_seen"]
                    if mutating_observation:
                        state["cleanup_seen"] = True
                        key = f"{prefix}/payload"
                        if mode == "missingcurrent":
                            created = client.delete_object(Bucket=buckets["source"], Key=key)
                            state["source_versions"]["payload"].add(created["VersionId"])
                            state["latest_versions"]["payload"] = created["VersionId"]
                        elif mode == "sourcechanged":
                            body = BODY + b"-changed"
                            created = client.put_object(Bucket=buckets["source"], Key=key, Body=body, Tagging="proof=raw")
                            state["source_payloads"]["payload"] = body
                            state["source_versions"]["payload"].add(created["VersionId"])
                            state["latest_versions"]["payload"] = created["VersionId"]
                        elif mode == "metachanged":
                            tags = [{"Key": "proof", "Value": "changed"}]
                            client.put_object_tagging(Bucket=buckets["source"], Key=key, Tagging={"TagSet": tags})
                            state["source_tags_expected"] = tags
                    source_metadata_failure = versioned_source and source and self.command == "GET" and "tagging" in query and mode == "metaread" and state["cleanup_seen"]
                    data_failure = not source and self.command == "PUT" and "tagging" not in query and mode == "data"
                    if not database and not source and self.command == "PUT" and "tagging" not in query:
                        state["target_data_writes"] += 1
                    metadata_failure = not source and self.command == "PUT" and "tagging" in query and mode == "metadata"
                    target_deleting = not database and not source and self.command == "DELETE" and "uploadId" not in query
                    verifying = not database and not source and self.command == "GET" and "tagging" not in query and path.endswith("/" + prefix + "/payload") and mode in ("verify", "cleanup403", "cleanupunknown")
                    if target_deleting:
                        state["target_deletes"] += 1
                    affected = mutating_observation or source_metadata_failure or (deleting and mode not in ("normal", "unchanged", "unchangedbad", "large")) or data_failure or metadata_failure or verifying or (target_deleting and mode in ("cleanup403", "cleanupunknown"))
                    if affected:
                        state["affected"] += 1
                        state["first_fault"] = state["first_fault"] or time.monotonic()
                selected = []
                if bulk:
                    selected = [(node.find("{*}Key").text, node.find("{*}VersionId").text)
                                for node in ET.fromstring(body) if node.tag.rsplit("}", 1)[-1] == "Object"]
                    assert selected and len(selected) <= 100 and all(version for _, version in selected), selected
                    with guard:
                        state["batch_requests"].append(selected)
                        for key, version in selected:
                            relative = key.removeprefix(prefix + "/")
                            assert version != state["latest_versions"][relative], (prefix, selected)
                    if mode in ("partial", "batchmany"):
                        denied = selected[:2] if mode == "partial" else selected
                        accepted = selected[2:] if mode == "partial" else []
                        actual = client.delete_objects(Bucket=buckets["source"], Delete={"Objects": [
                            {"Key": key, "VersionId": version} for key, version in accepted], "Quiet": False}) if accepted else {}
                        reply = ET.Element("DeleteResult")
                        for item in actual.get("Deleted", []):
                            node = ET.SubElement(reply, "Deleted")
                            ET.SubElement(node, "Key").text = item["Key"]
                            ET.SubElement(node, "VersionId").text = item["VersionId"]
                        for key, version in denied:
                            node = ET.SubElement(reply, "Error")
                            ET.SubElement(node, "Key").text = key
                            ET.SubElement(node, "VersionId").text = version
                            ET.SubElement(node, "Code").text = "AccessDenied"
                        with guard:
                            state["deleted_versions"].extend((item["Key"], item["VersionId"]) for item in actual.get("Deleted", []))
                            state["forwarded_deletes"] += len(actual.get("Deleted", []))
                        payload = ET.tostring(reply)
                        self.send_response(200)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                        return
                if database:
                    sql = query.get("query", [""])[0].upper()
                    held = not versioned_source and state and state["initial"] == "normal" and state["target"] == "latest" and not state["fact_released"] and "INSERT INTO" in sql and "FORMAT ROWBINARY" in sql and "BASE_OBJCLEAN_" in sql
                    if held:
                        assert state["deleted"].wait(8), (prefix, "source deletion must not wait for database fact receipt")
                        inspect(prefix, "normal")
                        state["fact_released"] = True
                        print(f"independent ordinary S3 source deleted before fact forwarded prefix={prefix}", flush=True)
                if source_metadata_failure or (affected and mode in ("readonly", "data", "metadata")) or (target_deleting and mode == "cleanup403"):
                    payload = f"<Error><Code>AccessDenied</Code><Message>owned object {mode} refusal</Message></Error>".encode()
                    self.send_response(403)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                historical_network = state and state["history_fault"] and (
                    (mode == "historydisconnect" and not database and not source and self.command == "HEAD")
                    or (mode == "historystall" and source and self.command == "GET" and "tagging" in query)
                    or (mode == "historymarkerstall" and not database and not source and self.command == "HEAD" and query.get("versionId", [None])[0] in state["target_marker_versions"]))
                if historical_network:
                    if mode in ("historystall", "historymarkerstall"):
                        stopped.wait(15)
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                if deleting and mode in ("disconnect", "stall"):
                    if mode == "stall":
                        stopped.wait(15)
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                connection = http.client.HTTPConnection(database_upstream.hostname if database else "127.0.0.1",
                                                        database_upstream.port if database else 19000, timeout=15 if database else 5)
                try:
                    headers = {name: value for name, value in self.headers.items()
                               if name.lower() not in ("connection", "content-length", "transfer-encoding")}
                    headers["Content-Length"] = str(len(body))
                    connection.request(self.command, self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    payload = response.read()
                    if not database and not source and self.command == "PUT" and "tagging" not in query and response.status == 200:
                        if created_version := response.getheader("x-amz-version-id"):
                            with guard:
                                state["target_data_versions"].append(created_version)
                    if target_deleting and "versionId" not in query and 200 <= response.status < 300:
                        if created_version := response.getheader("x-amz-version-id"):
                            with guard:
                                state["target_marker_versions"].append(created_version)
                    rewritten = source and "versions" in query and mode == "contradictory" and state["cleanup_seen"] and response.status == 200
                    if rewritten:
                        document = ET.fromstring(payload)
                        if document.tag.startswith("{"):
                            ET.register_namespace("", document.tag[1:].partition("}")[0])
                        latest = next(node for node in document if node.tag.rsplit("}", 1)[-1] == "Version" and node.find("{*}IsLatest").text == "true")
                        duplicate = copy.deepcopy(latest)
                        duplicate.find("{*}IsLatest").text = "false"
                        document.insert(list(document).index(latest) + 1, duplicate)
                        payload = ET.tostring(document, encoding="utf-8")
                    if deleting:
                        with guard:
                            if bulk:
                                acknowledged = [(node.find("{*}Key").text, node.find("{*}VersionId").text)
                                                for node in ET.fromstring(payload) if node.tag.rsplit("}", 1)[-1] == "Deleted"]
                                assert len(acknowledged) == len(selected), (prefix, payload)
                                state["deleted_versions"].extend(acknowledged)
                                state["forwarded_deletes"] += len(acknowledged)
                            else:
                                if versioned_source:
                                    assert "versionId" in query and len(query["versionId"]) == 1, (self.command, self.path)
                                    state["deleted_versions"].append((parts[1], query["versionId"][0]))
                                state["forwarded_deletes"] += 1
                            state["deleted"].set()
                    if bulk and mode == "malformed":
                        document = ET.fromstring(payload)
                        first = next(node for node in document if node.tag.rsplit("}", 1)[-1] == "Deleted")
                        first.remove(first.find("{*}VersionId"))
                        payload = ET.tostring(document)
                        rewritten = True
                    if verifying and response.status in (200, 206):
                        print(f"owned corrupt read mode={mode} status={response.status} path={path}", flush=True)
                        assert payload, (prefix, self.path)
                        payload = bytes([payload[0] ^ 1]) + payload[1:]
                    if (deleting and mode == "lostack") or (target_deleting and mode == "cleanupunknown"):
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for name, value in response.getheaders():
                        if name.lower() not in ("connection", "content-length", "transfer-encoding") and not ((verifying or rewritten) and name.lower().startswith("x-amz-checksum")):
                            self.send_header(name, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    connection.close()

            do_HEAD = do_PUT = do_DELETE = relay

        relay = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay)
        relay.daemon_threads = True
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        address = f"127.0.0.1:{relay.server_port}"
        environment = dict(environment, S3_CONNECT_TIMEOUT="1", S3_READ_TIMEOUT="1", S3_OPERATION_TIMEOUT="2",
                           LAB_OBJECT_CLEANUP_CONTROL=address, LAB_OBJECT_CLEANUP_CALLBACK=f"http://{address}/callback",
                           LAB_OBJECT_CLEANUP_ROOT=str(root), LAB_COPY_GENERATION_DSN=f"http://{address}",
                           LAB_OBJECT_CLEANUP_VERSIONED=str(versioned_source).lower())
        for role, bucket in buckets.items():
            environment["LAB_OBJECT_CLEANUP_" + role.upper()] = f"s3://migration-test:migration-test-isolated-only@{bucket}.127.0.0.1:{relay.server_port}"
        try:
            subprocess.run(["cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                            "copy_execution::object_cleanup_tests::real_unversioned_object_source_cleanup_and_failures",
                            "--", "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                           cwd=repository, env=environment, check=True, timeout=360)
            if environment.get("LAB_OBJECT_HISTORY_ONLY") == "true":
                summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
                assert len(states) == 36 and len(summaries) == 40 and all(report["diagnostics_complete"] for report in summaries)
                assert sum(report["migration"]["phase"] == "failed" for report in summaries) == 20
                assert all(state["history_fault"] for state in states.values() if state["initial"] in HISTORY_DATA_MODES and state["initial"] != "historydata")
                assert all(state["history_fault"] for state in states.values() if state["initial"] in HISTORY_MARKER_MODES[1:])
                for mode in (*HISTORY_DATA_MODES[1:], *HISTORY_MARKER_MODES[1:]):
                    failed = [report for report in summaries if report["job_id"].endswith("_" + mode)]
                    assert len(failed) == 2 and all(report["task_error_count"] == 1 and report["detail"]["error_count"] == 0 for report in failed)
                for prefix, state in states.items():
                    if state["initial"] not in HISTORY_MARKER_MODES[1:]: continue
                    job = "objclean_" + prefix.replace("-", "_")
                    details = [event for report in reports if report["job_id"] == job
                               for event in (report.get("diagnostics") or {}).get("events", [])
                               if event["code"] == "target_preserved" and event.get("path") == "payload"]
                    assert len(details) == 1, (prefix, details)
                    message = details[0]["message"]
                    assert any("source_version=" + version + " " in message for version in state["source_marker_versions"]), (prefix, message)
                    assert any('target_version=Some("' + version + '") ' in message for version in state["target_marker_versions"]), (prefix, message)
                print("independent focused history readiness deployments=2 normal=20 failure=20 callback_HTTP204=true source_retained_on_fault=true", flush=True)
                return
            assert len(states) == (84 if versioned_source else 30) and all(state["inspections"] == (3 if state["initial"] == "unchangedbad" else 2 if state["initial"] not in ("normal", "historyguard", "missingcurrent", "large", *HISTORY_DATA_MODES, *HISTORY_MARKER_MODES) or (not versioned_source and state["target"] == "latest") else 1) for state in states.values()), states
            assert sum(state["fact_released"] for state in states.values()) == (0 if versioned_source else 4), states
            assert all(state["affected"] for state in states.values() if state["initial"] not in ("normal", "historyguard", "unchanged", "large", "historydata", "historyunchanged", "historymarkerunchanged", "historylatestmarker")), states
            summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
            assert len(summaries) == (126 if versioned_source else 48) and all(report["diagnostics_complete"] for report in summaries), summaries
            assert sum(report["migration"]["phase"] == "failed" for report in summaries) == (56 if versioned_source else 18)
            events = {event["event_id"]: event for report in reports
                      for event in (report.get("diagnostics") or {}).get("events", [])}
            purge = [event for event in events.values() if event.get("retention", {}).get("reason") == "source_deletion_unconfirmed"]
            assert len(purge) == (20 if versioned_source else 8) and all(event["retention"]["source_presence"] == "unknown" and event["path"] == "payload" for event in purge), purge
            if versioned_source:
                large = [report for report in summaries if report["job_id"].endswith("_large")]
                assert len(large) == 2 and all(report["detail"]["scanned_files"] == 1 and report["detail"]["transferred_bytes"] == len(BODY) and report["task_error_count"] == 0 for report in large), large
                for mode, reason in (("missingcurrent", "无法重新确认版本化源"), ("sourcechanged", "source history changed"),
                                     ("metaread", "PermissionDenied"), ("metachanged", "Tags"),
                                     ("unchangedbad", "Content"),
                                     ("historytags", "Tags"), ("historysource", "Tags"),
                                     ("historymissing", "NotFound"), ("historygap", "段内事实缺失"),
                                     ("historydisconnect", "Connectivity"), ("historystall", "Timeout"),
                                     ("historymarkermissing", "NotFound"), ("historymarkerreplace", "Conflict"),
                                     ("historymarkerstall", "Timeout"), ("historymarkersource", "NotFound")):
                    failures = [report for report in summaries if report["job_id"].endswith("_" + mode) and report["migration"]["phase"] == "failed"]
                    assert len(failures) == 2 and all(report["task_error_count"] == 1 for report in failures), failures
                    details = [event for report in reports if report["job_id"].endswith("_" + mode)
                               for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "target_preserved" and event.get("path") == "payload"]
                    assert len(details) == 2 and all(reason in event["message"] for event in details), (mode, details)
                for mode, count, cause in [("partial", 2, "AccessDenied"), ("malformed", 2, "Protocol"), ("batchmany", 100, "AccessDenied")]:
                    for prefix, state in states.items():
                        if state["initial"] != mode:
                            continue
                        details = {event["event_id"]: event for report in reports if report["job_id"] == "objclean_" + prefix.replace("-", "_")
                                   for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "Delete"}
                        assert len(details) == count and all(cause in event["message"] and event["path"] == "payload" for event in details.values()), (mode, prefix, details)
                        # Only submitted data versions can have failed bulk receipts. A marker
                        # retained after that failure was never submitted and has no invented cause.
                        expected_ids = {version for key, version in state["failed_requested_versions"] if key.endswith("/payload")}
                        if mode == "partial":
                            expected_ids -= {version for key, version in state["failed_deleted_versions"] if key.endswith("/payload")}
                        actual_ids = {event["message"].split("source_version=", 1)[1].split(" phase=", 1)[0] for event in details.values()}
                        assert actual_ids == expected_ids, (prefix, actual_ids, expected_ids)
                        failed = [report for report in summaries if report["job_id"] == "objclean_" + prefix.replace("-", "_") and report["migration"]["phase"] == "failed"]
                        assert len(failed) == 1 and failed[0]["task_error_count"] == 1 and failed[0]["detail"]["error_count"] == 0 and failed[0]["detail"]["scanned_files"] == 1, failed
                        if mode == "batchmany":
                            batches = [report["diagnostics"] for report in reports if report["job_id"] == failed[0]["job_id"] and report.get("diagnostics")]
                            assert len({batch["batch_id"] for batch in batches}) >= 2 and all(len(batch["events"]) <= 64 for batch in batches), batches
                contradictory = [event for report in reports if report["job_id"].endswith("_contradictory")
                                 for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "target_preserved" and event.get("path") == "payload"]
                assert len(contradictory) == 2 and all("current and historical" in event["message"] and "Protocol" in event["message"] for event in contradictory), contradictory
            for mode in ("data", "metadata"):
                failures = [report for report in summaries if report["job_id"].endswith("_" + mode) and report["migration"]["phase"] == "failed"]
                assert len(failures) == 2 and all(report["detail"]["error_count"] == 1 for report in failures), failures
                detailed = {event["event_id"]: event for report in reports if report["job_id"].endswith("_" + mode)
                            for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "copy_version_failed"}
                assert len(detailed) == 2, detailed
                assert all(event["path"] == "payload" and "HTTP=403" in event["message"] and "code=AccessDenied" in event["message"]
                           and "backend=S3" in event["message"] and "phase=" in event["message"] for event in detailed.values()), detailed
                if mode == "data":
                    assert all("direct data cleanup failed" not in event["message"] for event in detailed.values()), detailed
                if mode == "metadata":
                    assert all("phase=target_metadata_apply" in event["message"] for event in detailed.values()), detailed
                print(f"callback object failure mode={mode} details=" + json.dumps([event["message"] for event in detailed.values()]), flush=True)
            for mode in ("verify", "cleanup403", "cleanupunknown"):
                failures = [report for report in summaries if report["job_id"].endswith("_" + mode) and report["migration"]["phase"] == "failed"]
                assert len(failures) == 2 and all(report["detail"]["error_count"] == 1 and report["task_error_count"] == 0 for report in failures), failures
                detailed = {event["event_id"]: event for report in reports if report["job_id"].endswith("_" + mode)
                            for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "copy_version_failed"}
                assert len(detailed) == 2 and all(event["path"] == "payload" and "S3 published checksum mismatch" in event["message"] for event in detailed.values()), detailed
                if mode == "verify":
                    assert all("direct data cleanup failed" not in event["message"] for event in detailed.values()), detailed
                if mode != "verify":
                    assert all("direct data cleanup failed" in event["message"] for event in detailed.values()), detailed
                if mode == "cleanup403":
                    assert all("PermissionDenied" in event["message"] for event in detailed.values()), detailed
                if mode == "cleanupunknown":
                    assert all("Unknown" in event["message"] for event in detailed.values()), detailed
                print(f"callback postpublication failure mode={mode} details=" + json.dumps([event["message"] for event in detailed.values()]), flush=True)
            for role, bucket in buckets.items():
                assert client.get_object(Bucket=bucket, Key="sentinel")["Body"].read() == b"retain sentinel"
                assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
            print(f"independent object cleanup source_versioned={versioned_source} deployments=2 normal={36 if versioned_source else 12} failures={56 if versioned_source else 18} recovery={34 if versioned_source else 18} HTTP_204=true final_reports_saved=true source_deleting_base_empty=true all_recovery_generations_full=true unchanged_incremental_has_no_data_recopy={versioned_source} large_history_records_per_key={256 if versioned_source else 0}", flush=True)
        finally:
            stopped.set()
            relay.shutdown()
            relay.server_close()
