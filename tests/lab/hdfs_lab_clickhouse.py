"""Private ClickHouse fixture for the managed HDFS lab.

Startup returns a live child and its client environment, or raises. StartupError
subclasses identify fixture failures. Filesystem and spawn errors retain their
native types, and unexpected exceptions propagate. A failed bootstrap after
dispatch can have an unknown write outcome and is never replayed. The caller
owns exit status mapping, cgroup cleanup and removal of the fixture directory.
"""

import hashlib
import http.client
import os
import pathlib
import secrets
import socket
import subprocess
import time
from collections.abc import Callable
from contextlib import closing, suppress
from xml.sax.saxutils import escape

_HOST = "127.0.0.1"
_USER = "terrasync_lab"
_DATABASE = "terrasync_lab"
_SETUP_SECONDS = 20
_IO_SECONDS = 0.5
_POLL_SECONDS = 0.1


class StartupError(RuntimeError):
    """An identified ClickHouse fixture setup failure."""


class StartupCancelled(StartupError):
    """The caller cancelled fixture setup."""


class StartupTimeout(StartupError):
    """The monotonic fixture setup budget expired."""


class StartupExited(StartupError):
    """The server child exited before setup completed."""

    def __init__(self, returncode: int):
        self.returncode = returncode
        super().__init__(
            f"ClickHouse fixture exited during setup (status {returncode})"
        )


class StartupOwnershipError(StartupError):
    """The HTTP endpoint could not prove its fixture identity."""


class StartupBootstrapError(StartupError):
    """ClickHouse did not confirm database creation."""


def _remaining(deadline: float, cancelled: Callable[[], bool]) -> float:
    if cancelled():
        raise StartupCancelled("ClickHouse fixture setup cancelled")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise StartupTimeout("ClickHouse fixture setup timed out")
    return remaining


class _SetupSocket(socket.socket):
    def __init__(self, deadline: float, cancelled: Callable[[], bool]):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM)
        self._deadline = deadline
        self._cancelled = cancelled

    def limit_wait(self) -> None:
        self.settimeout(min(_IO_SECONDS, _remaining(self._deadline, self._cancelled)))

    def recv_into(self, buffer, nbytes: int = 0, flags: int = 0) -> int:
        # 在 HTTP 解析器的每次实际读取前检查预算，防止慢速响应延长启动期限。
        self.limit_wait()
        try:
            return super().recv_into(buffer, nbytes, flags)
        except TimeoutError:
            _remaining(self._deadline, self._cancelled)
            raise


def _write_xml(path: pathlib.Path, contents: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        file.write(contents)


def _configure(
    directory: pathlib.Path, port: int, password: str, owner: str
) -> pathlib.Path:
    data = directory / "data"
    log = directory / "log"
    config = directory / "config"
    for path in (data, data / "tmp", log, config):
        path.mkdir(mode=0o700)

    password_hash = hashlib.sha256(password.encode("ascii")).hexdigest()
    _write_xml(
        config / "users.xml",
        f"""<clickhouse>
  <profiles><default><max_threads>4</max_threads></default></profiles>
  <users>
    <{_USER}>
      <password_sha256_hex>{password_hash}</password_sha256_hex>
      <networks><ip>{_HOST}</ip></networks>
      <profile>default</profile>
      <grants><query>GRANT ALL ON {_DATABASE}.*</query></grants>
    </{_USER}>
  </users>
</clickhouse>
""",
    )
    config_file = config / "config.xml"
    _write_xml(
        config_file,
        f"""<clickhouse>
  <logger>
    <level>warning</level>
    <log>{escape(str(log))}/server.log</log>
    <size>1M</size><count>2</count><console>false</console>
  </logger>
  <listen_host>{_HOST}</listen_host>
  <http_port>{port}</http_port>
  <keep_alive_timeout>5</keep_alive_timeout>
  <max_connections>64</max_connections>
  <path>{escape(str(data))}/</path>
  <tmp_path>{escape(str(data / "tmp"))}/</tmp_path>
  <users_config>{escape(str(config / "users.xml"))}</users_config>
  <macros><fixture_owner>{owner}</fixture_owner></macros>
  <max_server_memory_usage>2147483648</max_server_memory_usage>
  <mark_cache_size>33554432</mark_cache_size>
  <uncompressed_cache_size>16777216</uncompressed_cache_size>
  <max_thread_pool_size>256</max_thread_pool_size>
  <background_pool_size>4</background_pool_size>
  <background_schedule_pool_size>4</background_schedule_pool_size>
  <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <background_fetches_pool_size>2</background_fetches_pool_size>
  <background_move_pool_size>2</background_move_pool_size>
  <background_common_pool_size>2</background_common_pool_size>
  <merge_tree>
    <number_of_free_entries_in_pool_to_execute_mutation>4</number_of_free_entries_in_pool_to_execute_mutation>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>4</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>4</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
  </merge_tree>
  <mlock_executable>false</mlock_executable>
  <send_crash_reports><enabled>false</enabled></send_crash_reports>
</clickhouse>
""",
    )
    return config_file


def _query(
    connection: http.client.HTTPConnection,
    password: str,
    sql: str,
    expected: bytes,
    error_type: type[StartupError],
) -> None:
    sock = connection.sock
    if sock is None:
        raise error_type("ClickHouse fixture HTTP connection closed")
    sock.limit_wait()
    connection.request(
        "POST",
        "/?wait_end_of_query=1",
        body=sql,
        headers={"X-ClickHouse-User": _USER, "X-ClickHouse-Key": password},
    )
    with connection.getresponse() as response:
        if response.status != 200:
            raise error_type(f"ClickHouse fixture returned HTTP {response.status}")
        if response.getheader("X-ClickHouse-Exception-Code", "0") != "0":
            raise error_type("ClickHouse fixture reported a query error")
        body = response.read(len(expected) + 1)
        if (
            body != expected
            or not response.isclosed()
            or response.length not in (None, 0)
        ):
            raise error_type("ClickHouse fixture returned an unexpected response")


def _bootstrap(
    process: subprocess.Popen,
    port: int,
    password: str,
    owner: str,
    deadline: float,
    cancelled: Callable[[], bool],
) -> None:
    while True:
        _remaining(deadline, cancelled)
        returncode = process.poll()
        if returncode is not None:
            raise StartupExited(returncode)

        # 直接连 IPv4，不经过代理或重定向。认证与建库必须使用同一个连接。
        with (
            _SetupSocket(deadline, cancelled) as sock,
            closing(http.client.HTTPConnection(_HOST, port)) as connection,
        ):
            connection.auto_open = False
            try:
                sock.limit_wait()
                sock.connect((_HOST, port))
                connection.sock = sock
                _query(
                    connection,
                    password,
                    "SELECT getMacro('fixture_owner')",
                    (owner + "\n").encode("ascii"),
                    StartupOwnershipError,
                )
            except (ConnectionError, TimeoutError):
                time.sleep(min(_POLL_SECONDS, _remaining(deadline, cancelled)))
                continue
            except http.client.HTTPException:
                raise StartupOwnershipError(
                    "Invalid ClickHouse fixture HTTP response"
                ) from None

            try:
                _query(
                    connection,
                    password,
                    f"CREATE DATABASE {_DATABASE}",
                    b"",
                    StartupBootstrapError,
                )
            except OSError as error:
                raise StartupBootstrapError(
                    "ClickHouse fixture database outcome unknown"
                ) from error
            except http.client.HTTPException:
                raise StartupBootstrapError(
                    "Invalid database creation response; outcome unknown"
                ) from None
            except (StartupCancelled, StartupTimeout) as error:
                error.add_note("ClickHouse fixture database outcome may be unknown")
                raise
        return


def start(
    directory: pathlib.Path, binary: pathlib.Path, cancelled: Callable[[], bool]
) -> tuple[subprocess.Popen, dict[str, str]]:
    """Start ClickHouse in the caller's cgroup using an empty, caller-owned directory."""
    deadline = time.monotonic() + _SETUP_SECONDS
    _remaining(deadline, cancelled)
    binary = binary.resolve(strict=True)
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        if directory.is_symlink() or not directory.is_dir() or any(directory.iterdir()):
            raise StartupError(
                "ClickHouse fixture requires a new, empty directory"
            ) from None
    directory = directory.resolve(strict=True)
    directory.chmod(0o700)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.bind((_HOST, 0))
        port = candidate.getsockname()[1]
    password = secrets.token_hex(32)
    owner = secrets.token_hex(32)
    config = _configure(directory, port, password, owner)

    _remaining(deadline, cancelled)
    process = subprocess.Popen(
        [str(binary), "server", "--config-file", str(config)],
        cwd=directory,
        env={
            "HOME": str(directory),
            "TMPDIR": str(directory / "data" / "tmp"),
            "LANG": "C",
            # 让 Popen 直接管理前台服务进程。
            "CLICKHOUSE_WATCHDOG_ENABLE": "0",
        },
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        umask=0o077,
    )
    try:
        _bootstrap(process, port, password, owner, deadline, cancelled)
        _remaining(deadline, cancelled)
        returncode = process.poll()
        if returncode is not None:
            raise StartupExited(returncode)
        return process, {
            "LAB_CLICKHOUSE_DSN": f"http://{_HOST}:{port}",
            "LAB_CLICKHOUSE_USER": _USER,
            "LAB_CLICKHOUSE_PASSWORD": password,
            "LAB_CLICKHOUSE_DATABASE": _DATABASE,
        }
    except BaseException:
        # 只回收直接子进程，父级负责 cgroup 和目录清理。
        with suppress(OSError):
            process.terminate()
        try:
            process.wait(timeout=0.75)
        except subprocess.TimeoutExpired:
            with suppress(OSError):
                process.kill()
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=0.75)
        raise
