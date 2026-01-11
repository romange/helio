from __future__ import annotations

import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
import logging


logger = logging.getLogger(__name__)


def _extract_int_arg(args: list[str], name: str) -> int | None:
    """Helper to extract an integer argument like --name=value or --name value."""
    prefix = f"--{name}="
    for idx, arg in enumerate(args):
        if arg.startswith(prefix):
            return int(arg.split("=", 1)[1])
        if arg == f"--{name}" and idx + 1 < len(args):
            return int(args[idx + 1])
    return None


def _extract_port(args: list[str]) -> int:
    port = _extract_int_arg(args, "port")
    return port if port is not None else 8081


def _extract_size(args: list[str]) -> int | None:
    return _extract_int_arg(args, "size")


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

def _find_echo_binary() -> Path | None:
    root = Path(__file__).resolve().parent.parent
    logging.info("Looking for echo_server binary under %s", root)
    candidates = [
        root / "build" / "echo_server",
        root / "build-dbg" / "echo_server",
        root / "build-opt" / "echo_server",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    return None


class EchoServerHarness:
    def __init__(self, binary: Path):
        self.binary = binary
        self.proc: subprocess.Popen | None = None
        self._log = []
        self._reader: threading.Thread | None = None
        self._port = 8081
        self.size: int | None = None

    def start(self, server_args=None, ready_timeout: float = 10.0, port: int | None = None):
        if self.proc:
            raise RuntimeError("echo_server already running")

        args = [str(self.binary), "--alsologtostderr"]
        if server_args:
            args.extend(server_args)

        if port is None:
            port = _pick_free_port()
            args.extend(["--port", str(port)])

        self._port = _extract_port(args)
        self.size = _extract_size(args)
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        self._reader = threading.Thread(target=self._drain_stdout, daemon=True)
        self._reader.start()
        self._wait_for_port(ready_timeout)
        return self

    def _drain_stdout(self):
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            self._log.append(line.rstrip())

    def _wait_for_port(self, timeout: float):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(self.format_failure("echo_server exited early"))
            try:
                with socket.create_connection(("127.0.0.1", self._port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        raise TimeoutError(
            self.format_failure(f"Timed out waiting for echo_server on port {self._port}")
        )

    def run_client(self, client_args=None, timeout: float = 30.0) -> subprocess.CompletedProcess:
        args = [str(self.binary), "--connect", "localhost"]
        # Ensure client uses the server port unless caller overrides.
        if not client_args or not any(a.startswith("--port") or a == "--port" for a in client_args):
            args.extend(["--port", str(self._port)])
        if client_args:
            args.extend(client_args)
        return subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )

    def assert_client_ok(self, client_args=None, timeout: float = 30.0):
        result = self.run_client(client_args=client_args, timeout=timeout)
        # Surface client output even on success for observability.
        if result.stdout:
            logger.info("[client stdout] %s", result.stdout.rstrip())
        if result.stderr:
            logger.info("[client stderr] %s", result.stderr.rstrip())
        if result.returncode != 0:
            pytest.fail(self.format_failure("Client failed", result), pytrace=False)
        return result

    def format_failure(self, message: str, client_result: subprocess.CompletedProcess | None = None):
        lines = [message]
        if self.proc:
            lines.append(f"server returncode: {self.proc.returncode}")
        if self._log:
            lines.append("server output:")
            lines.extend(self._log)
        if client_result:
            lines.append(f"client returncode: {client_result.returncode}")
            lines.append("client stdout:")
            lines.append(client_result.stdout.rstrip())
            lines.append("client stderr:")
            lines.append(client_result.stderr.rstrip())
        return "\n".join(lines)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self._reader:
            self._reader.join(timeout=2)
        self.proc = None
        self._reader = None


@pytest.fixture
def echo_server(request):
    binary = _find_echo_binary()
    if not binary:
        pytest.skip("echo_server binary is missing; expected under build*/echo_server")

    server_args = []
    marker = request.node.get_closest_marker("server_args")
    if marker:
        if marker.args:
            server_args = list(marker.args[0])
        server_args.extend(marker.kwargs.get("args", []))
        fmt_ctx = {}
        callspec = getattr(request.node, "callspec", None)
        if callspec and hasattr(callspec, "params"):
            fmt_ctx = callspec.params
        elif request.node.funcargs:
            fmt_ctx = request.node.funcargs

        if fmt_ctx:
            server_args = [
                (arg.format(**fmt_ctx) if isinstance(arg, str) else arg)
                for arg in server_args
            ]

    harness = EchoServerHarness(binary)
    try:
        harness.start(server_args=server_args)
        yield harness
    finally:
        harness.stop()


def test_echo_server_default_roundtrip(echo_server):
    echo_server.assert_client_ok()


@pytest.mark.server_args(["--size", "32"])
def test_echo_server_custom_size(echo_server):
    client_flags = []
    if echo_server.size is not None:
        client_flags = ["--size", str(echo_server.size)]
    echo_server.assert_client_ok(client_args=client_flags)