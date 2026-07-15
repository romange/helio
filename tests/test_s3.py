from __future__ import annotations

import http.server
import os
import subprocess
import threading
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest


def _find_s3_demo() -> Path | None:
    env_dir = os.environ.get("HELIO_BIN_DIR")
    if env_dir:
        cand = Path(env_dir) / "s3_demo"
        if cand.exists():
            return cand

    root = Path(__file__).resolve().parent.parent
    for cand in [root / "build" / "s3_demo", root / "build-dbg" / "s3_demo"]:
        if cand.exists():
            return cand
    return None


@pytest.fixture(scope="module")
def s3_demo() -> Path:
    binary = _find_s3_demo()
    if not binary:
        pytest.skip("s3_demo binary not found")
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        pytest.skip("No AWS credentials in environment")
    return binary


def _run(binary: Path, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(binary), "--logtostderr", *args],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )


@pytest.fixture(scope="module")
def minio() -> dict:
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if not endpoint:
        pytest.skip("MINIO_ENDPOINT not set")
    binary = _find_s3_demo()
    if not binary:
        pytest.skip("s3_demo binary not found")
    env = {k: v for k, v in os.environ.items() if k not in ("AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN")}
    env.update({
        "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_S3_ENDPOINT": endpoint,
    })
    return {
        "binary": binary,
        "env": env,
        "bucket": os.environ.get("MINIO_BUCKET", "ci-test-bucket"),
    }


def test_put_get_object_minio(minio):
    key = "helio-ci-test/put_get_object.bin"
    upload_size = 10 * 1024 * 1024  # 10 MB: one full 8 MB part + 2 MB remainder

    result = _run(
        minio["binary"],
        "--cmd=put-object",
        f"--bucket={minio['bucket']}",
        f"--key={key}",
        f"--upload_size={upload_size}",
        env=minio["env"],
    )
    assert result.returncode == 0, f"put-object failed:\n{result.stderr}"
    assert f"put-object done; bytes={upload_size}" in result.stderr, result.stderr

    result = _run(
        minio["binary"],
        "--cmd=list-objects",
        f"--bucket={minio['bucket']}",
        f"--prefix={key}",
        env=minio["env"],
    )
    assert result.returncode == 0, f"list-objects failed:\n{result.stderr}"
    assert key in result.stdout, f"{key!r} not found in listing:\n{result.stdout}"

    result = _run(
        minio["binary"],
        "--cmd=get-object",
        f"--bucket={minio['bucket']}",
        f"--key={key}",
        env=minio["env"],
    )
    assert result.returncode == 0, f"get-object failed:\n{result.stderr}"
    assert f"get-object done; bytes={upload_size}" in result.stderr, result.stderr


def test_put_get_object(s3_demo):
    bucket = os.environ.get("S3_TEST_BUCKET")
    if not bucket:
        pytest.skip("S3_TEST_BUCKET not set")

    key = "helio-ci-test/put_get_object.bin"
    upload_size = 16 * 1024 * 1024  # 16 MB — two 8 MB parts

    result = _run(
        s3_demo,
        "--cmd=put-object",
        f"--bucket={bucket}",
        f"--key={key}",
        f"--upload_size={upload_size}",
    )
    assert result.returncode == 0, f"put-object failed:\n{result.stderr}"
    assert f"put-object done; bytes={upload_size}" in result.stderr, result.stderr

    result = _run(
        s3_demo, "--cmd=list-objects", f"--bucket={bucket}", f"--prefix={key}"
    )
    assert result.returncode == 0, f"list-objects failed:\n{result.stderr}"
    assert key in result.stdout, f"{key!r} not found in listing:\n{result.stdout}"

    result = _run(s3_demo, "--cmd=get-object", f"--bucket={bucket}", f"--key={key}")
    assert result.returncode == 0, f"get-object failed:\n{result.stderr}"
    assert f"get-object done; bytes={upload_size}" in result.stderr, result.stderr


def test_list_objects_with_prefix(s3_demo):
    """List with a non-empty prefix to exercise SigV4 signing of pre-encoded query params.

    When prefix is set, the URL contains percent-encoded characters (e.g. delimiter=%2F)
    and CanonicalQueryString must not re-encode them (which would produce %252F and break
    the signature).
    """
    bucket = os.environ.get("S3_TEST_BUCKET")
    if not bucket:
        pytest.skip("S3_TEST_BUCKET not set")
    result = _run(s3_demo, "--cmd=list-objects", f"--bucket={bucket}", "--prefix=nonexistent/path/")
    assert result.returncode == 0, f"list-objects with prefix failed:\n{result.stderr}"


def test_endpoint_flag_override(minio):
    """Pass the endpoint via --endpoint instead of AWS_S3_ENDPOINT to exercise
    AwsCredsProvider::SetEndpointOverride. The override must take precedence
    over (and work in the absence of) the env var."""
    endpoint = minio["env"]["AWS_S3_ENDPOINT"]
    env = {k: v for k, v in minio["env"].items() if k != "AWS_S3_ENDPOINT"}

    result = _run(
        minio["binary"],
        "--cmd=list-objects",
        f"--bucket={minio['bucket']}",
        f"--endpoint={endpoint}",
        env=env,
    )
    assert result.returncode == 0, f"list-objects with --endpoint failed:\n{result.stderr}"
    assert "-- page 0 end --" in result.stdout, result.stdout

    # Setting both: --endpoint should win. Point env at a bogus host; --endpoint
    # at the real minio.
    env_with_bogus = dict(env)
    env_with_bogus["AWS_S3_ENDPOINT"] = "http://does-not-resolve.invalid:1"
    result = _run(
        minio["binary"],
        "--cmd=list-objects",
        f"--bucket={minio['bucket']}",
        f"--endpoint={endpoint}",
        env=env_with_bogus,
    )
    assert result.returncode == 0, (
        f"--endpoint did not override AWS_S3_ENDPOINT:\n{result.stderr}"
    )


def _make_failing_upload_handler(*, connection_close: bool = False, end_of_stream: bool = False):
    """Build an http handler that accepts CreateMultipartUpload.
    If `end_of_stream=True`, drops the connection on the first PUT request
    (UploadPart), then succeeds with 200 OK and ETag on subsequent PUTs,
    and supports completing the upload.
    Otherwise, answers every UploadPart with 507 (out of capacity).
    `connection_close=True` mirrors MinIO's storage full response."""

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        put_count = 0

        def log_message(self, *args, **kwargs):
            pass

        def _send(self, status: int, body: bytes, *, close: bool, headers: dict | None = None):
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            if headers:
                for k, v in headers.items():
                    self.send_header(k, v)
            if close:
                self.send_header("Connection", "close")
                self.close_connection = True
            else:
                self.send_header("Connection", "keep-alive")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
            if "uploads" in query:
                body = (
                    b'<?xml version="1.0" encoding="UTF-8"?>'
                    b"<InitiateMultipartUploadResult>"
                    b"<Bucket>fake</Bucket><Key>fake</Key>"
                    b"<UploadId>fake-upload-id</UploadId>"
                    b"</InitiateMultipartUploadResult>"
                )
                self._send(200, body, close=False)
                return
            elif "uploadId" in query and end_of_stream:
                body = (
                    b'<?xml version="1.0" encoding="UTF-8"?>'
                    b"<CompleteMultipartUploadResult>"
                    b"<Bucket>fake</Bucket><Key>fake</Key>"
                    b"<ETag>fake-etag</ETag>"
                    b"</CompleteMultipartUploadResult>"
                )
                self._send(200, body, close=False)
                return
            self._send(404, b"<Error/>", close=False)

        def do_PUT(self):
            content_len = int(self.headers.get("Content-Length", "0"))
            if content_len:
                self.rfile.read(content_len)

            if end_of_stream:
                Handler.put_count += 1
                if Handler.put_count == 1:
                    self.close_connection = True
                    return
                self._send(200, b"", close=False, headers={"ETag": '"fake-etag"'})
                return

            body = (
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b"<Error><Code>XMinioStorageFull</Code>"
                b"<Message>Storage backend has reached its capacity</Message></Error>"
            )
            self._send(507, body, close=connection_close)

    return Handler


def _start_fake_s3(handler_cls):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{port}"


@pytest.fixture()
def failing_s3_endpoint_close():
    """Fake S3 that 507s UploadPart with `Connection: close` (MinIO out-of-disk)."""
    server, thread, url = _start_fake_s3(_make_failing_upload_handler(connection_close=True))
    try:
        yield url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture()
def failing_s3_endpoint_keepalive():
    """Fake S3 that 507s UploadPart but keeps the connection alive, so all
    retries hit the DoesServerPushback path in RobustSender::Send."""
    server, thread, url = _start_fake_s3(_make_failing_upload_handler(connection_close=False))
    try:
        yield url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture()
def end_of_stream_s3_endpoint():
    """Fake S3 that drops the first UploadPart request completely (end of stream)."""
    server, thread, url = _start_fake_s3(_make_failing_upload_handler(end_of_stream=True))
    try:
        yield url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _put_object_against(endpoint: str, *extra_args: str) -> subprocess.CompletedProcess:
    binary = _find_s3_demo()
    if not binary:
        pytest.skip("s3_demo binary not found")
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    env.update({
        "AWS_ACCESS_KEY_ID": "fake",
        "AWS_SECRET_ACCESS_KEY": "fake",
        "AWS_DEFAULT_REGION": "us-east-1",
    })
    return _run(
        binary,
        "--cmd=put-object",
        "--bucket=fake-bucket",
        "--key=fake/key.bin",
        "--upload_size=256",  # small upload size to make it faster.
        f"--endpoint={endpoint}",
        *extra_args,
        env=env,
    )


def test_upload_part_507_no_crash(failing_s3_endpoint_close):
    """Regression: dragonflydb/dragonfly#7410.

    A 507 on UploadPart with `Connection: close` caused a SIGABRT inside
    helio's socket Shutdown path (fd_ < 0 CHECK fail). After the fix, the
    upload must exit gracefully with a propagated error and no crash."""
    result = _put_object_against(failing_s3_endpoint_close)

    # SIGABRT shows up as a negative returncode (-signal.SIGABRT) on POSIX.
    assert result.returncode >= 0, (
        f"s3_demo terminated by signal {-result.returncode}:\n{result.stderr}"
    )
    assert "Check failed: fd_" not in result.stderr, (
        f"hit the LinuxSocketBase::Shutdown CHECK:\n{result.stderr}"
    )
    assert "put-object close error" in result.stderr or "put-object write error" in result.stderr, (
        f"expected a graceful upload error in stderr, got:\n{result.stderr}"
    )


def test_upload_part_507_retry_exhaustion_propagates_error(failing_s3_endpoint_keepalive):
    """Regression: RobustSender::Send must return an error after exhausting
    pushback retries. Before the fix, the for-loop fell out returning a
    default-constructed (success) error_code; s3_storage then tried to parse
    the (empty) response and logged `missing ETag in response`.

    With the keep-alive variant of the fake server, all 3 attempts take the
    DoesServerPushback path — exhausting retries cleanly so this path is
    actually exercised (the close variant short-circuits with end_of_stream
    on the third attempt's send)."""
    result = _put_object_against(failing_s3_endpoint_keepalive)

    assert result.returncode >= 0, (
        f"s3_demo terminated by signal {-result.returncode}:\n{result.stderr}"
    )
    # The smoking gun for the retry-exhaustion bug: with the bug present
    # Send returned success and s3_storage logged this while looking for ETag.
    assert "missing ETag in response" not in result.stderr, (
        "RobustSender returned success on retry-exhaustion; "
        "caller tried to parse the response and hit missing-ETag:\n" + result.stderr
    )
    # And we should still see at least 3 pushback retries having been attempted.
    assert result.stderr.count("Retrying(") >= 3, (
        f"expected RobustSender to exhaust 3 pushback retries:\n{result.stderr}"
    )
    assert "put-object close error" in result.stderr or "put-object write error" in result.stderr, (
        f"expected a graceful upload error in stderr, got:\n{result.stderr}"
    )


def test_upload_part_end_of_stream_retry(end_of_stream_s3_endpoint):
    """Test that if the first UploadPart gets a silent connection drop (end of stream),
    RobustSender retries and the upload ultimately succeeds."""
    result = _put_object_against(end_of_stream_s3_endpoint, "--v=1")

    assert result.returncode == 0, f"put-object failed:\n{result.stderr}"
    assert "Retrying. Connection closed with error: end of stream" in result.stderr, (
        f"expected end of stream retry message in stderr, got:\n{result.stderr}"
    )
    assert "put-object done; bytes=" in result.stderr, (
        f"expected successful put-object completion, got:\n{result.stderr}"
    )


def test_list_objects_cursor_pagination(minio):
    """Upload several objects under a unique prefix and verify cursor-driven listing
    returns every key across multiple pages."""
    prefix = "helio-ci-test/cursor-pagination/"
    num_objects = 5
    page_size = 2

    keys = [f"{prefix}obj_{i:02d}.bin" for i in range(num_objects)]
    for key in keys:
        result = _run(
            minio["binary"],
            "--cmd=put-object",
            f"--bucket={minio['bucket']}",
            f"--key={key}",
            "--upload_size=16",
            env=minio["env"],
        )
        assert result.returncode == 0, f"put-object {key} failed:\n{result.stderr}"

    result = _run(
        minio["binary"],
        "--cmd=list-objects",
        f"--bucket={minio['bucket']}",
        f"--prefix={prefix}",
        f"--list_max_results={page_size}",
        env=minio["env"],
    )
    assert result.returncode == 0, f"list-objects failed:\n{result.stderr}"

    for key in keys:
        assert key in result.stdout, f"{key!r} missing from listing:\n{result.stdout}"

    # With 5 objects at page size 2: pages 0 and 1 each return 2 objects (truncated),
    # page 2 returns 1 object (final). Expect at least 3 page markers.
    expected_pages = (num_objects + page_size - 1) // page_size
    for i in range(expected_pages):
        assert f"-- page {i} end --" in result.stdout, (
            f"missing page {i} marker in stdout:\n{result.stdout}"
        )
