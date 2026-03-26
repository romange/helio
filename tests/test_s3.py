from __future__ import annotations

import os
import subprocess
from pathlib import Path

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
        minio["binary"], "--cmd=list-objects", f"--bucket={minio['bucket']}", env=minio["env"]
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

    result = _run(s3_demo, "--cmd=list-objects", f"--bucket={bucket}")
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
