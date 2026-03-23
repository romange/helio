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


def _run(binary: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(binary), "--logtostderr", *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_list_objects(s3_demo):
    bucket = os.environ.get("S3_TEST_BUCKET")
    if not bucket:
        pytest.skip("S3_TEST_BUCKET not set")
    result = _run(s3_demo, "--cmd=list-objects", f"--bucket={bucket}")
    assert result.returncode == 0, f"list-objects failed:\n{result.stderr}"


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

    result = _run(s3_demo, "--cmd=get-object", f"--bucket={bucket}", f"--key={key}")
    assert result.returncode == 0, f"get-object failed:\n{result.stderr}"
    assert f"get-object done; bytes={upload_size}" in result.stderr, result.stderr
