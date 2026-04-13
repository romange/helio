from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
import uuid
from pathlib import Path

import pytest

AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)
AZURITE_PORT = 10000
AZURITE_CONN_STR = (
    f"DefaultEndpointsProtocol=http;"
    f"AccountName={AZURITE_ACCOUNT};"
    f"AccountKey={AZURITE_KEY};"
    f"BlobEndpoint=http://127.0.0.1:{AZURITE_PORT}/{AZURITE_ACCOUNT};"
)
CONTAINER = f"ci-test-{uuid.uuid4().hex[:8]}"


def _find_gcs_demo() -> Path | None:
    env_dir = os.environ.get("HELIO_BIN_DIR")
    if env_dir:
        cand = Path(env_dir) / "gcs_demo"
        if cand.exists():
            return cand

    root = Path(__file__).resolve().parent.parent
    for base in [root, root.parent]:
        for cand in [base / "build" / "gcs_demo", base / "build-dbg" / "gcs_demo"]:
            if cand.exists():
                return cand
    return None


def _azurite_running() -> bool:
    """Check whether Azurite is listening on the expected port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", AZURITE_PORT)) == 0


@pytest.fixture(scope="module")
def azurite():
    """Ensure Azurite is running. Try in order: already running (CI), start as
    subprocess, skip."""
    if _azurite_running():
        yield None  # Already running (e.g. started by CI step).
        return

    azurite_bin = shutil.which("azurite-blob")
    if not azurite_bin:
        pytest.skip("azurite-blob not found and Azurite is not already running")

    proc = subprocess.Popen(
        [azurite_bin, "--blobHost", "0.0.0.0", "--skipApiVersionCheck"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(30):
        if _azurite_running():
            break
        time.sleep(0.5)
    else:
        proc.kill()
        pytest.skip("Azurite did not become ready in time")

    yield proc

    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)


@pytest.fixture(scope="module")
def gcs_demo(azurite) -> Path:
    binary = _find_gcs_demo()
    if not binary:
        pytest.skip("gcs_demo binary not found")
    return binary


@pytest.fixture(scope="module")
def az_env() -> dict:
    """Environment dict with Azurite credentials."""
    env = os.environ.copy()
    env["AZURE_STORAGE_CONNECTION_STRING"] = AZURITE_CONN_STR
    return env


@pytest.fixture(scope="module", autouse=True)
def create_container(azurite, az_env):
    """Ensure the test container exists in Azurite."""
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.blob import BlobServiceClient

    client = BlobServiceClient.from_connection_string(AZURITE_CONN_STR)
    try:
        client.create_container(CONTAINER)
    except ResourceExistsError:
        pass


def _run(binary: Path, *args: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(binary), "--logtostderr", "--azure", *args],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )


def test_list_empty(gcs_demo, az_env):
    """Listing an empty container should succeed."""
    result = _run(gcs_demo, f"--bucket={CONTAINER}", env=az_env)
    assert result.returncode == 0, f"list failed:\n{result.stderr}"


def test_write_read(gcs_demo, az_env):
    """Write a blob and read it back, verifying sizes match."""
    prefix = "helio-ci/blob"
    result = _run(
        gcs_demo, f"--bucket={CONTAINER}", f"--prefix={prefix}", "--write=1", env=az_env
    )
    assert result.returncode == 0, f"write failed:\n{result.stderr}"
    assert "Written" in result.stderr, result.stderr

    result = _run(
        gcs_demo,
        f"--bucket={CONTAINER}",
        f"--prefix={prefix}_0",
        "--read=1",
        env=az_env,
    )
    assert result.returncode == 0, f"read failed:\n{result.stderr}"
    assert "Read" in result.stderr, result.stderr


def test_list_after_write(gcs_demo, az_env):
    """After writing, the blob should appear in a list."""
    result = _run(gcs_demo, f"--bucket={CONTAINER}", "--prefix=helio-ci/", env=az_env)
    assert result.returncode == 0, f"list failed:\n{result.stderr}"
    assert "helio-ci/" in result.stderr, f"expected blob not listed:\n{result.stderr}"


def test_list_containers(gcs_demo, az_env):
    """ListContainers should return at least the test container."""
    result = _run(gcs_demo, env=az_env)
    assert result.returncode == 0, f"list containers failed:\n{result.stderr}"
    assert CONTAINER in result.stderr, f"container not listed:\n{result.stderr}"
