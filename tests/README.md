# Python integration tests

Run these commands from the repository root. Build the test binaries first:

```bash
ninja -C build-dbg echo_server gcs_demo
```

Install the Python dependencies used by the Azure tests:

```bash
python3 -m pip install pytest azure-storage-blob
```

## Azure tests

The Azure tests use the Azurite Blob Storage emulator. Install it with the same command used in CI:

```bash
npm install -g azurite
```

Start Azurite in a separate terminal:

```bash
azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
```

It listens on `http://127.0.0.1:10000`. Confirm it is ready before testing:

```bash
curl -sf http://127.0.0.1:10000/ ||
  test "$(curl -so /dev/null -w '%{http_code}' http://127.0.0.1:10000/)" = 400
```

Run the Azure tests:

```bash
python3 -m pytest -q -s tests/test_azure.py
```

`test_azure.py` also starts and stops Azurite itself when `azurite-blob` is on `PATH`. Starting it
manually is useful for inspecting emulator logs and matches the CI setup.

## Running the full Python suite

```bash
python3 -m pytest -q -s tests
```

Tests that require an unavailable binary, service, or credentials skip themselves. Set
`HELIO_BIN_DIR` to a directory containing built binaries when they are not under `build/` or
`build-dbg/`.
