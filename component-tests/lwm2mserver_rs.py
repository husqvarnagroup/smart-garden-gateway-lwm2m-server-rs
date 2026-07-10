"""The LwM2M server implementation under test.

The component tests run against a single server implementation per test run,
selected with the ``--server`` pytest option (see ``conftest.py``). Each
implementation knows how to build itself and how to spawn a process that listens
for CoAP traffic on ``::1`` (IPv6 loopback).

Adding another implementation means adding a class here (with ``name``,
``startup_marker``, ``build`` and ``start``) and a branch in
``conftest._select_server``.
"""

import json
import subprocess
from pathlib import Path
from typing import IO

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Arbitrary 16-byte key, hex encoded, passed to implementations that need one.
TEST_NETWORK_KEY = "000102030405060708090a0b0c0d0e0f"


class RustServer:
    """The ``lwm2mserver-rs`` binary built from this repository."""

    #: Short identifier, also the value accepted by ``--server``.
    name = "rust"

    #: Substring that appears in the server's log output once it is ready to
    #: accept requests.
    startup_marker = "Server starting"

    #: How long to wait for ``startup_marker`` to show up, in seconds.
    startup_timeout = 10.0

    BINARY = PROJECT_ROOT / "target" / "debug" / "lwm2mserver-rs"

    def build(self) -> None:
        """Build the server. Called once per test session."""
        subprocess.run(["cargo", "build"], cwd=PROJECT_ROOT, check=True)
        assert self.BINARY.is_file()

    def start(self, *, port: int, log: IO[str], work_dir: Path) -> subprocess.Popen:
        """Spawn the server process.

        It listens for CoAP traffic on ``[::1]:port`` and writes all of its log
        output to ``log``. ``work_dir`` is a scratch directory private to the
        test that may be used for generated config files.
        """
        key_file = work_dir / "network_key.json"
        key_file.write_text(json.dumps({"network_key": TEST_NETWORK_KEY}))

        return subprocess.Popen(
            [
                str(self.BINARY),
                "lo",
                "--port",
                str(port),
                "--server-uri",
                f"coap://[::1]:{port}",
                "--lb-key-file",
                str(key_file),
                "--no-encryption",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            env={"RUST_LOG": "lwm2mserver_rs=debug", "PATH": "/usr/bin:/bin"},
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name!r}>"
