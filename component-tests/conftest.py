"""Pytest fixtures for running the lwm2mserver-rs binary on loopback."""

import json
import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVER_BINARY = PROJECT_ROOT / "target" / "debug" / "lwm2mserver-rs"

# Arbitrary 16-byte key, hex encoded, for the --lb-key-file argument.
TEST_NETWORK_KEY = "000102030405060708090a0b0c0d0e0f"

STARTUP_TIMEOUT = 10.0


@dataclass
class Lwm2mServer:
    """Handle to a running lwm2mserver instance bound on loopback."""

    process: subprocess.Popen
    port: int
    log_file: Path

    @property
    def address(self) -> tuple[str, int]:
        """UDP address (host, port) the CoAP server is reachable on."""
        return ("::1", self.port)

    @property
    def uri(self) -> str:
        return f"coap://[::1]:{self.port}"

    def logs(self) -> str:
        return self.log_file.read_text()


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as sock:
        sock.bind(("::1", 0))
        return sock.getsockname()[1]


def _wait_for_startup(process: subprocess.Popen, log_file: Path) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        logs = log_file.read_text() if log_file.exists() else ""
        if process.poll() is not None:
            raise RuntimeError(
                f"lwm2mserver exited early (rc={process.returncode}):\n{logs}"
            )
        if "Server starting" in logs:
            return
        time.sleep(0.05)
    raise TimeoutError(f"lwm2mserver did not start within {STARTUP_TIMEOUT}s:\n{logs}")


@pytest.fixture(scope="session")
def server_binary() -> Path:
    """Build the server with cargo and return the path to the binary."""
    subprocess.run(["cargo", "build"], cwd=PROJECT_ROOT, check=True)
    assert SERVER_BINARY.is_file()
    return SERVER_BINARY


@pytest.fixture
def lwm2m_server(server_binary: Path, tmp_path: Path) -> Lwm2mServer:
    """Run the lwm2mserver on loopback and tear it down after the test."""
    key_file = tmp_path / "network_key.json"
    key_file.write_text(json.dumps({"network_key": TEST_NETWORK_KEY}))

    port = _free_udp_port()
    log_file = tmp_path / "lwm2mserver.log"

    with log_file.open("w") as log:
        process = subprocess.Popen(
            [
                str(server_binary),
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

    try:
        _wait_for_startup(process, log_file)
        yield Lwm2mServer(process=process, port=port, log_file=log_file)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
