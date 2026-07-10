"""Pytest fixtures for running an LwM2M server implementation on loopback."""

import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from lwm2mserver_rs import RustServer


@dataclass
class Lwm2mServer:
    """Handle to a running LwM2M server instance bound on loopback."""

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


def _wait_for_startup(process: subprocess.Popen, log_file: Path, impl) -> None:
    deadline = time.monotonic() + impl.startup_timeout
    while time.monotonic() < deadline:
        logs = log_file.read_text() if log_file.exists() else ""
        if process.poll() is not None:
            raise RuntimeError(
                f"{impl.name} server exited early (rc={process.returncode}):\n{logs}"
            )
        if impl.startup_marker in logs:
            return
        time.sleep(0.05)
    raise TimeoutError(
        f"{impl.name} server did not start within {impl.startup_timeout}s:\n{logs}"
    )


@pytest.fixture(scope="session")
def server_impl(request: pytest.FixtureRequest):
    server = RustServer()
    server.build()
    return server


@pytest.fixture
def lwm2m_server(server_impl, tmp_path: Path) -> Lwm2mServer:
    """Run the server implementation on loopback and tear it down after the test."""
    port = _free_udp_port()
    log_file = tmp_path / "lwm2mserver.log"

    with log_file.open("w") as log:
        process = server_impl.start(port=port, log=log, work_dir=tmp_path)

    try:
        _wait_for_startup(process, log_file, server_impl)
        yield Lwm2mServer(process=process, port=port, log_file=log_file)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
