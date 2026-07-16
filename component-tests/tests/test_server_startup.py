import socket

from conftest import Lwm2mServer


def test_server_starts_on_loopback(lwm2m_server: Lwm2mServer):
    assert lwm2m_server.process.poll() is None

    # The UDP port must be taken by the server: binding it ourselves fails.
    with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as sock:
        try:
            sock.bind(("::", lwm2m_server.port))
        except OSError:
            pass
        else:
            raise AssertionError(f"port {lwm2m_server.port} is not bound")
