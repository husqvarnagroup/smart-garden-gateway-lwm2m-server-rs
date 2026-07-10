from aiocoap.numbers.codes import Code

from conftest import Lwm2mServer
from lwm2m_client import Lwm2mClient

ENDPOINT = "urn:sgtin:3034F87.000B1.400BYYcXA3"
SGTIN = "400BYYcXA3"


async def test_registration(lwm2m_server: Lwm2mServer):
    async with Lwm2mClient(lwm2m_server.uri, ENDPOINT) as client:
        response = await client.register()

    assert response.code == Code.CREATED
    assert client.registration_path is not None
    assert client.registration_path.startswith("rd/")

    assert "Device registered" in lwm2m_server.logs()
    assert SGTIN in lwm2m_server.logs()
