"""Minimal LwM2M client based on aiocoap, for driving the lwm2mserver in tests."""

from aiocoap import Context, Message
from aiocoap.numbers.codes import Code
from aiocoap.numbers.contentformat import ContentFormat

DEFAULT_OBJECTS = "</1/0>,</3/0>,</3303/0>"


class Lwm2mClient:
    """A CoAP client that speaks just enough LwM2M for component tests."""

    def __init__(
        self,
        server_uri: str,
        endpoint: str,
        lifetime: int = 86400,
        lwm2m_version: str = "1.1",
        binding_mode: str = "U",
    ):
        self.server_uri = server_uri
        self.endpoint = endpoint
        self.lifetime = lifetime
        self.lwm2m_version = lwm2m_version
        self.binding_mode = binding_mode
        self.registration_path: str | None = None
        self._context: Context | None = None

    async def __aenter__(self) -> "Lwm2mClient":
        self._context = await Context.create_client_context()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._context.shutdown()
        self._context = None

    async def register(self, objects: str = DEFAULT_OBJECTS) -> Message:
        """Send a registration (POST /rd) and return the server's response.

        On success (2.01 Created) the assigned registration path (e.g. "rd/1")
        is stored in self.registration_path for later updates/deregistration.
        """
        query = "&".join(
            [
                f"ep={self.endpoint}",
                f"lt={self.lifetime}",
                f"lwm2m={self.lwm2m_version}",
                f"b={self.binding_mode}",
            ]
        )
        request = Message(
            code=Code.POST,
            uri=f"{self.server_uri}/rd?{query}",
            payload=objects.encode(),
            content_format=ContentFormat.LINKFORMAT,
        )
        response = await self._context.request(request).response

        if response.code == Code.CREATED:
            self.registration_path = "/".join(response.opt.location_path)
        return response
