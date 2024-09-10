from bhakti.client.simple_reactive_client import SimpleReactiveClient


class SimpleDipamkaraReactiveClient(SimpleReactiveClient):
    def __init__(self, server: str, port: int, eof: bytes):
        super().__init__(server=server, port=port, eof=eof)
