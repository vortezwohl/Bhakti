import asyncio

from bhakti.const import DEFAULT_EOF
from bhakti.util import log


class SimpleReactiveClient:
    def __init__(
            self,
            server: str = '127.0.0.1',
            port: int = 23860,
            eof: bytes = DEFAULT_EOF,
    ):
        self.__server = server
        self.__port = port
        self.__eof = eof

    async def send_receive(self, message: bytes) -> bytes | None:
        reader, writer = await asyncio.open_connection(
            self.__server, self.__port)
        log.debug(f'Connected to {self.__server}:{self.__port}')
        try:
            writer.write(message + self.__eof)
            await writer.drain()
            data = await reader.readuntil(self.__eof)
            log.debug(f'Data received: {data}')
            return data
        except Exception as e:
            log.error(f'Exception: {e}')
            return None
        finally:
            writer.close()
            await writer.wait_closed()
            log.debug('Connection closed')
