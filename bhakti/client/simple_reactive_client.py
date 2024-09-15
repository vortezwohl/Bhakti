import asyncio
import logging

from bhakti.const import DEFAULT_EOF, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_PORT
from bhakti.util.readsuntil import readsuntil

log = logging.getLogger("bhakti.client")

READ_TIMEOUT = 0
CONNECTION_REFUSED = 1


class SimpleReactiveClient:
    def __init__(
            self,
            server: str = '127.0.0.1',
            port: int = DEFAULT_PORT,
            eof: bytes = DEFAULT_EOF,
            timeout: float = DEFAULT_TIMEOUT,
            buffer_size: int = DEFAULT_BUFFER_SIZE
    ):
        self.__server = server
        self.__port = port
        self.__eof = eof
        self.__timeout = timeout
        self.__buffer_size = buffer_size

    # 读取超时或连接被拒绝时返回 None
    async def send_receive(self, message: bytes) -> bytes | int:
        try:
            reader, writer = await asyncio.open_connection(
                host=self.__server, port=self.__port, limit=self.__buffer_size)
            log.debug(f'Connected to {self.__server}:{self.__port}')
            try:
                writer.write(message + self.__eof)
                await writer.drain()
                data = await readsuntil(
                    reader=reader,
                    buffer_size=self.__buffer_size,
                    until=self.__eof,
                    timeout=self.__timeout
                )
                log.debug(f'Data received: {data}')
                return data
            except asyncio.TimeoutError:
                return READ_TIMEOUT
            finally:
                writer.close()
                await writer.wait_closed()
                log.debug('Connection closed')
        except ConnectionRefusedError:
            return CONNECTION_REFUSED
