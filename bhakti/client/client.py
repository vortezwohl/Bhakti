import asyncio

from bhakti.util import log
from bhakti.const import DEFAULT_EOF


class NioClient:
    def __init__(self, server='127.0.0.1', port=23860):
        self.server = server
        self.port = port

    async def send_receive(self, message: str):
        reader, writer = await asyncio.open_connection(
            self.server, self.port)

        log.debug(f'Connection {self.server}:{self.port} claimed')

        try:
            # 发送消息
            writer.write(message.encode())
            await writer.drain()

            # # 等待并接收响应
            # data = await reader.readline()
            # log.debug(f'Data received: {data.decode()}')

        except Exception as e:
            log.error(f'Exception: {e}')
        finally:
            # 关闭连接
            writer.close()
            await writer.wait_closed()
            log.debug('Connection closed')


if __name__ == '__main__':
    client_message = "Hello, Server!" + DEFAULT_EOF.decode('utf-8')
    print(client_message)
    client = NioClient()
    asyncio.run(client.send_receive(message=client_message))