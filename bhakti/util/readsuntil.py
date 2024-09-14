import asyncio
from asyncio import StreamReader


async def readsuntil(reader: StreamReader, buffer_size: int, until: bytes, timeout: float) -> bytes:
    blocks: bytes = bytes(b'')
    while True:
        blocks += await asyncio.wait_for(reader.read(n=buffer_size), timeout)
        if until in blocks:
            return blocks

