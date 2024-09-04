import asyncio


def lock_on(lock: asyncio.Lock):
    def decorator(func: callable):
        async def wrapper(*args, **kwargs):
            async with lock:
                return await func(*args, **kwargs)
        return wrapper
    return decorator
