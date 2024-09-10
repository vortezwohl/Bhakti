import asyncio


def timeout(_timeout: float):
    def decorator(func: callable):
        async def wrapper(*args, **kwargs):
            return await asyncio.wait_for(func(*args, **kwargs), timeout=_timeout)
        return wrapper
    return decorator
