import asyncio


def await_repeat(_repeat: int):
    def decorator(func: callable):
        async def wrapper(*args, **kwargs) -> list:
            results = list()
            for _ in range(_repeat):
                results.append(await func(*args, **kwargs))
            return results
        return wrapper
    return decorator


def async_repeat(_repeat: int):
    def decorator(func: callable):
        async def wrapper(*args, **kwargs) -> list:
            results = list()
            tasks = list()
            for _ in range(_repeat):
                tasks.append(asyncio.create_task(func(*args, **kwargs)))
            for task in tasks:
                results.append(await task)
            return results
        return wrapper
    return decorator
