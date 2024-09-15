import asyncio


def sync(func: callable):
    def wrapper(*args, **kwargs):
        try:
            return asyncio.run(func(*args, **kwargs))
        except KeyboardInterrupt:
            exit(0)
    return wrapper
