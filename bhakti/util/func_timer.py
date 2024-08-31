import datetime


def time_kept(func):
    def wrapper(*args, **kwargs):
        start = datetime.datetime.now().timestamp()
        func(*args, **kwargs)
        end = datetime.datetime.now().timestamp()
        print(f'Time used by "{func.__name__}()": {end - start} s | {(end - start) * 1000} ms')
        return
    return wrapper
