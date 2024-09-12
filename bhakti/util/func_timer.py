import datetime


def time_kept(func: callable):
    def wrapper(*args, **kwargs):
        start = datetime.datetime.now().timestamp()
        ret = func(*args, **kwargs)
        end = datetime.datetime.now().timestamp()
        print(f'Time used by "{func.__name__}()": {end - start} s | {(end - start) * 1000} ms')
        return ret
    return wrapper
