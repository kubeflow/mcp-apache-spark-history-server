import time


def backoff_retry(delay=2, retries=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            current_retry = 0
            current_delay = delay
            while current_retry < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    current_retry += 1
                    if current_retry >= retries:
                        raise e
                    time.sleep(current_delay)
                    current_delay *= 2
        return wrapper
    return decorator
