
import time 
import requests

from functools import wraps

from .config import settings


def rate_limited(_calls_per_second=None):
    """Dynamic rate limiting using global settings."""
    def decorator(func):
        last_called = [0.0]
        interval = 1.0 / (_calls_per_second or settings.rate_limit)

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal interval
            now = time.time()
            elapsed = now - last_called[0]
            wait = interval - elapsed
            if wait > 0:
                time.sleep(wait)
            last_called[0] = time.time()
            return func(*args, **kwargs)
        return wrapper
    return decorator

def retry_on_failure():
    """Dynamic retries/backoff using global settings."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(settings.retries):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException,) as e:
                    if attempt < settings.retries - 1:
                        wait = settings.backoff * (2 ** attempt)
                        print(f"[Retry {attempt+1}/{settings.retries}] {e}, retrying in {wait:.1f}s...")
                        time.sleep(wait)
                    else:
                        raise
        return wrapper
    return decorator


# def rate_limited(max_per_second: float):
#     """Decorator that prevents a function from being called more than
#     `max_per_second` times per second."""
#     min_interval = 1.0 / max_per_second

#     def decorator(func):
#         last_time_called = [0.0]

#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             elapsed = time.perf_counter() - last_time_called[0]
#             wait = min_interval - elapsed
#             if wait > 0:
#                 time.sleep(wait)
#             result = func(*args, **kwargs)
#             last_time_called[0] = time.perf_counter()
#             return result

#         return wrapper
#     return decorator

# def retry_on_failure(retries: int = 5, backoff: float = 0.5, exceptions=(requests.exceptions.RequestException,)):
#     """
#     Decorator to retry a function with exponential backoff if certain exceptions occur.
    
#     Parameters
#     ----------
#     retries : int
#         Maximum number of attempts.
#     backoff : float
#         Initial backoff time in seconds.
#     exceptions : tuple
#         Exceptions that trigger a retry.
#     """
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             for attempt in range(retries):
#                 try:
#                     return func(*args, **kwargs)
#                 except exceptions as e:
#                     if attempt < retries - 1:
#                         wait = backoff * (2 ** attempt)
#                         print(f"[Retry {attempt+1}/{retries}] {e}, retrying in {wait:.1f}s...")
#                         time.sleep(wait)
#                     else:
#                         raise
#         return wrapper
#     return decorator