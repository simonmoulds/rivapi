
import os 
import sys
import shutil
import requests_cache
from pathlib import Path
from platformdirs import user_cache_dir

CACHE_DIR = Path(user_cache_dir('rivapi', 'rivapi'))
CACHE_NAME = CACHE_DIR / 'rivapi_cache'
CACHE_BACKEND = 'sqlite'

def install_cache(
    expire_after: int = 86400,
    **kwargs,
):
    """
    Install a persistent requests cache for API calls.

    Parameters
    ----------
    backend : str
        Backend type: "sqlite", "filesystem", "memory", etc.
    cache_name : str
        Path or name for the cache. For SQLite, this is the database file prefix.
    expire_after : int
        Expiration time in seconds. Default = 1 day.
    kwargs : dict
        Additional arguments passed to `requests_cache.install_cache`.
    """
    requests_cache.install_cache(
        cache_name=CACHE_NAME,
        backend=CACHE_BACKEND,
        expire_after=expire_after,
        **kwargs,
    )
    return None


def remove_cache(): 
    cache_path = f'{CACHE_NAME}.{CACHE_BACKEND}'
    if os.path.exists(cache_path):
        os.remove(cache_path)
        if not os.listdir(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
    return None
