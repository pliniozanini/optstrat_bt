
import os
import pandas as pd
from pathlib import Path
from typing import Optional

# The environment variable we will look for
CACHE_ENV_VAR = "OPSTRAT_CACHE_DIR"
MEMORY_CACHE = {}

def get_cache_dir(custom_path: Optional[Path] = None) -> Path:
    """
    Determines the cache directory path based on a clear priority:
    1. A custom path provided directly to the function.
    2. The path specified in the OPSTRAT_CACHE_DIR environment variable.
    3. The default path in the user's home directory (~/.opstrat_cache).
    
    Creates the directory if it doesn't exist.
    
    Args:
        custom_path: Optional explicit path override
        
    Returns:
        Path: The resolved cache directory path
    """
    # 1. Check for a direct path override
    if custom_path:
        path = custom_path
    # 2. Check for an environment variable
    elif os.getenv(CACHE_ENV_VAR):
        path = Path(os.getenv(CACHE_ENV_VAR))
    # 3. Fall back to the default
    else:
        path = Path.home() / ".opstrat_cache"
        
    path.mkdir(parents=True, exist_ok=True)
    return path

def generate_key(data_type: str, symbol: str, period: str) -> str:
    """Generate a standardized cache key."""
    return f"{data_type}/{symbol}/{period}"

def get_from_cache(key: str, cache_dir: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """
    Retrieves a DataFrame from the cache.
    
    Args:
        key: The cache key to look up
        cache_dir: Optional custom cache directory path
        
    Returns:
        Optional[pd.DataFrame]: The cached DataFrame if found, None otherwise
    """
    if key in MEMORY_CACHE:
        return MEMORY_CACHE[key].copy()

    final_cache_dir = get_cache_dir(cache_dir)
    file_path = final_cache_dir / f"{key.replace('/', '_')}.parquet"
    
    if file_path.exists():
        df = pd.read_parquet(file_path)
        MEMORY_CACHE[key] = df.copy()
        return df
    return None

def set_to_cache(key: str, df: pd.DataFrame, cache_dir: Optional[Path] = None):
    """
    Saves a DataFrame to the cache.
    
    Args:
        key: The cache key to store under
        df: The DataFrame to cache
        cache_dir: Optional custom cache directory path
    """
    if df.empty:
        return
        
    MEMORY_CACHE[key] = df.copy()
    final_cache_dir = get_cache_dir(cache_dir)
    file_path = final_cache_dir / f"{key.replace('/', '_')}.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path)
