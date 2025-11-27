import os
from pathlib import Path
from typing import Optional

import fasteners
import pandas as pd

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
    """Retrieve a DataFrame from cache with file locking.

    Uses file locking to prevent concurrent access and preserves corrupted files
    for manual inspection.

    Args:
        key: The cache key to look up
        cache_dir: Optional custom cache directory path

    Returns:
        Optional[pd.DataFrame]: Cached DataFrame if found and valid, else None
    """
    if key in MEMORY_CACHE:
        return MEMORY_CACHE[key].copy()

    final_cache_dir = get_cache_dir(cache_dir)
    final_cache_dir.mkdir(parents=True, exist_ok=True)
    file_path = final_cache_dir / f"{key.replace('/', '_')}.parquet"
    lock_path = file_path.with_suffix('.lock')
    
    if not file_path.exists():
        return None
        
    lock = fasteners.InterProcessLock(str(lock_path))
    
    try:
        with lock:
            try:
                df = pd.read_parquet(file_path)
                MEMORY_CACHE[key] = df.copy()
                return df
            except Exception as e:
                # Don't remove the file, just log the error and return None
                msg = f"Warning: Cache file {file_path} appears corrupted: {e}"
                print(msg)
                print("The file has been preserved for manual inspection.")
                return None
    except Exception as e:
        print(f"Error acquiring lock for {file_path}: {e}")
        return None

def set_to_cache(key: str, df: pd.DataFrame, cache_dir: Optional[Path] = None) -> None:
    """Save a DataFrame to the cache with file locking.
    
    Uses file locking to ensure thread-safe writes and prevent corruption.
    
    Args:
        key: The cache key to store under
        df: The DataFrame to cache
        cache_dir: Optional custom cache directory path
        
    Raises:
        RuntimeError: If the file cannot be written or locked
    """
    if df.empty:
        return
        
    MEMORY_CACHE[key] = df.copy()
    final_cache_dir = get_cache_dir(cache_dir)
    final_cache_dir.mkdir(parents=True, exist_ok=True)
    file_path = final_cache_dir / f"{key.replace('/', '_')}.parquet"
    lock_path = file_path.with_suffix('.lock')
    
    lock = fasteners.InterProcessLock(str(lock_path))
    
    try:
        with lock:
            # Write to a temporary file first
            temp_path = file_path.with_suffix('.tmp')
            try:
                df.to_parquet(temp_path)
                # Atomically replace the old file with the new one
                if file_path.exists():
                    file_path.unlink()
                temp_path.rename(file_path)
            except Exception as e:
                # Clean up temporary file if something goes wrong
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                raise RuntimeError(f"Failed to write cache file {file_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to acquire lock for {file_path}: {e}")
    finally:
        # Clean up the lock file if it exists
        if lock_path.exists():
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass
