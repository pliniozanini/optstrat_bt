import pytest
import tempfile
import shutil
from unittest.mock import MagicMock

@pytest.fixture
def mock_oplab_client():
    """Provides a generic MagicMock of the OplabClient and ensures no cache interference."""
    # Create an isolated temporary cache directory and clear in-memory cache
    tmpdir = tempfile.mkdtemp(prefix="opstrat_test_cache_")
    try:
        with pytest.MonkeyPatch().context() as mp:
            # Point the cache manager to an empty temporary directory
            mp.setenv("OPSTRAT_CACHE_DIR", tmpdir)
            # Clear any in-memory cache
            try:
                from opstrat_backtester import cache_manager
                cache_manager.MEMORY_CACHE.clear()
            except Exception:
                pass

            # Provide a mock client instance and patch the constructor used in data_loader
            mock_client_instance = MagicMock()
            mp.setattr("opstrat_backtester.data_loader.OplabClient", lambda: mock_client_instance)
            yield mock_client_instance
    finally:
        # Clean up temporary cache directory
        shutil.rmtree(tmpdir, ignore_errors=True)