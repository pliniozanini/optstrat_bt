import pytest
import pandas as pd
from unittest.mock import MagicMock
from .fixtures.mock_api_data import (
    MOCK_OPTIONS_LIST,
    MOCK_INSTRUMENTS_DETAILS,
    MOCK_STOCK_HISTORY
)

@pytest.fixture
def mock_oplab_client(monkeypatch):
    """
    This fixture provides a mock of the OplabClient.
    It patches the client at the source, so any code that imports and uses it
    will get this mock instead of the real one during tests.
    """
    # Import OplabClient here to ensure the mock_env_token fixture is already applied
    from opstrat_backtester.api_client import OplabClient
    
    # Create a mock instance of the client
    class MockOplabClient:
        def historical_options(self, spot, date_str):
            return pd.DataFrame(MOCK_OPTIONS_LIST['data'])

        def historical_instruments_details(self, tickers, date_str):
            return pd.DataFrame(MOCK_INSTRUMENTS_DETAILS['data'])

        def historical_stock(self, symbol, start_date, end_date):
            return pd.DataFrame(MOCK_STOCK_HISTORY['data'])
    
    return MockOplabClient()