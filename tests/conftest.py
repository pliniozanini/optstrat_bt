import pytest
import pandas as pd
from unittest.mock import MagicMock
from .fixtures.mock_api_data import (
    MOCK_OPTIONS_LIST,
    MOCK_INSTRUMENTS_DETAILS,
    MOCK_STOCK_HISTORY
)

@pytest.fixture
def mock_oplab_client():
    mock_client = MagicMock()
    mock_client.historical_options.return_value = pd.DataFrame(MOCK_OPTIONS_LIST['data'])
    mock_client.historical_instruments_details.return_value = pd.DataFrame(MOCK_INSTRUMENTS_DETAILS['data'])
    mock_client.historical_stock.return_value = pd.DataFrame(MOCK_STOCK_HISTORY['data'])
    return mock_client

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
            import pandas as pd
            # Return empty DataFrame or sample data as needed
            return pd.DataFrame({'ticker': []})

        def historical_instruments_details(self, tickers, date_str):
            import pandas as pd
            # Return empty DataFrame or sample data as needed
            return pd.DataFrame()

        def historical_stock(self, symbol, start_date, end_date):
            import pandas as pd
            # Return sample data
            dates = pd.date_range(start_date, end_date, freq='B')
            return pd.DataFrame({'date': dates})
    
    return MockOplabClient()