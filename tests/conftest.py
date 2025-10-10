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
def mock_oplab_client(mocker):
    """
    This fixture provides a mock of the OplabClient.
    It patches the client at the source, so any code that imports and uses it
    will get this mock instead of the real one during tests.
    """
    # Import OplabClient here to ensure the mock_env_token fixture is already applied
    from opstrat_backtester.api_client import OplabClient
    
    # Create a mock instance of the client
    mock_client = MagicMock(spec=OplabClient)
    
    # Configure the return values for each method
    mock_client.historical_options.return_value = pd.DataFrame(MOCK_OPTIONS_LIST['data'])
    mock_client.historical_instruments_details.return_value = pd.DataFrame(MOCK_INSTRUMENTS_DETAILS['data'])
    mock_client.historical_stock.return_value = pd.DataFrame(MOCK_STOCK_HISTORY['data'])
    
    # Use mocker.patch to replace the real client instance in the data_loader module
    mocker.patch('opstrat_backtester.data_loader.api_client', new=mock_client)
    
    # Yield the configured mock client so tests can inspect it if needed
    yield mock_client