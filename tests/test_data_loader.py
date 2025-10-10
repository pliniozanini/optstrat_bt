import os
import pandas as pd
import pytest
from unittest.mock import patch
import sys
from pathlib import Path

# Add the src directory to the Python path
src_dir = str(Path(__file__).parent.parent / 'src')
sys.path.insert(0, src_dir)

# Set test token before importing
os.environ['OPLAB_ACCESS_TOKEN'] = 'test_token'

# Import mock data
from .fixtures.mock_api_data import (
    MOCK_OPTIONS_LIST,
    MOCK_INSTRUMENTS_DETAILS
)

# Now we can safely import the module
from opstrat_backtester.data_loader import _fetch_and_enrich_for_month

def test_fetch_and_enrich_for_month_uses_mock_api(mock_oplab_client):
    """
    Tests the internal _fetch_and_enrich_for_month function to ensure it correctly
    calls the mocked API client and processes the returned data.
    """
    # --- 1. Configure mocks ---
    mock_oplab_client.historical_options.return_value = pd.DataFrame(MOCK_OPTIONS_LIST['data'])
    mock_oplab_client.historical_instruments_details.return_value = pd.DataFrame(MOCK_INSTRUMENTS_DETAILS['data'])

    # --- 2. Execution ---
    # Use patch to override get_api_client() in data_loader
    with patch('opstrat_backtester.data_loader.get_api_client', return_value=mock_oplab_client):
        result_df = _fetch_and_enrich_for_month(spot="PETR4", year=2023, month=11)

            # --- 3. Assertions ---
        # a) Check that our mock API methods were called with correct params
        mock_oplab_client.historical_options.assert_called()
        mock_oplab_client.historical_instruments_details.assert_called()
    
        # b) Check the content of the resulting DataFrame
        assert not result_df.empty
        expected_tickers = ['PETRA110', 'PETRM110']
        unique_tickers = sorted(result_df['ticker'].unique().tolist())
        assert unique_tickers == sorted(expected_tickers)  # We have the expected unique tickers
        assert 'ticker' in result_df.columns
        assert 'delta' in result_df.columns
        
        # c) Check that the date was correctly added
        assert 'time' in result_df.columns
        assert pd.api.types.is_datetime64_any_dtype(result_df['time'])