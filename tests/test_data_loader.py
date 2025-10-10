import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
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
from opstrat_backtester.data_loader import OplabDataSource

def test_fetch_and_enrich_for_month_uses_mock_api(mock_oplab_client):
    """
    Tests the internal _fetch_and_enrich_for_month function to ensure it correctly
    calls the mocked API client and processes the returned data.
    """
    # Define mock functions that vary responses based on date_str
    def mock_historical_options(spot, date_str):
        # Only return data for specific dates for testing
        if date_str in ['2023-11-01', '2023-11-02']:
            # Return two tickers for these dates
            return pd.DataFrame({'ticker': ['PETRA', 'PETRM']})
        return pd.DataFrame({'ticker': []})

    def mock_historical_instruments_details(tickers, date_str):
        # Return a DataFrame with ticker names appended with '110' and a fixed price
        return pd.DataFrame({
            'ticker': [t + '110' for t in tickers],
            'price': [100 for _ in tickers]
        })

    # Assign the mock functions to the mock_oplab_client using MagicMock for call assertions
    mock_oplab_client.historical_options = MagicMock(side_effect=mock_historical_options)
    mock_oplab_client.historical_instruments_details = MagicMock(side_effect=mock_historical_instruments_details)

    from opstrat_backtester.data_loader import OplabDataSource
    datasource = OplabDataSource(api_client=mock_oplab_client)
    result_df = datasource._fetch_and_enrich_for_month(spot="PETR4", year=2023, month=11)

    # Ensure that the mocked methods were called at least once
    assert mock_oplab_client.historical_options.call_count > 0
    assert mock_oplab_client.historical_instruments_details.call_count > 0

    # Assert that result is a DataFrame
    assert isinstance(result_df, pd.DataFrame)

    # Check the content of the resulting DataFrame: it should contain the two tickers from our test days
    unique_tickers = sorted(result_df['ticker'].unique().tolist())
    expected_tickers = sorted(['PETRA110', 'PETRM110'])
    assert unique_tickers == expected_tickers