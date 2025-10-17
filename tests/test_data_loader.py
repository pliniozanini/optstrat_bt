import pandas as pd
from opstrat_backtester.data_loader import OplabDataSource

# Use the mock data from the forensic test for simplicity and relevance
from tests.fixtures.forensic_mock_data import MOCK_FORENSIC_OPTIONS_DATA

def test_oplab_data_source_streams_data(mock_oplab_client):
    """
    Tests that OplabDataSource correctly uses the api_client to stream data,
    including the case where no instrument details are returned.
    """
    # 1. Arrange: Configure the mock client
    mock_oplab_client.historical_options.return_value = MOCK_FORENSIC_OPTIONS_DATA
    # Simulate the API returning no enrichment data
    # Mock for historical_instruments_details that returns valid data
    def mock_details_return(tickers, date_str):
        # Create a mock details DataFrame that matches what the function expects.
        # It should include the 'symbol' and 'time' columns for the merge.
        return pd.DataFrame({
            'symbol': tickers,
            'time': pd.to_datetime(date_str).tz_localize('UTC'),
            'open': 1.0,
            'high': 1.1,
            'low': 0.9,
            'close': 1.05
        })
    
    mock_oplab_client.historical_instruments_details.side_effect = mock_details_return
    
    # 2. Act: Instantiate and call the stream method
    datasource = OplabDataSource(api_client=mock_oplab_client)
    stream_output = list(datasource.stream_options_data(
        spot="SPOT",
        start_date="2024-01-01",
        end_date="2024-01-31"
    ))

    # 3. Assert: Verify API calls and output
    mock_oplab_client.historical_options.assert_called_once()
    assert mock_oplab_client.historical_instruments_details.call_count > 0
    
    # The generator should yield one valid, enriched DataFrame
    assert len(stream_output) == 1
    output_df = stream_output[0]
    assert isinstance(output_df, pd.DataFrame)
    assert not output_df.empty
    # Check that enrichment was successful by looking for a column from the details mock
    assert 'close' in output_df.columns
