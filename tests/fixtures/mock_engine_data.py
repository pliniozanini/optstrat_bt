import pandas as pd

# Mock options data for 3 consecutive days
MOCK_OPTIONS_DATA = pd.DataFrame([
    # Day 1
    {'time': pd.Timestamp('2023-01-02'), 'ticker': 'TICKA', 'high': 10.5, 'low': 9.5, 'close': 10.0},
    # Day 2: The price for executing the trade from Day 1
    {'time': pd.Timestamp('2023-01-03'), 'ticker': 'TICKA', 'high': 12.0, 'low': 10.8, 'close': 11.5},
    # Day 3: For mark-to-market after the trade
    {'time': pd.Timestamp('2023-01-04'), 'ticker': 'TICKA', 'high': 13.0, 'low': 11.8, 'close': 12.5},
])

# Mock stock data for the same period
MOCK_STOCK_DATA = pd.DataFrame([
    {'date': pd.Timestamp('2023-01-02'), 'close': 100},
    {'date': pd.Timestamp('2023-01-03'), 'close': 101},
    {'date': pd.Timestamp('2023-01-04'), 'close': 102},
])