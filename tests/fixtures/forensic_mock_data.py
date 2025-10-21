import pandas as pd

# Mock data for the underlying stock (SPOT)
MOCK_FORENSIC_STOCK_DATA = pd.DataFrame([
    {'date': pd.to_datetime('2024-01-02', utc=True), 'close': 100.0},
    # On this day, the stock price moves to make the 100-strike call ITM
    {'date': pd.to_datetime('2024-01-03', utc=True), 'close': 102.0},
    {'date': pd.to_datetime('2024-01-04', utc=True), 'close': 103.0},
    # The price of the new call option moves up for our final MTM check
    {'date': pd.to_datetime('2024-01-05', utc=True), 'close': 105.0},
])

# Mock data for the options chains for each day of the test
MOCK_FORENSIC_OPTIONS_DATA = pd.DataFrame([
    # --- Day 1 (2024-01-02): Decision to sell straddle ---
    # Call to be sold (expires next day)
    {'time': pd.to_datetime('2024-01-02', utc=True), 'symbol': 'SPOTC100', 'type': 'CALL', 'strike': 100.0, 'due_date': '2024-01-03', 'close': 2.50, 'high': 2.60, 'low': 2.40},
    # Put to be sold (expires next day)
    {'time': pd.to_datetime('2024-01-02', utc=True), 'symbol': 'SPOTP100', 'type': 'PUT', 'strike': 100.0, 'due_date': '2024-01-03', 'close': 2.00, 'high': 2.10, 'low': 1.90},

    # --- Day 2 (2024-01-03): Execution of straddle & Expiration Day ---
    # Execution prices for the straddle sold on Day 1 (pessimistic: low for sells)
    {'time': pd.to_datetime('2024-01-03', utc=True), 'symbol': 'SPOTC100', 'type': 'CALL', 'strike': 100.0, 'due_date': '2024-01-03', 'close': 2.00, 'high': 2.10, 'low': 1.95}, # Sell executed at 1.95
    {'time': pd.to_datetime('2024-01-03', utc=True), 'symbol': 'SPOTP100', 'type': 'PUT', 'strike': 100.0, 'due_date': '2024-01-03', 'close': 0.00, 'high': 0.10, 'low': 0.00},  # Sell executed at 0.00

    # --- Day 3 (2024-01-04): Decision to buy a new call ---
    {'time': pd.to_datetime('2024-01-04', utc=True), 'symbol': 'SPOTC105', 'type': 'CALL', 'strike': 105.0, 'due_date': '2024-02-15', 'close': 3.00, 'high': 3.10, 'low': 2.90},

    # --- Day 4 (2024-01-05): Execution of the new call & Final MTM ---
    # Execution price for the call bought on Day 3 (pessimistic: high for buys)
    {'time': pd.to_datetime('2024-01-05', utc=True), 'symbol': 'SPOTC105', 'type': 'CALL', 'strike': 105.0, 'due_date': '2024-02-15', 'close': 4.00, 'high': 4.20, 'low': 3.80} # Buy executed at 4.20
])
