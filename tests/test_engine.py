import pandas as pd
from unittest.mock import MagicMock

# --- CORRECTED IMPORTS ---
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.core.strategy import Strategy
from opstrat_backtester.data.datasource import DataSource

# Import mock data
from tests.fixtures.mock_engine_data import MOCK_OPTIONS_DATA, MOCK_STOCK_DATA

# 1. Define an updated strategy for predictable signals
class BuyAndHoldStrategy(Strategy):
    def generate_signals(self, date, daily_options_data, stock_history, portfolio):
        # Only generate a signal on the first day
        if date.date() == pd.Timestamp('2023-01-02').date() and not portfolio.get_positions():
            signals = [{'ticker': 'TICKA', 'quantity': 10}] # Buy 10 shares
            return signals, {} # Return tuple
        return [], {}

# 2. Create the updated test
def test_backtester_run_pessimistic_execution():
    """
    Tests that the refactored backtester executes a trade on the next day
    at the pessimistic price (high for a buy).
    """
    # a. Set up mock DataSource
    mock_datasource = MagicMock(spec=DataSource)
    mock_datasource.stream_options_data.return_value = [MOCK_OPTIONS_DATA]
    mock_datasource.stream_stock_data.return_value = iter([MOCK_STOCK_DATA])

    # b. Instantiate components
    strategy = BuyAndHoldStrategy()
    backtester = Backtester(
        strategy=strategy,
        start_date="2023-01-02",
        end_date="2023-01-04",
        spot_symbol="TEST",
        initial_cash=10000
    )
    backtester.set_data_source(mock_datasource)

    # c. Run the backtest (unpack the tuple)
    results_df, trades_df = backtester.run()
    
    # d. Assertions
    # The signal is on day 1, trade executes on day 2
    assert len(trades_df) == 1
    trade = trades_df.iloc[0]
    
    # Assert trade details: happened on day 2 at day 2's HIGH price
    assert trade['date'].date() == pd.Timestamp('2023-01-03').date()
    assert trade['ticker'] == 'TICKA'
    assert trade['quantity'] == 10
    assert trade['price'] == 12.0  # 'high' price on 2023-01-03

    # Assert final portfolio value on day 3
    # Final cash = 10000 - (10 * 12.0) = 9880
    # Final value = 9880 + (10 * 12.5) (close on day 3) = 10005
    final_value = results_df.iloc[-1]['portfolio_value']
    assert final_value == 10005