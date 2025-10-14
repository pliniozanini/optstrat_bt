import pandas as pd
from unittest.mock import MagicMock

# Import core components
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.core.strategy import Strategy
from opstrat_backtester.data.datasource import DataSource

# Import mock data
from .fixtures.mock_engine_data import MOCK_OPTIONS_DATA, MOCK_STOCK_DATA

# 1. Define a dummy strategy for predictable signals
class BuyAndHoldStrategy(Strategy):
    def generate_signals(self, date, daily_options_data, stock_history, portfolio):
        # Only generate a signal on the first day
        if date == pd.Timestamp('2023-01-02').date() and not portfolio.get_positions():
            return [{'ticker': 'TICKA', 'quantity': 10}] # Buy 10 shares
        return []

# 2. Create the test
def test_backtester_run_pessimistic_execution():
    """
    Tests that the backtester correctly executes a trade on the next day
    at the pessimistic price (high for a buy).
    """
    # a. Set up mock DataSource
    mock_datasource = MagicMock(spec=DataSource)
    mock_datasource.stream_options_data.return_value = [MOCK_OPTIONS_DATA] # Stream the chunk
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

    # c. Run the backtest
    results_df = backtester.run()
    
    # d. Assertions
    # The signal is on day 1, so the trade should execute on day 2
    trade_history = backtester.portfolio.get_trade_history()
    assert len(trade_history) == 1
    trade = trade_history[0]
    
    # Assert trade details: happened on day 2 at day 2's HIGH price
    assert trade['date'] == pd.Timestamp('2023-01-03').date()
    assert trade['ticker'] == 'TICKA'
    assert trade['quantity'] == 10
    assert trade['price'] == 12.0  # The 'high' price on 2023-01-03

    # Assert final portfolio value on day 3
    # Final cash = 10000 - (10 * 12.0) = 9880
    # Final value = 9880 + (10 * 12.5) (close price on day 3) = 9880 + 125 = 10005
    final_value = results_df.iloc[-1]['portfolio_value']
    assert final_value == 10005