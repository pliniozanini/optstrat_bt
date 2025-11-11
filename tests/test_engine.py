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

def test_backtester_commission_and_fees():
    """
    Tests that the backtester correctly applies commission and fees to trades.
    """
    # Set up mock DataSource
    mock_datasource = MagicMock(spec=DataSource)
    mock_datasource.stream_options_data.return_value = [MOCK_OPTIONS_DATA]
    mock_datasource.stream_stock_data.return_value = iter([MOCK_STOCK_DATA])

    # Define a strategy that makes trades
    class TestStrategy(Strategy):
        def generate_signals(self, date, daily_options_data, stock_history, portfolio):
            if date.date() == pd.Timestamp('2023-01-02').date() and not portfolio.get_positions():
                return [{'ticker': 'TICKA', 'quantity': 5}], {}  # Buy 5 contracts
            return [], {}

    strategy = TestStrategy()

    # Create backtester with custom commission and fees
    commission_per_contract = 1.0  # R$ 1.00 per contract
    fees_pct = 0.001  # 0.1% fee
    initial_cash = 10000

    backtester = Backtester(
        strategy=strategy,
        start_date="2023-01-02",
        end_date="2023-01-04",
        spot_symbol="TEST",
        initial_cash=initial_cash,
        commission_per_contract=commission_per_contract,
        fees_pct=fees_pct
    )
    backtester.set_data_source(mock_datasource)

    # Run the backtest
    results_df, trades_df = backtester.run()

    # Verify trade was executed
    assert len(trades_df) == 1
    trade = trades_df.iloc[0]

    # Verify trade details
    assert trade['date'].date() == pd.Timestamp('2023-01-03').date()
    assert trade['ticker'] == 'TICKA'
    assert trade['quantity'] == 5
    assert trade['price'] == 12.0  # high price on 2023-01-03

    # Calculate expected costs
    trade_value = 5 * 12.0  # 60.0
    expected_commission = 5 * commission_per_contract  # 5.0
    expected_fees = trade_value * fees_pct  # 0.06
    expected_total_costs = expected_commission + expected_fees  # 5.06
    expected_raw_cost = trade_value  # 60.0
    expected_total_trade_cost = expected_raw_cost + expected_total_costs  # 65.06

    # Verify costs are recorded in trade
    assert trade['commission'] == expected_commission
    assert trade['fees'] == expected_fees
    assert trade['total_trade_cost'] == expected_total_trade_cost
    assert trade['cost'] == expected_raw_cost  # Raw cost should be just price * quantity

    # Verify cash balance reflects all costs
    # Initial cash - (raw cost + commission + fees)
    expected_final_cash = initial_cash - expected_total_trade_cost
    actual_final_cash = results_df.iloc[-1]['cash']

    # The cash should be reduced by the total trade cost including fees and commissions
    assert abs(actual_final_cash - expected_final_cash) < 0.01  # Allow for small floating point differences

    # Verify metadata includes commission and fees
    assert 'commission' in trade.index
    assert 'fees' in trade.index