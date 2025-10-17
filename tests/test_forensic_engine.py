# tests/test_forensic_engine.py
import pytest
import pandas as pd
from unittest.mock import MagicMock

# --- CORRECTED IMPORTS ---
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.data.datasource import DataSource
from opstrat_backtester.core.strategy import Strategy

# --- Import Mock Data ---
# This test assumes the mock data file exists at this location
from tests.fixtures.forensic_mock_data import (
    MOCK_FORENSIC_STOCK_DATA,
    MOCK_FORENSIC_OPTIONS_DATA
)

# --- Test-Specific Strategy (moved from forensic_strategy.py) ---
class ForensicTestStrategy(Strategy):
    """A strategy that generates specific signals on hardcoded dates for testing."""
    def generate_signals(self, date, daily_options_data, stock_history, portfolio):
        signals = []
        # Day 1: Sell a straddle
        if date.date() == pd.Timestamp('2024-01-02').date():
            signals.extend([
                {'ticker': 'SPOTC100', 'quantity': -100},
                {'ticker': 'SPOTP100', 'quantity': -100}
            ])
        # Day 3: Buy a call
        if date.date() == pd.Timestamp('2024-01-04').date():
            signals.append({'ticker': 'SPOTC105', 'quantity': 100})
        
        return signals, {}

# --- Pytest Fixtures and Test Class ---

@pytest.fixture(scope="class")
def backtest_results():
    """
    A pytest fixture that runs the full forensic backtest once and yields
    the results for the test methods to use. This is highly efficient.
    """
    # 1. Set up the mock data source
    mock_datasource = MagicMock(spec=DataSource)
    mock_datasource.stream_options_data.return_value = iter([MOCK_FORENSIC_OPTIONS_DATA])
    mock_datasource.stream_stock_data.return_value = iter([MOCK_FORENSIC_STOCK_DATA])

    # 2. Instantiate the backtester with the forensic strategy
    strategy = ForensicTestStrategy()
    backtester = Backtester(
        strategy=strategy,
        start_date="2024-01-02",
        end_date="2024-01-05",
        spot_symbol="SPOT",
        initial_cash=100_000
    )
    backtester.set_data_source(mock_datasource)
    
    # 3. Run the backtest and yield the results
    summary_df, trades_df = backtester.run()
    
    # Yield a dictionary of results for easy access in tests
    yield {
        "summary": summary_df,
        "trades": trades_df,
        "portfolio": backtester.portfolio
    }

@pytest.mark.usefixtures("backtest_results")
class TestForensicEngineLogic:
    """
    A test class that uses the pre-computed backtest results to verify
    each critical calculation and state change.
    """
    
    def test_short_straddle_execution(self, backtest_results):
        """Verify the initial short straddle trade and its impact on cash."""
        trades_df = backtest_results["trades"]
        
        trade_call = trades_df[trades_df['ticker'] == 'SPOTC100'].iloc[0]
        assert trade_call['quantity'] == -100
        assert trade_call['price'] == 1.95  # Pessimistic 'low' for a sell

        trade_put = trades_df[trades_df['ticker'] == 'SPOTP100'].iloc[0]
        assert trade_put['quantity'] == -100
        assert trade_put['price'] == 0.00   # Pessimistic 'low' for a sell

    def test_option_expiration_handling(self, backtest_results):
        """Verify the ITM/OTM expiration handling and its impact on cash."""
        trades_df = backtest_results["trades"]
        summary_df = backtest_results["summary"]

        # Verify ITM Call (SPOTC100) was exercised correctly
        itm_trade = trades_df[trades_df['action'] == 'EXERCISE_ITM'].iloc[0]
        assert itm_trade['ticker'] == 'SPOTC100'
        assert itm_trade['quantity'] == 100  # Closing trade is a buy
        assert itm_trade['price'] == 2.00   # Intrinsic value (102 stock - 100 strike)

        # Verify OTM Put (SPOTP100) expired worthless
        otm_trade = trades_df[trades_df['action'] == 'EXPIRE_OTM'].iloc[0]
        assert otm_trade['ticker'] == 'SPOTP100'
        assert otm_trade['quantity'] == 100  # Closing trade is a buy
        assert otm_trade['price'] == 0.00

        # Verify cash on the day of expiration
        day3_summary = summary_df[summary_df['date'] == pd.to_datetime('2024-01-03', utc=True)].iloc[0]
        expected_cash_after_expiration = 100_195.00 - 200.00  # Cash after straddle - ITM cost
        assert day3_summary['cash'] == expected_cash_after_expiration

    def test_long_call_execution(self, backtest_results):
        """Verify the execution of the new long call position."""
        trades_df = backtest_results["trades"]
        long_call_trade = trades_df[trades_df['ticker'] == 'SPOTC105'].iloc[0]
        
        assert long_call_trade['quantity'] == 100
        assert long_call_trade['price'] == 4.20  # Pessimistic 'high' for a buy
        assert long_call_trade['date'].date() == pd.to_datetime('2024-01-05').date()

    def test_final_portfolio_state(self, backtest_results):
        """Verify the final cash, positions, and total portfolio value."""
        portfolio = backtest_results["portfolio"]
        summary_df = backtest_results["summary"]

        # 1. Final Cash Verification
        expected_final_cash = 99_995.00 - 420.00  # Cash after expiration - long call cost
        assert portfolio.cash == pytest.approx(expected_final_cash)

        # 2. Final Position Verification
        final_positions = portfolio.get_positions()
        assert len(final_positions) == 1
        assert 'SPOTC105' in final_positions
        assert final_positions['SPOTC105']['quantity'] == 100

        # 3. Final Portfolio Value Verification
        final_day_summary = summary_df.iloc[-1]
        position_market_value = 100 * 4.00  # Qty * 'close' price on final day
        expected_final_value = expected_final_cash + position_market_value
        
        assert final_day_summary['portfolio_value'] == pytest.approx(expected_final_value)
        assert final_day_summary['date'].date() == pd.to_datetime('2024-01-05').date()