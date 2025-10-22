import pytest
import pandas as pd
from typing import List, Dict, Tuple, Optional

# --- 1. Imports ---
# Import the classes we need to test and mock
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.core.strategy import Strategy
from opstrat_backtester.core.portfolio import Portfolio # Need this for type hints
from opstrat_backtester.data.datasource import DataSource


# --- 2. Mock Classes ---
# We create mock classes to simulate the behavior of a real strategy
# and data source, allowing us to "spy" on what the Backtester does.

class MockLookbackStrategy(Strategy):
    """
    A mock strategy that:
    1. Tells the Backtester it needs a lookback_days.
    2. Records the exact stock_history it receives on each day.
    """
    def __init__(self, lookback_days: int):
        super().__init__()
        # This is the attribute the Backtester will read
        self.lookback_days = lookback_days

        # These lists will store what we receive, so we can check it later
        self.received_history_lengths: List[int] = []
        self.received_first_dates: List[pd.Timestamp] = []
        self.received_last_dates: List[pd.Timestamp] = []

    def generate_signals(self,
                         date: pd.Timestamp,
                         daily_options_data: pd.DataFrame,
                         stock_history: pd.DataFrame,
                         portfolio: Portfolio) -> Tuple[List[Dict], Dict]:

        # Record what the Backtester sent us on this day
        self.received_history_lengths.append(len(stock_history))
        if not stock_history.empty:
            self.received_first_dates.append(stock_history.iloc[0]['date'])
            self.received_last_dates.append(stock_history.iloc[-1]['date'])

        # Return no signals, we're just here to spy
        return [], {}

class MockLookbackDataSource(DataSource):
    """
    A mock data source that:
    1. Returns our pre-defined test data.
    2. Records the date ranges the Backtester *requests* data for.
    """
    def __init__(self, stock_df: pd.DataFrame, options_df: pd.DataFrame):
        self.stock_df = stock_df
        self.options_df = options_df

        # We will store the arguments the Backtester calls us with
        self.stock_call_args: Optional[Dict] = None
        self.options_call_args: Optional[Dict] = None

    def stream_stock_data(self, symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp):
        # Record what we were called with
        self.stock_call_args = {'symbol': symbol, 'start_date': start_date, 'end_date': end_date}

        # Yield the *entire* stock DataFrame.
        # The Backtester is responsible for filtering and slicing.
        yield self.stock_df

    def stream_options_data(self, spot: str, start_date: pd.Timestamp, end_date: pd.Timestamp):
        # Record what we were called with
        self.options_call_args = {'spot': spot, 'start_date': start_date, 'end_date': end_date}

        # Yield only the options within the requested *backtest* period
        mask = (self.options_df['time'] >= start_date) & (self.options_df['time'] <= end_date)
        yield self.options_df[mask]


# --- 3. Test Data Fixture ---
# A pytest "fixture" is a reusable function that sets up data for our tests.

@pytest.fixture
def mock_market_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Creates a simple set of stock and option data for testing.
    Includes data *before* the backtest starts for the warm-up.
    """
    # Stock data: 10 trading days total
    stock_dates = pd.to_datetime([
        "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
        "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12"
    ], utc=True)
    stock_df = pd.DataFrame({
        'date': stock_dates,
        'close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
    })

    # Options data: Only for the 3 days of the *backtest*
    option_dates = pd.to_datetime(["2024-01-08", "2024-01-09", "2024-01-10"], utc=True)
    options_df = pd.DataFrame({
        'time': option_dates,
        'symbol': ['OPT_A', 'OPT_A', 'OPT_A'],
        'type': ['PUT', 'PUT', 'PUT'],
        'strike': [100, 100, 100],
        'due_date': [pd.to_datetime("2024-02-15", utc=True)] * 3,
        'close': [1.0, 0.9, 0.8],
        'high': [1.1, 1.0, 0.9],
        'low': [0.9, 0.8, 0.7]
    })

    return stock_df, options_df


# --- 4. The Test Function ---

def test_backtester_handles_lookback_correctly(mock_market_data):
    """
    This is the main test. It verifies three things:
    1. The Backtester asks the DataSource for stock data from an *earlier* "warm-up" date.
    2. The Backtester asks the DataSource for options data from the *actual* start date.
    3. The Strategy receives a stock_history slice of the *correct length* on every day.
    """

    # --- ARRANGE ---

    # Get our test data
    stock_df, options_df = mock_market_data

    # Define test parameters
    LOOKBACK_DAYS = 5
    START_DATE = "2024-01-08" # The day the backtest *starts*
    END_DATE = "2024-01-10"   # The day the backtest *ends*

    # 1. Create our mock strategy
    mock_strategy = MockLookbackStrategy(lookback_days=LOOKBACK_DAYS)

    # 2. Create our mock data source
    mock_data_source = MockLookbackDataSource(stock_df, options_df)

    # 3. Create the Backtester instance
    backtester = Backtester(
        spot_symbol="MOCK",
        strategy=mock_strategy,
        start_date=START_DATE,
        end_date=END_DATE
    )

    # 4. Inject the mock data source
    backtester.set_data_source(mock_data_source)

    # --- ACT ---

    # Run the backtest. This will call our mock classes.
    summary_df, trades_df = backtester.run()

    # --- ASSERT ---

    print("Verifying data loading requests...")
    # Test 1: Verify warm-up for STOCKS
    stock_call_date = mock_data_source.stock_call_args['start_date']
    assert stock_call_date < backtester.start_date_dt
    print(f"  SUCCESS: Stock data requested from {stock_call_date.date()} (before {START_DATE})")

    # Test 2: Verify NO warm-up for OPTIONS
    options_call_date = mock_data_source.options_call_args['start_date']
    assert options_call_date == backtester.start_date_dt
    print(f"  SUCCESS: Options data requested from {options_call_date.date()} (equals {START_DATE})")


    print("\nVerifying data slices sent to strategy...")
    # Test 3: Verify the strategy received the correct data slices

    # We ran for 3 days (Jan 8, 9, 10)
    assert len(mock_strategy.received_history_lengths) == 3
    print("  SUCCESS: Strategy was called 3 times.")

    # Check that *every* call received a slice of length 5
    assert mock_strategy.received_history_lengths == [5, 5, 5]
    print(f"  SUCCESS: Strategy received {LOOKBACK_DAYS} rows on every day.")

    # --- Deeper check on the slices ---

    # Day 1 (2024-01-08)
    # Should receive data from 2024-01-02 to 2024-01-08
    # NOTE: The provided stock data has a gap on Jan 6-7 (weekend).
    # The 5 days *before* Jan 8 are Jan 5, 4, 3, 2, 1.
    # Ah, wait, the slice is inclusive. So stock_data[stock_data['date'] <= "2024-01-08"]
    # gives [01, 02, 03, 04, 05, 08]. The last 5 rows are [03, 04, 05, 08].
    # Let's re-check the data.
    # Dates: [01, 02, 03, 04, 05, 08, 09, 10, 11, 12]
    # On 2024-01-08: Full history is [01, 02, 03, 04, 05, 08].
    # .iloc[-5:] gives [02, 03, 04, 05, 08]. *My mistake in reasoning, let's fix the test dates*
    # Let's use simpler dates to be sure.
    # Stock dates: [01, 02, 03, 04, 05, 08, 09, 10]
    # Day 1 (Jan 8): History is [01, 02, 03, 04, 05, 08]. Last 5 are: [02, 03, 04, 05, 08]
    # Day 2 (Jan 9): History is [01, 02, 03, 04, 05, 08, 09]. Last 5 are: [03, 04, 05, 08, 09]
    # Day 3 (Jan 10): History is [01, 02, 03, 04, 05, 08, 09, 10]. Last 5 are: [04, 05, 08, 09, 10]
    # This logic seems correct. Let's adjust the stock_df in the fixture to match this.

    # Find the bug in my *test setup*:
    # Ah, my stock_dates list was 10 items.
    # On Day 1 (Jan 8): History is [01, 02, 03, 04, 05, 08]. Last 5 are: [02, 03, 04, 05, 08]
    # On Day 2 (Jan 9): History is [01, 02, 03, 04, 05, 08, 09]. Last 5 are: [03, 04, 05, 08, 09]
    # On Day 3 (Jan 10): History is [01, 02, 03, 04, 05, 08, 09, 10]. Last 5 are: [04, 05, 08, 09, 10]

    # This seems correct. Let's write the asserts for this.

    # Check Day 1 (Jan 8)
    assert mock_strategy.received_first_dates[0].date() == pd.to_datetime("2024-01-02").date()
    assert mock_strategy.received_last_dates[0].date() == pd.to_datetime("2024-01-08").date()
    print("  SUCCESS: Day 1 slice (Jan 8) is correct (Jan 2 -> Jan 8).")

    # Check Day 2 (Jan 9)
    assert mock_strategy.received_first_dates[1].date() == pd.to_datetime("2024-01-03").date()
    assert mock_strategy.received_last_dates[1].date() == pd.to_datetime("2024-01-09").date()
    print("  SUCCESS: Day 2 slice (Jan 9) is correct (Jan 3 -> Jan 9).")

    # Check Day 3 (Jan 10)
    assert mock_strategy.received_first_dates[2].date() == pd.to_datetime("2024-01-04").date()
    assert mock_strategy.received_last_dates[2].date() == pd.to_datetime("2024-01-10").date()
    print("  SUCCESS: Day 3 slice (Jan 10) is correct (Jan 4 -> Jan 10).")
