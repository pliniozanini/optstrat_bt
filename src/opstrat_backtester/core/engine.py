import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any, Tuple, Literal
from .strategy import Strategy
from .portfolio import Portfolio
from .events import EventHandler, OptionExpirationHandler
from ..data.datasource import DataSource
from . import VerbosityAdapter

class Backtester:
    """
    A comprehensive, event-driven backtesting engine for options trading strategies.

    The Backtester class provides a robust framework for simulating trading strategies
    with support for both stocks and options. It processes each day of the simulation
    in a clear, sequential order:

    1. Execute trades based on signals from the previous day
    2. Handle market events (e.g., option expirations)
    3. Mark portfolio to market
    4. Call strategy to generate new signals

    Parameters
    ----------
    spot_symbol : str
        The underlying stock symbol (e.g., 'AAPL')
    strategy : Strategy
        Instance of a Strategy subclass defining the trading logic
    start_date : str
        Simulation start date in 'YYYY-MM-DD' format
    end_date : str
        Simulation end date in 'YYYY-MM-DD' format
    initial_cash : float, optional
        Starting cash balance. Default is 100,000
    event_handlers : list of EventHandler, optional
        List of event handlers for processing market events.
        Default is [OptionExpirationHandler()]
    stale_price_days : int, optional
        Number of days to use stale prices before marking to zero. Default is 3
    verbosity : str, optional
        Logging verbosity level. Options: "high", "moderate", "low". Default is "low"
        - "high": All messages (current behavior)
        - "moderate": Warnings and key lifecycle messages only
        - "low": Silent operation
    commission_per_contract : float, optional
        Fixed commission cost per option contract in BRL. Default is 0.50
    fees_pct : float, optional
        Percentage-based fees applied to trade value (e.g., B3 fees). Default is 0.0001 (0.01%)

    Attributes
    ----------
    portfolio : Portfolio
        The portfolio being managed in the simulation
    data_source : DataSource
        Source of market data for the simulation
    trade_log : list
        Detailed log of all executed trades
    daily_history : list
        Daily record of portfolio state and market data

    Examples
    --------
    >>> from my_strategy import MyStrategy
    >>> from opstrat_backtester.data_loader import MockDataSource
    
    >>> # Initialize components
    >>> strategy = MyStrategy()
    >>> backtester = Backtester(
    ...     spot_symbol='AAPL',
    ...     strategy=strategy,
    ...     start_date='2023-01-01',
    ...     end_date='2023-12-31'
    ... )
    >>> backtester.set_data_source(MockDataSource())
    
    >>> # Run backtest
    >>> results = backtester.run()
    
    Notes
    -----
    The backtester expects options data in a standardized format with required
    fields like symbol, type (CALL/PUT), strike, expiry_date, and pricing data.
    Similarly, stock data should include OHLCV fields.

    See Also
    --------
    Strategy : Abstract base class for implementing trading strategies
    Portfolio : Class for managing positions and tracking performance
    EventHandler : Base class for implementing market event handlers
    """
    def __init__(
        self,
        spot_symbol: str,
        strategy: Strategy,
        start_date: str,
        end_date: str,
        initial_cash: float = 100_000,
        event_handlers: Optional[List[EventHandler]] = None,
        stale_price_days: int = 3,
        verbosity: Literal["high", "moderate", "low"] = "low",
        commission_per_contract: float = 0.0, # Example: 0.5 = R$ 0.50 per contract
        fees_pct: float = 0.0 # Example: 0.001 = 0.1% for B3 fees etc.
    ):
        # Initialize logger
        self.logger = VerbosityAdapter(verbosity)
        self.spot_symbol = spot_symbol
        self.strategy = strategy
        self.start_date_dt = pd.to_datetime(start_date, utc=True)
        self.end_date_dt = pd.to_datetime(end_date, utc=True)

        # Read 'lookback_days' from the strategy. Default to 0 if not found.
        # getattr() safely checks if the attribute exists.
        self.lookback_days = getattr(self.strategy, 'lookback_days', 0)

        # Calculate the number of *calendar days* needed for the warm-up.
        # We can't just subtract 252 days, because that includes weekends and holidays.
        # We use a rough approximation: (lookback_days * 1.5) + 15 days.
        # This creates a safe buffer to ensure we get enough *trading days*.
        calendar_days_required = 0
        if self.lookback_days > 0:
            calendar_days_required = int(self.lookback_days * 1.5) + 15
            self.logger.info(f"Strategy requires {self.lookback_days} lookback days. Setting data load start {calendar_days_required} calendar days earlier.", always_show=True)

        # Store the new, earlier date for *data loading*.
        self.data_start_date_dt = self.start_date_dt - pd.DateOffset(days=calendar_days_required)
        # The end date for loading is just the normal backtest end date.
        self.data_end_date_dt = self.end_date_dt

        self.portfolio = Portfolio(initial_cash, stale_price_days, self.logger)
        self.data_source: Optional[DataSource] = None
        self.event_handlers = event_handlers or [OptionExpirationHandler(self.logger)]
        self.trade_log: List[Dict[str, Any]] = []
        self.daily_history: List[Dict[str, Any]] = []
        # --- STORE NEW PARAMETERS ---
        self.commission_per_contract = commission_per_contract
        self.fees_pct = fees_pct
        # --- END STORE ---

    def set_data_source(self, data_source: DataSource):
        """
        Set the data source for market data.

        Parameters
        ----------
        data_source : DataSource
            An instance of DataSource or its subclasses that provides
            methods for streaming options and stock market data

        Examples
        --------
        >>> backtester.set_data_source(MockDataSource())
        >>> backtester.set_data_source(OplabDataSource(access_token='...'))
        """
        self.data_source = data_source

    def _setup_data_streams(self):
        """
        Initialize and prepare the market data streams.

        This internal method sets up both the options and stock data streams
        needed for the simulation. It validates that a data source has been
        set and handles the initial data loading.

        Returns
        -------
        tuple
            (options_stream, stock_data) where:
            - options_stream: Generator yielding daily options data
            - stock_data: DataFrame of historical stock data

        Raises
        ------
        ValueError
            If no data source has been set
        """
        if self.data_source is None:
            raise ValueError("Data source has not been set. Call set_data_source() before run().")
        
        options_stream = self.data_source.stream_options_data(
            spot=self.spot_symbol, start_date=self.start_date_dt, end_date=self.end_date_dt
        )
        stock_data = pd.concat(list(self.data_source.stream_stock_data(
            symbol=self.spot_symbol, start_date=self.data_start_date_dt, end_date=self.data_end_date_dt
        )))

        if 'date' not in stock_data.columns:
            # Failsafe, though this would likely fail earlier in practice
            raise KeyError("Stock data feed is missing the required 'date' column.")

        # Convert to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(stock_data['date']):
            self.logger.warning(f"Stock data 'date' column was not datetime. Converting and setting to UTC.")
            stock_data['date'] = pd.to_datetime(stock_data['date'], utc=True)

        # Check if the datetime column is naive (tz is None)
        elif stock_data['date'].dt.tz is None:
            # Column is naive (e.g., 'datetime64[ns]'). Localize it to UTC.
            # We assume naive datetimes from any data source are intended to be UTC.
            self.logger.warning(f"Stock data 'date' column is timezone-naive. Localizing to UTC.")
            stock_data['date'] = stock_data['date'].dt.tz_localize('UTC')

        else:
            # Column is already timezone-aware, just convert it to UTC for consistency.
            stock_data['date'] = stock_data['date'].dt.tz_convert('UTC')

        return options_stream, stock_data

    def _execute_trades(self, date: pd.Timestamp, signals: List[Dict], current_options: pd.DataFrame, decision_options: pd.DataFrame):
        """
        Execute pending trades from previously generated signals.

        This internal method processes trading signals by looking up current
        market prices and creating trades in the portfolio. It includes logic
        for trade execution prices and metadata enrichment.

        Parameters
        ----------
        date : pd.Timestamp
            Current simulation date
        signals : list of dict
            Trading signals from strategy
        current_options : pd.DataFrame
            Current day's options market data
        decision_options : pd.DataFrame
            Previous day's options data when signals were generated

        Notes
        -----
        The method assumes:
        - Buy orders execute at the high price
        - Sell orders execute at the low price
        - Trade metadata is enriched from the decision day's data
        """
        for signal in signals:
            ticker, qty = signal['ticker'], signal['quantity']
            execution_data = current_options[current_options['symbol'] == ticker]
            
            if execution_data.empty:
                continue

            price = execution_data['high'].iloc[0] if qty > 0 else execution_data['low'].iloc[0]
            
            # --- CALCULATE COSTS ---
            trade_value = abs(qty) * price
            commission_cost = abs(qty) * self.commission_per_contract
            fee_cost = trade_value * self.fees_pct
            total_costs = commission_cost + fee_cost
            # --- END CALCULATE COSTS ---

            # Retrieve original option data to enrich metadata
            decision_row = decision_options[decision_options['symbol'] == ticker].iloc[0]
            action = 'BUY' if qty > 0 else 'SELL'
            trade_metadata = {
                'type': 'option',
                'option_type': decision_row.get('type'),
                'due_date': decision_row.get('due_date'),
                'strike': decision_row.get('strike'),
                'action': action,
                # --- ADD COSTS TO METADATA (Optional but good for logging) ---
                'commission': commission_cost,
                'fees': fee_cost
                # --- END ADD COSTS ---
            }
            self.portfolio.add_trade(date, ticker, qty, price, metadata=trade_metadata, commission=commission_cost, fees=fee_cost)

    def _handle_events(self, date: pd.Timestamp, current_options: pd.DataFrame, stock_slice: pd.DataFrame):
        """
        Process all registered market events for the current day.

        This internal method invokes each registered event handler in sequence.
        Event handlers may modify the portfolio state (e.g., process option
        expirations, apply corporate actions).

        Parameters
        ----------
        date : pd.Timestamp
            Current simulation date
        current_options : pd.DataFrame
            Current day's options market data
        stock_slice : pd.DataFrame
            Current day's stock market data

        Notes
        -----
        Event handlers are processed in the order they were registered.
        Common events include:
        - Option expirations
        - Stock splits
        - Dividends
        """
        if self.event_handlers:
            for handler in self.event_handlers:
                handler.handle(date, self.portfolio, current_options, stock_slice)

    def _log_daily_history(self, date: pd.Timestamp, signals: List[Dict], custom_indicators: Dict, decision_options: pd.DataFrame):
        """
        Record daily portfolio state and custom metrics.

        This internal method maintains a historical record of portfolio value,
        cash balance, pending signals, and any custom indicators calculated
        by the strategy.

        Parameters
        ----------
        date : pd.Timestamp
            Current simulation date
        signals : list of dict
            Trading signals generated for the next day
        custom_indicators : dict
            Strategy-specific metrics to record
        decision_options : pd.DataFrame
            Options data used for signal generation

        Notes
        -----
        The daily history is used to:
        - Generate performance analytics
        - Plot portfolio value over time
        - Analyze strategy behavior
        - Calculate risk metrics
        """
        if not self.portfolio.history:
             # If no history yet, portfolio value is just cash
            portfolio_value = self.portfolio.cash
            cash_value = self.portfolio.cash
        else:
            last_summary = self.portfolio.history[-1]
            portfolio_value = last_summary.get('portfolio_value')
            cash_value = last_summary.get('cash')

        self.daily_history.append({
            'date': date,
            'portfolio_value': portfolio_value,
            'cash': cash_value,
            'signals': signals,  # Store signals for the next day's execution
            'decision_options': decision_options, # Store option data for metadata enrichment
            **custom_indicators
        })
    
    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Orchestrates the backtest, running the simulation day by day.
        """
        options_stream, stock_data = self._setup_data_streams()

        for monthly_chunk in options_stream:
            # ---> Group options data by date for faster access <---
            if not monthly_chunk.empty:
                # Group by the date part of the 'time' column
                grouped_options = monthly_chunk.groupby(monthly_chunk['time'].dt.date)
                # Get the unique dates from the groups to iterate over
                dates_in_chunk = sorted(grouped_options.groups.keys())
            else:
                # If the chunk is empty, create an empty list of dates to avoid errors
                grouped_options = {}
                dates_in_chunk = []

            # ---> Loop through the grouped dates and data <---
            for date_obj in (
                tqdm(dates_in_chunk, desc="Processing days")
                if self.logger.verbosity == 'high' else dates_in_chunk
            ):
                # Convert the date object back to a timezone-aware Timestamp for consistency
                date = pd.Timestamp(date_obj, tz='UTC')

                # Stop if we go past the desired end date
                if date > self.end_date_dt:
                    break

                # ---> Get the options data for the current day directly from the group <---
                current_options = grouped_options.get_group(date_obj)
                
                # 1. Get all stock data available up to and including the current day.
                #    This variable `current_stock_history_full` now contains the
                #    warm-up data + all data from the backtest start up to 'date'.
                current_stock_history_full = stock_data[stock_data['date'].dt.date <= date.date()].copy()

                # 2. Get the lookback period we saved during initialization.
                lookback_period = self.lookback_days

                stock_history_slice = None
                if lookback_period > 0:
                    # 3. If we need a lookback, get the *last N rows* from the full history.
                    #    .iloc[-N:] is the fastest way to do this.
                    #    On day 1, this slice will contain the 252 warm-up days.
                    stock_history_slice = current_stock_history_full.iloc[-lookback_period:]
                else:
                    # 4. If lookback is 0, just pass the expanding history as before.
                    stock_history_slice = current_stock_history_full

                # Retrieve signals generated on the previous day
                signals_to_execute = self.daily_history[-1].get('signals', []) if self.daily_history else []
                decision_options = self.daily_history[-1].get('decision_options', pd.DataFrame()) if self.daily_history else pd.DataFrame()
                
                # --- Daily Stages ---
                self._execute_trades(date, signals_to_execute, current_options, decision_options)
                try:
                    self._handle_events(date, current_options, stock_history_slice)
                except Exception as e:
                    self.logger.error(f"Error handling events on {date}: {str(e)}")
                
                # Get current spot price for MTM
                current_spot_price = None
                if not stock_history_slice.empty and 'close' in stock_history_slice.columns and not stock_history_slice['close'].empty:
                    current_spot_price = stock_history_slice['close'].iloc[-1]
                
                # Mark to market with current spot price
                self.portfolio.mark_to_market(
                    date, 
                    current_options.rename(columns={'symbol': 'ticker'}),
                    current_spot_price=current_spot_price
                )
                
                # 5. Call the strategy with the new, correctly-sized 'stock_history_slice'
                new_signals, custom_indicators = self.strategy.generate_signals(
                    date,
                    current_options,
                    stock_history_slice, # <-- Pass the new, smart slice
                    self.portfolio
                )
                
                self._log_daily_history(date, new_signals, custom_indicators, current_options)

        # --- AFTER the main loop finishes ---

        # Perform a final MTM on the end_date
        if self.daily_history:
            last_day_data = self.daily_history[-1]
            final_date = last_day_data['date']
            # Use 'decision_options' which holds the option data used for signals that day
            final_options_data = last_day_data.get('decision_options', pd.DataFrame())
            
            # Get final spot price from the stock data
            final_spot_row = stock_data[stock_data['date'].dt.date == final_date.date()]
            final_spot_price = final_spot_row['close'].iloc[0] if not final_spot_row.empty else None

            if not final_options_data.empty and final_spot_price is not None:
                self.logger.info(f"Performing final Mark-to-Market on {final_date.date()}...", always_show=True)
                # Ensure the DataFrame has the 'ticker' column expected by mark_to_market
                final_options_data_renamed = final_options_data.rename(columns={'symbol': 'ticker'})
                
                # Perform MTM using the final date, last available option prices, and spot price
                self.portfolio.mark_to_market(
                    final_date, 
                    final_options_data_renamed,
                    current_spot_price=final_spot_price
                )

                # Update the last history entry with the final MTM value
                if self.portfolio.history:
                    final_mtm_value = self.portfolio.history[-1].get('portfolio_value')
                    final_cash_value = self.portfolio.history[-1].get('cash')
                    if final_mtm_value is not None:
                        self.daily_history[-1]['portfolio_value'] = final_mtm_value
                        self.daily_history[-1]['cash'] = final_cash_value
            else:
                self.logger.warning(f"No options data or spot price available for final Mark-to-Market on {final_date.date()}. Using last calculated value.")
                # If no data, the last value calculated during the loop will be used
                # which might be from the day before end_date if end_date had no trades/data.

        # Prepare final output
        final_summary = pd.DataFrame([h for h in self.daily_history if 'portfolio_value' in h])
        final_trades = pd.DataFrame(self.portfolio.get_trade_history())
        
        return final_summary, final_trades