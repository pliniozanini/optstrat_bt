import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any, Tuple
from .strategy import Strategy
from .portfolio import Portfolio
from .events import EventHandler, OptionExpirationHandler
from ..data.datasource import DataSource

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
        event_handlers: Optional[List[EventHandler]] = None
    ):
        self.spot_symbol = spot_symbol
        self.strategy = strategy
        self.start_date = pd.to_datetime(start_date, utc=True)
        self.end_date = pd.to_datetime(end_date, utc=True)
        self.portfolio = Portfolio(initial_cash)
        self.data_source: Optional[DataSource] = None
        self.event_handlers = event_handlers or [OptionExpirationHandler()]
        
        self.trade_log: List[Dict[str, Any]] = []
        self.daily_history: List[Dict[str, Any]] = []

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
            spot=self.spot_symbol, start_date=self.start_date, end_date=self.end_date
        )
        stock_data = pd.concat(list(self.data_source.stream_stock_data(
            symbol=self.spot_symbol, start_date=self.start_date, end_date=self.end_date
        )))
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
            
            # Retrieve original option data to enrich metadata
            decision_row = decision_options[decision_options['symbol'] == ticker].iloc[0]
            trade_metadata = {
                'type': 'option',
                'option_type': decision_row.get('type'),
                'expiry_date': decision_row.get('expiry_date'),
                'strike': decision_row.get('strike'),
                'action': 'BUY' if qty > 0 else 'SELL'
            }
            self.portfolio.add_trade(date, ticker, qty, price, metadata=trade_metadata)

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
            dates_in_chunk = sorted(pd.to_datetime(monthly_chunk['time'].dt.date.unique(), utc=True))

            for date in tqdm(dates_in_chunk, desc="Processing days"):
                if date > self.end_date:
                    break

                current_options = monthly_chunk[monthly_chunk['time'].dt.date == date.date()]
                stock_slice = stock_data[stock_data['date'].dt.date <= date.date()]
                
                # Retrieve signals generated on the previous day
                signals_to_execute = self.daily_history[-1].get('signals', []) if self.daily_history else []
                decision_options = self.daily_history[-1].get('decision_options', pd.DataFrame()) if self.daily_history else pd.DataFrame()
                
                # --- Daily Stages ---
                self._execute_trades(date, signals_to_execute, current_options, decision_options)
                self._handle_events(date, current_options, stock_slice)
                self.portfolio.mark_to_market(date, current_options.rename(columns={'symbol': 'ticker'}))
                
                # This stage preserves the original, user-facing strategy interface
                new_signals, custom_indicators = self.strategy.generate_signals(
                    date=date,
                    daily_options_data=current_options,
                    stock_history=stock_slice,
                    portfolio=self.portfolio
                )
                
                self._log_daily_history(date, new_signals, custom_indicators, current_options)

        # Prepare final output
        final_summary = pd.DataFrame([h for h in self.daily_history if 'portfolio_value' in h])
        final_trades = pd.DataFrame(self.portfolio.get_trade_history())
        
        return final_summary, final_trades