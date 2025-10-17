import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any, Tuple
from .strategy import Strategy
from .portfolio import Portfolio
from .events import EventHandler, OptionExpirationHandler
from ..data.datasource import DataSource

class Backtester:
    """
    A modular, event-driven backtesting engine for options strategies.

    This engine processes each day of a simulation in a clear, sequential order:
    1. Executes trades based on signals from the previous day.
    2. Handles market events for the current day (e.g., option expirations).
    3. Values the portfolio (Mark-to-Market).
    4. Calls the user-defined strategy to generate new signals for the next day.
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
        """Assigns a data source to the backtester."""
        self.data_source = data_source

    def _setup_data_streams(self):
        """Loads and prepares the necessary options and stock data streams."""
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
        """Executes trades based on signals from the previous day."""
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
        """Processes daily market events, such as expirations."""
        if self.event_handlers:
            for handler in self.event_handlers:
                handler.handle(date, self.portfolio, current_options, stock_slice)

    def _log_daily_history(self, date: pd.Timestamp, signals: List[Dict], custom_indicators: Dict, decision_options: pd.DataFrame):
        """Records the state of the portfolio at the end of the day."""
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