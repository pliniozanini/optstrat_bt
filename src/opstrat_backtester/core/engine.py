import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from .strategy import Strategy
from .portfolio import Portfolio


class Backtester:
    def __init__(
        self, 
        spot_symbol: str, 
        strategy: Strategy, 
        start_date: str, 
        end_date: str, 
        initial_cash: float = 100_000,
        cache_dir: Optional[str] = None,
        force_redownload: bool = False
    ):
        self.spot_symbol = spot_symbol
        self.strategy = strategy
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.portfolio = Portfolio(initial_cash)
        self.cache_dir_path = Path(cache_dir) if cache_dir else None
        self.force_redownload = force_redownload
        self.data_source = None

    def set_data_source(self, data_source):
        """Set a custom data source for testing or alternative data providers."""
        self.data_source = data_source

    def run(self) -> pd.DataFrame:
        """
        Runs the backtest simulation using memory-efficient data streaming.
        Implements pessimistic pricing: buy at next day's high, sell at next day's low.
        """
        if self.data_source is None:
            from ..data_loader import OplabDataSource
            self.data_source = OplabDataSource()

        print("Starting backtest using streaming data...")
        options_stream = self.data_source.stream_options_data(
            spot=self.spot_symbol,
            start_date=self.start_date,
            end_date=self.end_date
        )

        print(f"Loading stock data between {self.start_date.date()} and {self.end_date.date()}...")
        stock_data = pd.concat(list(self.data_source.stream_stock_data(
            symbol=self.spot_symbol,
            start_date=self.start_date,
            end_date=self.end_date
        )))


        for monthly_chunk in options_stream:
            print(f"Processing options data chunk from {monthly_chunk['time'].min().date()} to {monthly_chunk['time'].max().date()}...")
            print(monthly_chunk)
            dates_in_chunk = sorted(pd.to_datetime(monthly_chunk['time'].dt.date.unique(), utc=True))
            
            # Process each trading day
            for i, date in enumerate(dates_in_chunk[:-1]):  # Skip last day for signals
                current_options = monthly_chunk[monthly_chunk['time'].dt.date == date.date()]
                print('stock_data')
                print(stock_data)
                print(stock_data['date'].dt.date.max(), stock_data['date'].dt.date.min())
                print(date.date())
                stock_slice = stock_data[stock_data['date'].dt.date <= date.date()]
                
                # Get trading signals for the day
                signals = self.strategy.generate_signals(
                    date=date,
                    daily_options_data=current_options,
                    stock_history=stock_slice,
                    portfolio=self.portfolio
                )
                
                # Execute signals on the next day at pessimistic prices
                next_day = dates_in_chunk[i + 1]
                next_day_data = monthly_chunk[monthly_chunk['time'].dt.date == next_day.date()]

                print(f"Executing trades for {next_day.date()} based on signals from {date.date()}...")
                print(next_day_data)
                
                for signal in signals:
                    qty = signal['quantity']
                    ticker = signal['ticker']
                    
                    # Find the ticker's data for tomorrow
                    ticker_data = next_day_data[next_day_data['symbol'] == ticker]
                    if ticker_data.empty:
                        print(f"Warning: No data found for {ticker} on {next_day}")
                        continue
                        
                    # Use high price for buys, low price for sells
                    price = ticker_data['high'].iloc[0] if qty > 0 else ticker_data['low'].iloc[0]
                    self.portfolio.add_trade(next_day, ticker, qty, price)
                
                # Update portfolio value using closing prices
                self.portfolio.mark_to_market(date, next_day_data)

        return pd.DataFrame(self.portfolio.history)

class Backtester2:
    def __init__(
        self,
        spot_symbol: str,
        strategy: Strategy,
        start_date: str,
        end_date: str,
        initial_cash: float = 100_000,
        cache_dir: Optional[str] = None,
        force_redownload: bool = False
    ):
        self.spot_symbol = spot_symbol
        self.strategy = strategy
        self.start_date = pd.to_datetime(start_date, utc=True)
        self.end_date = pd.to_datetime(end_date, utc=True)
        # Use your provided Portfolio class
        self.portfolio = Portfolio(initial_cash)
        self.cache_dir_path = Path(cache_dir) if cache_dir else None
        self.force_redownload = force_redownload
        self.data_source = None
        
        self.trade_log: List[Dict[str, Any]] = []
        self.daily_history: List[Dict[str, Any]] = []

    def set_data_source(self, data_source):
        self.data_source = data_source

    def _prefix_dict_keys(self, d: Dict[str, Any], prefix: str) -> Dict[str, Any]:
        return {f"{prefix}_{key}": val for key, val in d.items()}

    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if self.data_source is None:
            # from ..data_loader import OplabDataSource
            # self.data_source = OplabDataSource()
            raise ValueError("Data source has not been set. Call set_data_source() before run().")

        print("Starting backtest...")
        options_stream = self.data_source.stream_options_data(
            spot=self.spot_symbol,
            start_date=self.start_date,
            end_date=self.end_date
        )

        stock_data = pd.concat(list(self.data_source.stream_stock_data(
            symbol=self.spot_symbol,
            start_date=self.start_date,
            end_date=self.end_date
        )))

        for monthly_chunk in options_stream:
            print(f"Processing chunk: {monthly_chunk['time'].min().date()} to {monthly_chunk['time'].max().date()}")
            dates_in_chunk = sorted(pd.to_datetime(monthly_chunk['time'].dt.date.unique(), utc=True))
            
            for i, date in enumerate(tqdm(dates_in_chunk[:-1], desc="Processing days")):
                next_day = dates_in_chunk[i + 1]
                current_options = monthly_chunk[monthly_chunk['time'].dt.date == date.date()]
                next_day_options = monthly_chunk[monthly_chunk['time'].dt.date == next_day.date()]
                stock_slice = stock_data[stock_data['date'].dt.date <= date.date()]

                signals_output = self.strategy.generate_signals(
                    date=date,
                    daily_options_data=current_options,
                    stock_history=stock_slice,
                    portfolio=self.portfolio
                )
                
                if isinstance(signals_output, tuple) and len(signals_output) == 2:
                    signals, custom_indicators = signals_output
                else:
                    signals, custom_indicators = signals_output, {}

                for signal in signals:
                    ticker, qty = signal['ticker'], signal['quantity']
                    log_record = {'decision_date': date, 'execution_date': next_day, 'ticker': ticker, 'signal_quantity': qty}
                    
                    decision_data = current_options[current_options['symbol'] == ticker]
                    if not decision_data.empty:
                        log_record.update(self._prefix_dict_keys(decision_data.iloc[0].to_dict(), 'decision'))

                    execution_data = next_day_options[next_day_options['symbol'] == ticker]
                    if execution_data.empty:
                        log_record['trade_status'] = 'FAILED_NO_DATA'
                    else:
                        price = execution_data['high'].iloc[0] if qty > 0 else execution_data['low'].iloc[0]
                        
                        ## --- CHANGE 1: Call add_trade with metadata --- ##
                        # Your Portfolio class accepts metadata, so we provide it.
                        trade_metadata = {'action': 'BUY' if qty > 0 else 'SELL'}
                        self.portfolio.add_trade(next_day, ticker, qty, price, metadata=trade_metadata)
                        
                        log_record['trade_status'] = 'EXECUTED'
                        log_record['execution_price'] = price
                        log_record.update(self._prefix_dict_keys(execution_data.iloc[0].to_dict(), 'execution'))
                    
                    log_record.update(custom_indicators)
                    self.trade_log.append(log_record)

                ## --- CHANGE 2: Prepare data for mark_to_market --- ##
                # Your Portfolio class expects a 'ticker' column, not 'symbol'.
                market_data_for_mtm = next_day_options.rename(columns={'symbol': 'ticker'})
                self.portfolio.mark_to_market(date, market_data_for_mtm)
                
                ## --- CHANGE 3: Get values from portfolio history --- ##
                # Instead of calling non-existent methods, we get the latest value
                # calculated by the mark_to_market method above.
                if self.portfolio.history:
                    last_day_summary = self.portfolio.history[-1]
                    portfolio_value = last_day_summary.get('portfolio_value', 0)
                    cash_value = last_day_summary.get('cash', 0)
                    positions_value = portfolio_value - cash_value
                else: # Fallback for the very first day
                    portfolio_value = self.portfolio.cash
                    cash_value = self.portfolio.cash
                    positions_value = 0
                
                daily_record = {
                    'date': date,
                    'portfolio_value': portfolio_value,
                    'cash': cash_value,
                    'positions_value': positions_value,
                }
                daily_record.update(custom_indicators)
                self.daily_history.append(daily_record)

        trades_df = pd.DataFrame(self.trade_log)
        daily_summary_df = pd.DataFrame(self.daily_history)
        
        return daily_summary_df, trades_df