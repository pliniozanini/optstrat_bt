import pandas as pd
from tqdm import tqdm
from typing import Optional
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

        stock_data = pd.concat(list(self.data_source.stream_stock_data(
            spot=self.spot_symbol,
            start_date=self.start_date,
            end_date=self.end_date
        )))

        for monthly_chunk in options_stream:
            dates_in_chunk = sorted(monthly_chunk['time'].dt.date.unique())
            
            # Process each trading day
            for i, date in enumerate(dates_in_chunk[:-1]):  # Skip last day for signals
                current_options = monthly_chunk[monthly_chunk['time'].dt.date == date]
                stock_slice = stock_data[stock_data['date'].dt.date <= date]
                
                # Get trading signals for the day
                signals = self.strategy.generate_signals(
                    date=date,
                    daily_options_data=current_options,
                    stock_history=stock_slice,
                    portfolio=self.portfolio
                )
                
                # Execute signals on the next day at pessimistic prices
                next_day = dates_in_chunk[i + 1]
                next_day_data = monthly_chunk[monthly_chunk['time'].dt.date == next_day]
                
                for signal in signals:
                    qty = signal['quantity']
                    ticker = signal['ticker']
                    
                    # Find the ticker's data for tomorrow
                    ticker_data = next_day_data[next_day_data['ticker'] == ticker]
                    if ticker_data.empty:
                        print(f"Warning: No data found for {ticker} on {next_day}")
                        continue
                        
                    # Use high price for buys, low price for sells
                    price = ticker_data['high'].iloc[0] if qty > 0 else ticker_data['low'].iloc[0]
                    self.portfolio.add_trade(next_day, ticker, qty, price)
                
                # Update portfolio value using closing prices
                self.portfolio.mark_to_market(date, next_day_data)

        return pd.DataFrame(self.portfolio.history)
