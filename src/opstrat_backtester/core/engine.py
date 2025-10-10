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
        
    def run(self) -> pd.DataFrame:
        """
        Runs the backtest simulation using memory-efficient data streaming.
        
        The data streaming is handled internally - users only need to provide
        start and end dates. Cache validation and management is handled
        automatically.
        """
        from ..data_loader import stream_options_data, stream_stock_data
        
        print("Starting backtest using streaming data...")
        options_stream = stream_options_data(
            self.spot_symbol,
            self.start_date.strftime('%Y-%m-%d'),
            self.end_date.strftime('%Y-%m-%d'),
            cache_dir=self.cache_dir_path,
            force_redownload=self.force_redownload
        )
        
        # Load stock data for the entire period (it's much smaller)
        stock_data = pd.concat(list(stream_stock_data(
            self.spot_symbol,
            self.start_date.strftime('%Y-%m-%d'),
            self.end_date.strftime('%Y-%m-%d')
        )))
        
        # Process data month by month
        for monthly_chunk in options_stream:
            # Get all unique dates in this month's data
            dates_in_chunk = sorted(monthly_chunk['time'].dt.date.unique())
            
            for current_date in tqdm(dates_in_chunk, desc=f"Processing {dates_in_chunk[0].strftime('%Y-%m')}"):
                # 1. Get daily options data
                options_for_day = monthly_chunk[monthly_chunk['time'].dt.date == current_date]
                
                # 2. Get stock history up to current date
                stock_history = stock_data[stock_data['date'].dt.date <= current_date]
                
                # 3. Update portfolio value (Mark-to-Market)
                self.portfolio.mark_to_market(current_date, options_for_day)
                
                # 4. Generate and execute trading signals
                signals = self.strategy.generate_signals(
                    current_date,
                    options_for_day,
                    stock_history,
                    self.portfolio
                )
                
                for trade in signals:
                    trade_price = options_for_day.loc[
                        options_for_day['ticker'] == trade['ticker'],
                        'close'
                    ].iloc[0]
                    self.portfolio.add_trade(
                        current_date,
                        trade['ticker'],
                        trade['quantity'],
                        trade_price
                    )
        
        print("Backtest complete.")
        return pd.DataFrame(self.portfolio.history)
