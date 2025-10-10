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
        Implements pessimistic pricing: buy at next day's high, sell at next day's low.
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

        stock_data = pd.concat(list(stream_stock_data(
            self.spot_symbol,
            self.start_date.strftime('%Y-%m-%d'),
            self.end_date.strftime('%Y-%m-%d')
        )))

        for monthly_chunk in options_stream:
            dates_in_chunk = sorted(monthly_chunk['time'].dt.date.unique())

            # Iterate with index to allow for lookahead to the next day
            for i, current_date in enumerate(tqdm(dates_in_chunk, desc=f"Processing {dates_in_chunk[0].strftime('%Y-%m')}")):
                # Stop before the last day in the chunk to avoid an index error
                if i + 1 >= len(dates_in_chunk):
                    continue

                # 1. Get data for the current and next day
                next_date = dates_in_chunk[i + 1]
                options_for_day = monthly_chunk[monthly_chunk['time'].dt.date == current_date]
                options_for_next_day = monthly_chunk[monthly_chunk['time'].dt.date == next_date]
                
                # 2. Get stock history for signal generation
                stock_history = stock_data[stock_data['date'].dt.date <= current_date]
                
                # 3. Mark portfolio to market
                self.portfolio.mark_to_market(current_date, options_for_day)
                
                # 4. Generate signals using current day's data
                signals = self.strategy.generate_signals(
                    current_date,
                    options_for_day,
                    stock_history,
                    self.portfolio
                )
                
                # 5. Execute signals using next day's pessimistic prices
                for trade in signals:
                    try:
                        if trade['quantity'] > 0:  # Buy order
                            trade_price = options_for_next_day.loc[
                                options_for_next_day['ticker'] == trade['ticker'], 'high'
                            ].iloc[0]
                        else:  # Sell order
                            trade_price = options_for_next_day.loc[
                                options_for_next_day['ticker'] == trade['ticker'], 'low'
                            ].iloc[0]

                        # Execute the trade logged on the next day's date
                        self.portfolio.add_trade(
                            next_date,
                            trade['ticker'],
                            trade['quantity'],
                            trade_price,
                            trade.get('metadata', {})
                        )
                    except (KeyError, IndexError):
                        print(f"Warning: Could not find next-day market data for {trade['ticker']} on {next_date}. Trade skipped.")

        print("Backtest complete.")
        return pd.DataFrame(self.portfolio.history)
