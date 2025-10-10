from __future__ import annotations
import pandas as pd
from typing import Generator, Optional
from pathlib import Path

# --- Import the new DataSource and the OplabClient ---
from .data.datasource import DataSource
from .api_client import OplabClient
from .cache_manager import get_from_cache, set_to_cache, generate_key
from tqdm import tqdm


class OplabDataSource(DataSource):
    """A data source implementation that fetches data from the Oplab API."""
    def __init__(self, api_client: Optional[OplabClient] = None):
        self.api_client = api_client or OplabClient()

    def stream_options_data(
        self, 
        spot: str, 
        start_date: str, 
        end_date: str, 
        cache_dir: Optional[Path] = None, 
        force_redownload: bool = False
    ) -> Generator[pd.DataFrame, None, None]:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        months_to_process = pd.date_range(start=start.to_period('M').start_time, end=end, freq='MS')
        
        today = pd.Timestamp.now().normalize()
        
        print(f"Streaming data for {spot} from {start_date} to {end_date}")
        for month_start in tqdm(months_to_process, desc="Processing Data Months"):
            year, month = month_start.year, month_start.month
            
            is_current_month_loop = (year == today.year and month == today.month)
            period_str = f"{year}-{month:02d}"
            cache_key = generate_key(data_type="options", symbol=spot, period=period_str)
            
            month_data = None
            use_cache = not force_redownload and not is_current_month_loop
            
            if use_cache:
                month_data = get_from_cache(cache_key, cache_dir=cache_dir)
                if month_data is not None:
                    last_day_in_cache = month_data['time'].max()
                    expected_last_bday = pd.Period(period_str).end_time.to_period('B').end_time
                    if last_day_in_cache < expected_last_bday:
                        print(f"INFO: Stale cache found for {period_str}. Re-downloading.")
                        month_data = None
            
            if month_data is None:
                month_data = self._fetch_and_enrich_for_month(spot, year, month)
                if not month_data.empty and not is_current_month_loop:
                    set_to_cache(cache_key, month_data, cache_dir=cache_dir)
            
            if month_data is not None and not month_data.empty:
                mask = (month_data['time'] >= start) & (month_data['time'] <= end)
                yield month_data.loc[mask]

    def _fetch_and_enrich_for_month(self, spot: str, year: int, month: int) -> pd.DataFrame:
        start_date = f"{year}-{month:02d}-01"
        end_of_month = pd.Period(f"{year}-{month:02d}", freq='M').end_time
        end_date = end_of_month.strftime('%Y-%m-%d')
        
        all_days_data = []
        date_range = pd.bdate_range(start=start_date, end=end_date)
        
        print(f"Downloading data for {spot} for month {year}-{month:02d}...")
        for day in date_range:
            date_str = day.strftime('%Y-%m-%d')
            try:
                options_list_df = self.api_client.historical_options(spot, date_str)
                if options_list_df.empty:
                    continue
                tickers = options_list_df['ticker'].tolist()
                enriched_df = self.api_client.historical_instruments_details(tickers, date_str)
                if not enriched_df.empty:
                    enriched_df['time'] = pd.to_datetime(date_str)
                    all_days_data.append(enriched_df)
            except Exception as e:
                print(f"Warning: Could not fetch data for {date_str}. Reason: {e}")
        
        if not all_days_data:
            return pd.DataFrame()
        return pd.concat(all_days_data, ignore_index=True)

    def stream_stock_data(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str, 
        cache_dir: Optional[Path] = None, 
        force_redownload: bool = False
    ) -> Generator[pd.DataFrame, None, None]:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        for year in range(start.year, end.year + 1):
            year_start = max(start, pd.Timestamp(f"{year}-01-01"))
            year_end = min(end, pd.Timestamp(f"{year}-12-31"))
            period_str = f"{year}"
            cache_key = generate_key(data_type="stock", symbol=symbol, period=period_str)
            
            year_data = get_from_cache(cache_key, cache_dir) if not force_redownload else None
            
            if year_data is None:
                print(f"Cache miss for stock data {cache_key}. Fetching from API...")
                year_data = self.api_client.historical_stock(symbol, year_start.strftime('%Y-%m-%d'), year_end.strftime('%Y-%m-%d'))
                if not year_data.empty:
                    set_to_cache(cache_key, year_data, cache_dir)
            
            if year_data is not None and not year_data.empty:
                mask = (year_data['date'] >= start) & (year_data['date'] <= end)
                filtered_data = year_data[mask]
                
                if not filtered_data.empty:
                    yield filtered_data
