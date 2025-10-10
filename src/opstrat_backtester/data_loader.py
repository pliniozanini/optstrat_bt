import pandas as pd
from tqdm import tqdm
from typing import Generator, Optional
from datetime import datetime
from pathlib import Path

from .api_client import OplabClient
from .cache_manager import get_from_cache, set_to_cache, generate_key

api_client = None

def get_api_client():
    global api_client
    if api_client is None:
        api_client = OplabClient()
    return api_client

def _fetch_and_enrich_for_month(spot: str, year: int, month: int) -> pd.DataFrame:
    """
    Internal worker that downloads and enriches all data for a SINGLE, FULL month.
    """
    start_date = f"{year}-{month:02d}-01"
    end_of_month = pd.Period(f"{year}-{month:02d}", freq='M').end_time
    end_date = end_of_month.strftime('%Y-%m-%d')
    
    all_days_data = []
    date_range = pd.bdate_range(start=start_date, end=end_date)
    
    print(f"Downloading data for {spot} for month {year}-{month:02d}...")
    api = get_api_client()
    for day in date_range:
        date_str = day.strftime('%Y-%m-%d')
        try:
            options_list_df = api.historical_options(spot, date_str)
            if options_list_df.empty:
                continue
            tickers = options_list_df['ticker'].tolist()
            enriched_df = api.historical_instruments_details(tickers, date_str)
            if not enriched_df.empty:
                enriched_df['time'] = pd.to_datetime(date_str)
                all_days_data.append(enriched_df)
        except Exception as e:
            print(f"Warning: Could not fetch data for {date_str}. Reason: {e}")
    
    if not all_days_data:
        return pd.DataFrame()
    return pd.concat(all_days_data, ignore_index=True)

def stream_options_data(
    spot: str, 
    start_date: str, 
    end_date: str,
    cache_dir: Optional[Path] = None,
    force_redownload: bool = False
) -> Generator[pd.DataFrame, None, None]:
    """
    A memory-efficient generator that loads and yields options data one month at a time.
    
    Args:
        spot: The spot symbol to fetch data for
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        cache_dir: Optional custom cache directory path
        force_redownload: If True, ignore cache and redownload all data
    
    Yields:
        DataFrame containing options data for each month in the date range
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    # Generate the list of months we need to process
    months_to_process = pd.date_range(start=start.to_period('M').start_time, end=end, freq='MS')
    
    # --- LOGIC TO HANDLE THE CURRENT, IN-PROGRESS MONTH ---
    # We should not cache the current month until it is over, as it's incomplete by definition
    today = pd.Timestamp.now().normalize()
    is_last_month_current = (months_to_process[-1].year == today.year and 
                           months_to_process[-1].month == today.month)

    print(f"Streaming data for {spot} from {start_date} to {end_date}")
    for month_start in tqdm(months_to_process, desc="Processing Data Months"):
        year, month = month_start.year, month_start.month
        
        is_current_month_loop = (year == today.year and month == today.month)
        period_str = f"{year}-{month:02d}"
        cache_key = generate_key(data_type="options", symbol=spot, period=period_str)
        
        month_data = None
        
        # Determine if we should even attempt to use the cache
        use_cache = not force_redownload and not is_current_month_loop
        
        if use_cache:
            month_data = get_from_cache(cache_key, cache_dir=cache_dir)
            if month_data is not None:
                # ** STALE CACHE CHECK **
                # Check if the cached data covers the full historical month
                last_day_in_cache = month_data['time'].max()
                expected_last_bday = pd.Period(period_str).end_time.to_period('B').end_time
                if last_day_in_cache < expected_last_bday:
                    print(f"INFO: Stale cache found for {period_str}. Re-downloading.")
                    month_data = None  # Invalidate the cache

        if month_data is None:
            # This block runs if:
            # 1. We had a cache miss
            # 2. The cache was stale
            # 3. force_redownload is True
            # 4. We are processing the current, ongoing month
            month_data = _fetch_and_enrich_for_month(spot, year, month)
            
            # Only save to cache if the data is for a completed, historical month
            if not month_data.empty and not is_current_month_loop:
                set_to_cache(cache_key, month_data, cache_dir=cache_dir)

        if month_data is not None and not month_data.empty:
            # Filter the loaded month's data to the precise date range
            mask = (month_data['time'] >= start) & (month_data['time'] <= end)
            yield month_data.loc[mask]

def stream_stock_data(symbol: str, start_date: str, end_date: str) -> Generator[pd.DataFrame, None, None]:
    """
    Streams stock data in yearly chunks for consistency with options data streaming.
    Stock data is much smaller, but we maintain the same pattern for uniformity.
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    for year in range(start.year, end.year + 1):
        year_start = max(start, pd.Timestamp(f"{year}-01-01"))
        year_end = min(end, pd.Timestamp(f"{year}-12-31"))
        period_str = f"{year}"
        cache_key = generate_key(data_type="stock", symbol=symbol, period=period_str)
        
        cached_data = get_from_cache(cache_key)
        year_data = cached_data
        
        if year_data is None:
            print(f"Cache miss for stock data {cache_key}. Fetching from API...")
            api = get_api_client()
            year_data = api.historical_stock(symbol, year_start.strftime('%Y-%m-%d'), year_end.strftime('%Y-%m-%d'))
            if not year_data.empty:
                set_to_cache(cache_key, year_data)
        
        if year_data is not None and not year_data.empty:
            # Filter for exact date range
            mask = (year_data['date'] >= start) & (year_data['date'] <= end)
            filtered_data = year_data[mask]
            
            if not filtered_data.empty:
                yield filtered_data
