# src/opstrat_backtester/data_loader.py

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
        start = (
            pd.to_datetime(start_date).tz_localize('UTC') if pd.to_datetime(start_date).tz is None
            else pd.to_datetime(start_date)
        )
        end = (
            pd.to_datetime(end_date).tz_localize('UTC') if pd.to_datetime(end_date).tz is None
            else pd.to_datetime(end_date)
        )
        # Create a date range for the months to be processed
        months_to_process = pd.date_range(start=start.replace(day=1), end=end, freq='MS')
        
        today = pd.Timestamp.now(tz='UTC').normalize()

        print(f"Streaming data for {spot} from {start_date} to {end_date}")
        for month_start in tqdm(months_to_process, desc="Processing Data Months"):
            year, month = month_start.year, month_start.month

            is_current_month_loop = (year == today.year and month == today.month)
            period_str = f"{year}-{month:02d}"
            cache_key = generate_key(data_type="options", symbol=spot, period=period_str)

            month_data = None
            # Do not use cache for the current, ongoing month
            use_cache = not force_redownload and not is_current_month_loop

            if use_cache:
                month_data = get_from_cache(cache_key, cache_dir=cache_dir)

            # If cache is missed or stale, fetch from the API
            if month_data is None:
                # This is the corrected, more efficient fetching logic
                month_data = self._fetch_and_enrich_for_month(spot, year, month)
                if not month_data.empty and not is_current_month_loop:
                    set_to_cache(cache_key, month_data, cache_dir=cache_dir)
                if not month_data.empty:
                    month_data['time'] = pd.to_datetime(month_data['time'], utc=True)
                

            # Yield the relevant slice of the monthly data for the backtest
            if month_data is not None and not month_data.empty:
                mask = (month_data['time'] >= start) & (month_data['time'] <= end)
                yield month_data.loc[mask]

    def _fetch_and_enrich_for_month(self, spot: str, year: int, month: int) -> pd.DataFrame:
        """
        Fetches all options data for a given spot for an entire month with a single API call.
        This is much more efficient than the previous day-by-day approach.
        """
        start_date = f"{year}-{month:02d}-01"
        end_of_month = pd.Period(f"{year}-{month:02d}", freq='M').end_time
        end_date = end_of_month.strftime('%Y-%m-%d')
        
        print(f"Downloading data for {spot} for month {year}-{month:02d}...")
        try:
            # Use the updated API client to fetch the whole month at once
            monthly_df = self.api_client.historical_options(spot, start_date, end_date)
            
            if monthly_df.empty:
                return pd.DataFrame()

            # Ensure the 'time' column is in datetime format
            monthly_df['time'] = pd.to_datetime(monthly_df['time'])

            # Call historical_instruments_details to enrich the data
            unique_tickers = monthly_df['symbol'].unique().tolist()
            dates = pd.to_datetime(monthly_df['time']).dt.date.unique()
            details_dfs = []
            for date in dates:
                date_str = pd.Timestamp(date).strftime('%Y-%m-%d')
                details_df = self.api_client.historical_instruments_details(unique_tickers, date_str)
                if not details_df.empty:
                    details_dfs.append(details_df)
            details_dfs = pd.concat(details_dfs) if details_dfs else pd.DataFrame()
            if not details_dfs.empty:
                details_dfs['time'] = pd.to_datetime(details_dfs['time'])
                # Merge the details back into the monthly options data on 'symbol'/'ticker' and 'date'
                monthly_df = monthly_df.merge(details_dfs, how='left', on=['symbol', 'time'],suffixes=('', '_detail'))
                # Drop redundant columns if any
                monthly_df.drop(columns=['ticker', 'date'], errors='ignore', inplace=True)
            else:
                raise ValueError("No instrument details returned from API.")
            return monthly_df

        except Exception as e:
            import traceback
            traceback.print_exc()
            raise f"Warning: Could not fetch data for month {year}-{month:02d}. Reason: {e}"
            # return pd.DataFrame()


    def stream_stock_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        cache_dir: Optional[Path] = None,
        force_redownload: bool = False
    ) -> Generator[pd.DataFrame, None, None]:
        # check if start_date and end_date are timezone-aware
        start = (
            pd.to_datetime(start_date).tz_localize('UTC') if pd.to_datetime(start_date).tz is None
            else pd.to_datetime(start_date)
        )
        end = (
            pd.to_datetime(end_date).tz_localize('UTC') if pd.to_datetime(end_date).tz is None
            else pd.to_datetime(end_date)
        )
        print(f"Streaming stock data for {symbol} from {start_date} to {end_date}")

        for year in range(start.year, end.year + 1):
            print(f"Processing stock data for year {year}...")
            year_start = max(start, pd.Timestamp(f"{year}-01-01").tz_localize('UTC'))
            year_end = min(end, pd.Timestamp(f"{year}-12-31").tz_localize('UTC'))
            # if current year, year end should not exceed today
            today = pd.Timestamp.now(tz='UTC').normalize()
            if year == today.year:
                year_end = min(year_end, today)
            period_str = f"{year}"
            cache_key = generate_key(data_type="stock", symbol=symbol, period=period_str)

            year_data = get_from_cache(cache_key, cache_dir) if not force_redownload else None
            if year_data is not None and not year_data.empty:
                if year_data['date'].dt.tz is None:
                    year_data['date'] = year_data['date'].dt.tz_localize('UTC')
            
            # Check if cached data contains full year range
            if year_data is not None and not year_data.empty:
                if year_data['date'].min() > year_start or year_data['date'].max() < year_end:
                    print(f"Cache miss for stock data {cache_key}. Fetching from API...")
                    year_data = None  # Invalidate cache if it doesn't cover the full year range

            if year_data is None:
                print(f"Cache miss for stock data {cache_key}. Fetching from API...")
                year_data = self.api_client.historical_stock(symbol, year_start.strftime('%Y-%m-%d'), year_end.strftime('%Y-%m-%d'))
                if not year_data.empty:
                    year_data['date'] = pd.to_datetime(year_data['date'], utc=True)
                    set_to_cache(cache_key, year_data, cache_dir)

            if year_data is not None and not year_data.empty:
                # breakpoint()
                # print(f'year data starts in {year_data["date"].min()} and ends in {year_data["date"].max()}')
                # print(start, end)
                mask = (year_data['date'] >= start) & (year_data['date'] <= end)
                # print((year_data['date'] >= start).sum(), (year_data['date'] <= end).sum())
                # print(f"Mask from {year_start} to {year_end} yields {mask} records.")
                filtered_data = year_data.loc[mask]
                # print(f"Yielding {len(filtered_data)} records for year {year}, with date range"
                #       f" {filtered_data['date'].min()} to {filtered_data['date'].max()}")

                if not filtered_data.empty:
                    yield filtered_data
          