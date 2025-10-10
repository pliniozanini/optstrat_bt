from abc import ABC, abstractmethod
import pandas as pd
from typing import Generator, Optional
from pathlib import Path


class DataSource(ABC):
    """
    Abstract Base Class for a data source.
    Defines the interface for fetching options and stock data.
    """
    @abstractmethod
    def stream_options_data(
        self, 
        spot: str, 
        start_date: str, 
        end_date: str,
        cache_dir: Optional[Path] = None,
        force_redownload: bool = False
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Streams options data for a given spot symbol and date range.
        """
        pass

    @abstractmethod
    def stream_stock_data(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str,
        cache_dir: Optional[Path] = None,
        force_redownload: bool = False
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Streams stock data for a given symbol and date range.
        """
        pass
