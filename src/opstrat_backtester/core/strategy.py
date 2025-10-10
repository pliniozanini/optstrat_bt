from abc import ABC, abstractmethod
import pandas as pd

class Strategy(ABC):
    """
    Abstract Base Class for a trading strategy.
    Users must inherit from this class and implement the `generate_signals` method.
    """
    def __init__(self):
        pass

    @abstractmethod
    def generate_signals(self, date: pd.Timestamp, daily_options_data: pd.DataFrame, stock_history: pd.DataFrame, portfolio) -> list:
        """
        The core logic of the strategy.

        Args:
            date: The current date of the backtest simulation.
            daily_options_data: DataFrame containing all available options for the current day.
            stock_history: Historical stock data up to the current date.
            portfolio: The current state of the portfolio object. Useful for checking existing positions.

        Returns:
            A list of trade orders. E.g., [{'ticker': 'PETR4', 'quantity': 100, 'action': 'BUY'}]
        """
        pass
