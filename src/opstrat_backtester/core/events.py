from abc import ABC, abstractmethod
import pandas as pd
from .portfolio import Portfolio

class EventHandler(ABC):
    """
    Abstract Base Class for an event handler.
    Event handlers are processed by the backtester at the start of each day
    to simulate market events like expirations, dividends, etc.
    """
    @abstractmethod
    def handle(self, current_date: pd.Timestamp, portfolio: Portfolio, market_data: pd.DataFrame, stock_data: pd.DataFrame):
        """
        The core logic of the event handler.

        Args:
            current_date: The current date of the simulation.
            portfolio: The portfolio object to be modified.
            market_data: Options data for the current day.
            stock_data: Stock data up to the current day.
        """
        pass

class OptionExpirationHandler(EventHandler):
    """
    A concrete event handler for processing option expirations.
    """
    def handle(self, current_date: pd.Timestamp, portfolio: Portfolio, market_data: pd.DataFrame, stock_data: pd.DataFrame):
        if stock_data.empty:
            return # Cannot process expirations without the underlying's price

        current_stock_price = stock_data.iloc[-1]['close']
        positions_to_check = list(portfolio.get_positions().keys())

        for ticker in positions_to_check:
            position = portfolio.get_positions().get(ticker)
            
            # Skip non-option positions or those without metadata
            if not position or position['metadata'].get('type') != 'option':
                continue
            
            expiry_date_str = position['metadata'].get('expiry_date')
            if not expiry_date_str:
                continue

            expiry_date = pd.to_datetime(expiry_date_str, utc=True).date()

            if expiry_date == current_date.date():
                strike = position['metadata'].get('strike', 0)
                opt_type = position['metadata'].get('option_type', '')
                qty = position['quantity']
                
                intrinsic_value = 0
                if opt_type == 'Call':
                    intrinsic_value = max(0, current_stock_price - strike)
                elif opt_type == 'Put':
                    intrinsic_value = max(0, strike - current_stock_price)

                action = 'EXPIRE_OTM' if intrinsic_value == 0 else 'EXERCISE_ITM'
                price = intrinsic_value

                portfolio.add_trade(
                    trade_date=current_date,
                    ticker=ticker,
                    quantity=-qty, # Close the position
                    price=price,
                    metadata={'action': action}
                )