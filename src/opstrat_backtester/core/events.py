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
        # This robustly gets the stock price for the specific day needed.
        stock_price_row = stock_data[stock_data['date'].dt.date == current_date.date()]
        if stock_price_row.empty:
            # Silently return; no action can be taken without the stock price.
            return
        current_stock_price = stock_price_row.iloc[0]['close']

        positions_to_check = list(portfolio.get_positions().keys())
        for ticker in positions_to_check:
            position = portfolio.get_positions().get(ticker)
            if not position or position['metadata'].get('type') != 'option':
                continue
            
            expiry_date_str = position['metadata'].get('expiry_date')
            if not expiry_date_str:
                continue

            expiry_ts = pd.to_datetime(expiry_date_str, utc=True)
            
            # This comparison is now reliable.
            if expiry_ts.date() == current_date.date():
                print(f"INFO [{current_date.date()}]: Option {ticker} has expired. Processing exercise...")
                strike = position['metadata'].get('strike', 0)
                opt_type = position['metadata'].get('option_type', '')
                qty = position['quantity']
                
                intrinsic_value = 0
                if opt_type == 'CALL':
                    intrinsic_value = max(0, current_stock_price - strike)
                elif opt_type == 'PUT':
                    intrinsic_value = max(0, strike - current_stock_price)

                action = 'EXPIRE_OTM' if intrinsic_value == 0 else 'EXERCISE_ITM'
                portfolio.add_trade(
                    trade_date=current_date, ticker=ticker, quantity=-qty,
                    price=intrinsic_value, metadata={'action': action}
                )