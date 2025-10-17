from abc import ABC, abstractmethod
import pandas as pd
from .portfolio import Portfolio

class EventHandler(ABC):
    """
    Abstract base class for processing market events in the backtester.

    Event handlers are responsible for simulating market events that affect
    the portfolio state, such as option expirations, stock splits, dividends,
    and corporate actions. The backtester processes all registered event
    handlers at the start of each trading day.

    To implement a custom event handler:
    1. Subclass EventHandler
    2. Implement the handle() method
    3. Register the handler with the backtester

    Examples
    --------
    >>> class DividendHandler(EventHandler):
    ...     def handle(self, current_date, portfolio, market_data, stock_data):
    ...         # Check for dividend events
    ...         dividend_events = get_dividends(current_date)
    ...         for event in dividend_events:
    ...             # Adjust portfolio cash and stock prices
    ...             portfolio.cash += event['amount'] * portfolio.positions[event['ticker']]['quantity']
    
    >>> # Register with backtester
    >>> handlers = [OptionExpirationHandler(), DividendHandler()]
    >>> backtester = Backtester(
    ...     spot_symbol='AAPL',
    ...     strategy=strategy,
    ...     event_handlers=handlers,
    ...     start_date='2023-01-01',
    ...     end_date='2023-12-31'
    ... )

    See Also
    --------
    OptionExpirationHandler : Concrete handler for option expirations
    Backtester : Main backtesting engine class
    Portfolio : Portfolio management class
    """
    @abstractmethod
    def handle(self, current_date: pd.Timestamp, portfolio: Portfolio, market_data: pd.DataFrame, stock_data: pd.DataFrame):
        """
        Process market events for the current simulation day.

        This method is called by the backtester at the start of each trading
        day. Implementations should check for relevant market events and
        update the portfolio state accordingly.

        Parameters
        ----------
        current_date : pd.Timestamp
            The current date in the simulation
        portfolio : Portfolio
            The portfolio object that can be modified to reflect event impacts
        market_data : pd.DataFrame
            Options market data for the current day containing:
            - symbol: Option contract identifier
            - type: Option type ('CALL' or 'PUT')
            - strike: Strike price
            - expiry_date: Expiration date
            Additional fields may be available
        stock_data : pd.DataFrame
            Stock market data up to the current day with:
            - date: Trading date
            - open, high, low, close: Price data
            - volume: Trading volume
            Additional fields may be available

        Returns
        -------
        None
            The portfolio object is modified in place

        Notes
        -----
        Common market events to handle:
        - Option expirations and exercises
        - Stock splits and reverse splits
        - Cash and stock dividends
        - Mergers and acquisitions
        - Trading halts
        """
        pass

class OptionExpirationHandler(EventHandler):
    """
    Event handler for processing option expirations.

    This handler checks for any option positions that expire on the current
    date and processes them according to their intrinsic value:
    - In-the-money options are exercised at their intrinsic value
    - Out-of-the-money options expire worthless

    The handler updates the portfolio by:
    1. Identifying expiring options
    2. Calculating their intrinsic value
    3. Closing the positions with appropriate P&L

    Examples
    --------
    >>> handler = OptionExpirationHandler()
    >>> handler.handle(
    ...     current_date=pd.Timestamp('2023-01-21'),
    ...     portfolio=portfolio,
    ...     market_data=options_data,
    ...     stock_data=stock_data
    ... )

    Notes
    -----
    - Call option intrinsic value = max(0, stock_price - strike)
    - Put option intrinsic value = max(0, strike - stock_price)
    - The handler logs expiration events for transparency
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