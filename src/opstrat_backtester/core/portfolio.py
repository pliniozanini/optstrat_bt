from typing import Dict, Any
import pandas as pd

class Portfolio:
    """
    A class representing a trading portfolio that manages positions, cash, and performance tracking.

    The Portfolio class handles all aspects of position management, trade execution,
    and portfolio valuation. It supports rich metadata for both trades and positions,
    making it suitable for complex option strategies and multi-asset portfolios.

    Parameters
    ----------
    initial_cash : float, optional
        Initial cash balance in the portfolio. Default is 100,000.

    Attributes
    ----------
    cash : float
        Current cash balance in the portfolio
    positions : dict
        Dictionary of current positions with detailed metadata
    history : list
        Historical record of daily portfolio values
    trades : list
        Complete log of all executed trades with metadata

    Examples
    --------
    >>> portfolio = Portfolio(initial_cash=100_000)
    >>> portfolio.add_trade(
    ...     trade_date=pd.Timestamp('2023-01-01'),
    ...     ticker='AAPL',
    ...     quantity=100,
    ...     price=150.0,
    ...     metadata={'type': 'stock'}
    ... )
    >>> portfolio.get_positions()
    {'AAPL': {'quantity': 100, 'cost_basis': 150.0, 'metadata': {'type': 'stock'}}}
    """
    def __init__(self, initial_cash: float = 100_000):
        self.cash = initial_cash
        self.positions = {}  # Enhanced position tracking with metadata
        self.history = []    # Log of daily portfolio value
        self.trades = []     # Log of all trades with metadata

    def add_trade(
        self, 
        trade_date: pd.Timestamp, 
        ticker: str, 
        quantity: int, 
        price: float,
        metadata: Dict[str, Any] = None
    ):
        """
        Execute a trade and update the portfolio state.

        This method processes a trade by updating the cash balance, position sizes,
        cost basis, and maintaining detailed trade records with metadata. It supports
        both simple stock trades and complex derivative trades with rich metadata.

        Parameters
        ----------
        trade_date : pd.Timestamp
            The timestamp when the trade occurs
        ticker : str
            The instrument identifier (e.g., stock symbol, option contract code)
        quantity : int
            Number of units to trade (positive for buy, negative for sell)
        price : float
            Execution price per unit
        metadata : dict, optional
            Additional trade information like:
            - type: 'stock' or 'option'
            - action: 'BUY', 'SELL'
            - option_type: 'CALL' or 'PUT'
            - strike: Strike price for options
            - expiry_date: Option expiration date
            - delta: Option delta
            - hedged_stock_ticker: For delta hedging

        Returns
        -------
        bool
            True if the trade was successfully executed

        Examples
        --------
        >>> # Simple stock trade
        >>> portfolio.add_trade(
        ...     pd.Timestamp('2023-01-01'),
        ...     'AAPL',
        ...     100,  # Buy 100 shares
        ...     150.0,  # at $150 per share
        ...     {'type': 'stock'}
        ... )
        
        >>> # Option trade with metadata
        >>> portfolio.add_trade(
        ...     pd.Timestamp('2023-01-01'),
        ...     'AAPL230121C150',
        ...     -1,  # Sell 1 contract
        ...     5.0,  # at $5.00 premium
        ...     {
        ...         'type': 'option',
        ...         'option_type': 'CALL',
        ...         'strike': 150.0,
        ...         'expiry_date': '2023-01-21'
        ...     }
        ... )
        """
        metadata = metadata or {}
        trade_cost = quantity * price

        self.cash -= trade_cost
        
        # Record the trade with full metadata
        trade_record = {
            'date': trade_date,
            'ticker': ticker,
            'quantity': quantity,
            'price': price,
            'cost': trade_cost,
            **metadata  # Include all additional metadata
        }
        self.trades.append(trade_record)
        
        # Update or create position
        if ticker not in self.positions:
            self.positions[ticker] = {
                'quantity': 0,
                'cost_basis': 0,
                'metadata': {}  # Position-level metadata
            }
            
        position = self.positions[ticker]
        old_quantity = position['quantity']
        new_quantity = old_quantity + quantity
        
        # Update cost basis (for buys) and metadata
        if quantity > 0:
            old_cost = position['cost_basis'] * old_quantity
            new_cost = trade_cost
            position['cost_basis'] = (old_cost + new_cost) / new_quantity if new_quantity > 0 else 0
            
        position['quantity'] = new_quantity
        
        # Update position metadata
        position['metadata'].update({
            k: v for k, v in metadata.items() 
            if k in ['type', 'expiry_date', 'strike', 'option_type', 'delta', 'hedged_stock_ticker']
        })
        
        # Remove position if closed
        if position['quantity'] == 0:
            del self.positions[ticker]
            
        return True

    def mark_to_market(self, date: pd.Timestamp, market_data: pd.DataFrame):
        """
        Mark the portfolio positions to market using closing prices.

        This method calculates the current market value of all positions using 
        provided market data and updates the portfolio's historical record. It 
        handles missing market data gracefully by printing warnings.

        Parameters
        ----------
        date : pd.Timestamp
            The date to mark positions to
        market_data : pd.DataFrame
            DataFrame containing current market data with columns:
            - ticker: Instrument identifier
            - close: Closing price
            Additional columns are ignored

        Examples
        --------
        >>> market_data = pd.DataFrame({
        ...     'ticker': ['AAPL', 'GOOGL'],
        ...     'close': [150.0, 2800.0]
        ... })
        >>> portfolio.mark_to_market(
        ...     pd.Timestamp('2023-01-01'),
        ...     market_data
        ... )
        """
        total_value = self.cash
        
        for ticker, position in self.positions.items():
            # Find the closing price for the ticker in today's market data
            try:
                current_price = market_data.loc[market_data['ticker'] == ticker, 'close'].iloc[0]
                market_value = position['quantity'] * current_price
                position['market_value'] = market_value
                position['last_price'] = current_price
                total_value += market_value
            except (KeyError, IndexError):
                print(f"Warning: No market data found for {ticker} on {date}")
        
        self.history.append({
            'date': date,
            'portfolio_value': total_value,
            'cash': self.cash
        })

    def get_positions(self) -> dict:
        """
        Get the current state of all positions.

        Returns
        -------
        dict
            Dictionary where keys are tickers and values are position details including:
            - quantity: Current position size
            - cost_basis: Average cost basis
            - metadata: Additional position information (type, expiry, etc.)
            - market_value: Most recent market value (if marked to market)
            - last_price: Most recent price (if marked to market)

        Examples
        --------
        >>> positions = portfolio.get_positions()
        >>> for ticker, pos in positions.items():
        ...     print(f"{ticker}: {pos['quantity']} @ {pos['cost_basis']}")
        """
        return self.positions

    def get_trade_history(self) -> list:
        """
        Get the complete history of all trades.

        Returns
        -------
        list
            List of dictionaries containing trade details including:
            - date: Trade execution date
            - ticker: Instrument identifier
            - quantity: Trade size
            - price: Execution price
            - cost: Total trade cost
            - metadata: Additional trade information

        Examples
        --------
        >>> trades = portfolio.get_trade_history()
        >>> for trade in trades:
        ...     print(f"{trade['date']}: {trade['ticker']} x {trade['quantity']}")
        """
        return self.trades

    def get_position_type(self, ticker: str) -> str:
        """
        Get the type of a specific position.

        Parameters
        ----------
        ticker : str
            The instrument identifier to check

        Returns
        -------
        str or None
            The position type ('stock', 'option', etc.) or None if position not found

        Examples
        --------
        >>> portfolio.get_position_type('AAPL')
        'stock'
        >>> portfolio.get_position_type('AAPL230121C150')
        'option'
        """
        if ticker in self.positions:
            return self.positions[ticker]['metadata'].get('type', 'stock')
        return None
