from typing import Dict, Any
import pandas as pd
import logging

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
    def __init__(self, initial_cash: float = 100_000, stale_price_days: int = 3):
        self.cash = initial_cash
        self.positions = {}  # Enhanced position tracking with metadata
        self.history = []    # Log of daily portfolio value
        self.trades = []     # Log of all trades with metadata
        self.stale_price_days = stale_price_days

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
                'market_value': 0,
                'last_price': price,
                'last_price_date': trade_date,
                'metadata': metadata or {}
            }
        else:
            self.positions[ticker]['metadata'].update(metadata or {})
            
        position = self.positions[ticker]
        old_quantity = position['quantity']
        new_quantity = old_quantity + quantity
        
        # Update cost basis (for buys) and metadata
        if quantity > 0:
            old_cost = position['cost_basis'] * old_quantity
            new_cost = trade_cost
            position['cost_basis'] = (old_cost + new_cost) / new_quantity if new_quantity > 0 else 0
            
            # Update last price for new buys
            position['last_price'] = price
            position['last_price_date'] = trade_date
            
        position['quantity'] = new_quantity
        
        # Update position metadata with allowed fields
        allowed_metadata = ['type', 'due_date', 'strike', 'option_type', 'delta', 'hedged_stock_ticker']
        position['metadata'].update({
            k: v for k, v in (metadata or {}).items() 
            if k in allowed_metadata
        })
        
        # Remove position if closed
        if position['quantity'] == 0:
            del self.positions[ticker]
            
        return True

    def mark_to_market(self, date: pd.Timestamp, market_data: pd.DataFrame, current_spot_price: float = None):
        """
        Mark portfolio to market using Stale Price with Conservative Fallback.
        
        This method implements a tiered approach to position valuation:
        1. First tries to use current market data
        2. Falls back to stale prices within the grace period
        3. For options, falls back to intrinsic value when stale
        4. Marks to zero as a last resort

        Parameters
        ----------
        date : pd.Timestamp
            The date to mark positions to
        market_data : pd.DataFrame
            DataFrame containing current market data with columns:
            - ticker: Instrument identifier
            - close: Closing price
        current_spot_price : float, optional
            Current underlying spot price, required for intrinsic value calculation
        """
        total_value = self.cash
        positions_missing_data = []

        # ---> ADD THIS: Create a Series for quick price lookups by ticker <---
        # Set 'ticker' as the index and select the 'close' column.
        # Use an empty Series if market_data is None or empty to prevent errors.
        market_prices = market_data.set_index('ticker')['close'] if market_data is not None and not market_data.empty else pd.Series(dtype=float)

        for ticker, position in self.positions.items():
            current_price = None
            price_source = "N/A"

            # Skip non-option positions
            is_option = position['metadata'].get('type') == 'option' if position.get('metadata') else False
            
            # 1. Try to find price in today's market data using the fast lookup
            try:
                # Try getting the price directly using the ticker index
                current_price = market_prices.loc[ticker]
                # Check if the found price is valid (not NaN)
                if pd.notna(current_price):
                    price_source = "MARKET_CLOSE"
                    position['last_price'] = current_price
                    position['last_price_date'] = date
                else:
                    # If price is NaN, treat it as not found
                    raise KeyError("Price is NaN")
                    
            except (KeyError, IndexError):
                # 2. Price Missing - Check staleness
                days_stale = (date - position.get('last_price_date', date)).days
                
                if days_stale <= self.stale_price_days:
                    # 2a. Grace Period: Use last known price
                    current_price = position.get('last_price', 0)
                    price_source = f"STALE_FWD ({days_stale}d)"
                    logging.warning(
                        f"[{date.date()}] MTM for {ticker}: No price. "
                        f"Using stale price {current_price:.2f} from {position.get('last_price_date', 'N/A')}."
                    )
                
                # 3. Price Stale: Fallback to Intrinsic Value (for options)
                elif is_option and current_spot_price is not None:
                    positions_missing_data.append(ticker)
                    strike = position['metadata'].get('strike')
                    option_type = position['metadata'].get('option_type')

                    if strike is None or option_type is None:
                        # Failsafe: Cannot calculate intrinsic, mark to zero
                        current_price = 0.0
                        price_source = "ZERO (STALE/NO_METADATA)"
                        logging.error(
                            f"[{date.date()}] MTM for {ticker}: Price stale ({days_stale}d). "
                            f"FALLING BACK TO ZERO (missing strike or option_type)."
                        )
                    else:
                        # Calculate intrinsic value
                        if option_type.upper() == 'PUT':
                            intrinsic_value = max(0, strike - current_spot_price)
                        else:  # CALL
                            intrinsic_value = max(0, current_spot_price - strike)
                        
                        current_price = intrinsic_value
                        price_source = f"INTRINSIC (STALE {days_stale}d)"
                        logging.warning(
                            f"[{date.date()}] MTM for {ticker}: Price stale ({days_stale}d). "
                            f"FALLING BACK TO INTRINSIC VALUE: {current_price:.2f}"
                        )
                    
                    # Update last_price to this new conservative value
                    position['last_price'] = current_price
                    position['last_price_date'] = date
                
                else:
                    # For non-options or missing spot price, mark to zero
                    current_price = 0.0
                    price_source = "ZERO (NO_DATA)"
                    logging.error(
                        f"[{date.date()}] MTM for {ticker}: No price data "
                        f"and no fallback available. Marking to zero."
                    )

            # Calculate final value for this position
            if current_price is not None:
                market_value = position['quantity'] * current_price
                position['market_value'] = market_value
                position['mtm_price_source'] = price_source
                total_value += market_value

        # Record the portfolio value for this date
        self.history.append({
            'date': date,
            'portfolio_value': total_value,
            'cash': self.cash,
            'positions_value': total_value - self.cash,
            'missing_mtm_tickers': positions_missing_data
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
