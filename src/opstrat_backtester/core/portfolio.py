from typing import Dict, Any
import pandas as pd

class Portfolio:
    """
    Manages the state of the portfolio including cash, positions, and historical performance.
    Supports rich metadata for both trades and positions.
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
        Executes a trade and updates cash and positions.
        
        Args:
            trade_date: When the trade occurs
            ticker: The instrument identifier
            quantity: Number of units (+ve for buy, -ve for sell)
            price: Execution price
            metadata: Additional trade information (e.g., type, action, greeks)
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
        Updates the market value of all open positions based on the day's closing prices.
        
        Args:
            date: The date to mark positions to
            market_data: DataFrame with current market prices
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
        """Returns the current state of all positions with their metadata."""
        return self.positions

    def get_trade_history(self) -> list:
        """Returns the complete trade history with metadata."""
        return self.trades

    def get_position_type(self, ticker: str) -> str:
        """Helper to get the type of a position (e.g., 'option', 'stock')"""
        if ticker in self.positions:
            return self.positions[ticker]['metadata'].get('type', 'stock')
        return None
