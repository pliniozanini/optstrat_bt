from abc import ABC, abstractmethod
import pandas as pd

class Strategy(ABC):
    """
    Abstract base class for implementing trading strategies.

    This class defines the interface for all trading strategies in the backtesting system.
    Concrete strategy classes must inherit from this class and implement the generate_signals
    method to define their trading logic.

    The backtesting engine calls generate_signals for each trading day with current market
    data and portfolio state, expecting a list of trading signals in response.

    Examples
    --------
    >>> class BuyAndHoldStrategy(Strategy):
    ...     def generate_signals(self, date, daily_options_data, stock_history, portfolio):
    ...         if not portfolio.get_positions():  # If no positions
    ...             return [{'ticker': 'AAPL', 'quantity': 100}]  # Buy and hold
    ...         return []  # No new signals
    ...
    >>> strategy = BuyAndHoldStrategy()

    >>> class ShortStraddleStrategy(Strategy):
    ...     def generate_signals(self, date, daily_options_data, stock_history, portfolio):
    ...         signals = []
    ...         if not portfolio.get_positions():
    ...             # Find ATM options
    ...             stock_price = stock_history['close'].iloc[-1]
    ...             atm_call = daily_options_data[
    ...                 (daily_options_data['type'] == 'CALL') &
    ...                 (daily_options_data['strike'] == stock_price)
    ...             ]
    ...             atm_put = daily_options_data[
    ...                 (daily_options_data['type'] == 'PUT') &
    ...                 (daily_options_data['strike'] == stock_price)
    ...             ]
    ...             # Sell straddle
    ...             signals.extend([
    ...                 {'ticker': atm_call.iloc[0]['symbol'], 'quantity': -1},
    ...                 {'ticker': atm_put.iloc[0]['symbol'], 'quantity': -1}
    ...             ])
    ...         return signals
    """
    def __init__(self):
        pass

    @abstractmethod
    def generate_signals(self, date: pd.Timestamp, daily_options_data: pd.DataFrame, stock_history: pd.DataFrame, portfolio) -> list:
        """
        Generate trading signals based on current market data and portfolio state.

        This method is called by the backtesting engine for each trading day in 
        the simulation. Strategies should analyze the provided market data and
        portfolio state to generate appropriate trading signals.

        Parameters
        ----------
        date : pd.Timestamp
            The current date in the backtest simulation
        daily_options_data : pd.DataFrame
            DataFrame containing all available options for the current day with columns:
            - symbol: Option contract identifier
            - time: Timestamp of the data
            - spot: Underlying symbol
            - type: Option type ('CALL' or 'PUT')
            - due_date: Option expiration date
            - strike: Strike price
            - premium: Option premium
            - maturity_type: Type of maturity
            - days_to_maturity: Days until expiration
            - moneyness: Option moneyness measure
            - delta: Option delta
            - gamma: Option gamma
            - vega: Option vega
            - theta: Option theta
            - rho: Option rho
            - volatility: Implied volatility
            - poe: Probability of exercise
            - bs: Black-Scholes price
            - open/high/low/close: Trading prices
            - volume: Trading volume
            - financial_volume: Volume in currency
            - ewma_current: Exponentially weighted moving average
            - parent_symbol: Underlying stock symbol
            - spot_price: Current price of underlying
            - category: Option category
            - due_date_detail: Detailed expiration info
            - days_to_maturity_detail: Detailed maturity info
            - strike_detail: Detailed strike info
            - premium_detail: Detailed premium info
            - maturity_type_detail: Detailed maturity type info
        stock_history : pd.DataFrame
            Historical stock data up to the current date with columns:
            - date: Trading date
            - open: Opening price
            - high: High price
            - low: Low price
            - close: Closing price
            - fvolume: Trading volume
        portfolio : Portfolio
            The current portfolio state object to check existing positions

        Returns
        -------
        list
            List of dictionaries containing trading signals with keys:
            - ticker: Instrument to trade (required)
            - quantity: Number of units (required, positive for buy, negative for sell)
            - action: Trading action (optional, e.g., 'BUY', 'SELL')
            Additional metadata can be included and will be stored with the trade

        Examples
        --------
        >>> def generate_signals(self, date, daily_options_data, stock_history, portfolio):
        ...     signals = []
        ...     if not portfolio.get_positions():  # If no positions
        ...         # Buy 100 shares
        ...         signals.append({
        ...             'ticker': 'AAPL',
        ...             'quantity': 100,
        ...             'action': 'BUY'
        ...         })
        ...     return signals
        """
        pass
