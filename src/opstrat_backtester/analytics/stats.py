import pandas as pd
import numpy as np

def calculate_sharpe_ratio(results_df: pd.DataFrame, risk_free_rate: float = 0.0):
    """
    Calculate the annualized Sharpe Ratio for the portfolio.

    The Sharpe Ratio measures the risk-adjusted return of the portfolio by calculating
    the excess return (over risk-free rate) per unit of risk (standard deviation of returns).
    The ratio is annualized assuming 252 trading days per year.

    Parameters
    ----------
    results_df : pd.DataFrame
        Backtesting results DataFrame containing:
        - portfolio_value: Daily portfolio values
    risk_free_rate : float, optional
        Annual risk-free rate (e.g., 0.02 for 2%). Default is 0.0

    Returns
    -------
    float
        Annualized Sharpe Ratio

    Examples
    --------
    >>> results = backtester.run()
    >>> sharpe = calculate_sharpe_ratio(results, risk_free_rate=0.02)
    >>> print(f"Sharpe Ratio: {sharpe:.2f}")
    """
    returns = results_df['portfolio_value'].pct_change().dropna()
    excess_returns = returns - risk_free_rate / 252
    sharpe = np.sqrt(252) * excess_returns.mean() / excess_returns.std()
    return sharpe

def calculate_max_drawdown(results_df: pd.DataFrame):
    """
    Calculate the maximum drawdown and its duration.

    Maximum drawdown measures the largest peak-to-trough decline in portfolio value.
    This function calculates both the maximum percentage drop from a peak and the
    duration of that drawdown period.

    Parameters
    ----------
    results_df : pd.DataFrame
        Backtesting results DataFrame containing:
        - portfolio_value: Daily portfolio values

    Returns
    -------
    tuple
        (max_drawdown, duration) where:
        - max_drawdown (float): Maximum percentage decline from peak (-0.20 means 20% decline)
        - duration (pd.Timedelta): Time period between peak and trough

    Examples
    --------
    >>> results = backtester.run()
    >>> max_dd, dd_duration = calculate_max_drawdown(results)
    >>> print(f"Max Drawdown: {max_dd:.1%} over {dd_duration.days} days")
    """
    cumulative = results_df['portfolio_value'].cummax()
    drawdown = results_df['portfolio_value'] / cumulative - 1
    max_drawdown = drawdown.min()
    end_idx = drawdown.idxmin()
    start_idx = (drawdown[:end_idx] == 0).idxmax()
    duration = end_idx - start_idx
    return max_drawdown, duration
