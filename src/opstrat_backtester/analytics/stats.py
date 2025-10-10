import pandas as pd
import numpy as np

def calculate_sharpe_ratio(results_df: pd.DataFrame, risk_free_rate: float = 0.0):
    """
    Calculates the Sharpe Ratio for the portfolio.
    """
    returns = results_df['portfolio_value'].pct_change().dropna()
    excess_returns = returns - risk_free_rate / 252
    sharpe = np.sqrt(252) * excess_returns.mean() / excess_returns.std()
    return sharpe

def calculate_max_drawdown(results_df: pd.DataFrame):
    """
    Calculates the maximum drawdown and its duration.
    """
    cumulative = results_df['portfolio_value'].cummax()
    drawdown = results_df['portfolio_value'] / cumulative - 1
    max_drawdown = drawdown.min()
    end_idx = drawdown.idxmin()
    start_idx = (drawdown[:end_idx] == 0).idxmax()
    duration = end_idx - start_idx
    return max_drawdown, duration
