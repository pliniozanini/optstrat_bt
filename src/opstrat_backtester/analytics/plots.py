import pandas as pd
import matplotlib.pyplot as plt

def plot_pnl(results_df: pd.DataFrame, title: str = 'Portfolio Performance'):
    """
    Plots the portfolio value over time.

    Args:
        results_df: The DataFrame returned by the Backtester's run() method.
                    It must contain 'date' and 'portfolio_value' columns.
    """
    if 'date' not in results_df.columns or 'portfolio_value' not in results_df.columns:
        raise ValueError("Results DataFrame must contain 'date' and 'portfolio_value' columns.")

    plt.figure(figsize=(12, 8))
    plt.plot(results_df['date'], results_df['portfolio_value'], label='Portfolio Value')
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Portfolio Value ($)')
    plt.grid(True)
    plt.legend()
    plt.show()
