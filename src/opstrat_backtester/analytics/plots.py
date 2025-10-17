import pandas as pd
import matplotlib.pyplot as plt

def plot_pnl(results_df: pd.DataFrame, title: str = 'Portfolio Performance'):
    """
    Plot the portfolio value over time using matplotlib.

    Creates a line plot showing the evolution of the portfolio value throughout
    the backtesting period. The plot includes a grid and basic formatting.

    Parameters
    ----------
    results_df : pd.DataFrame
        Backtesting results DataFrame containing:
        - date: Trading dates
        - portfolio_value: Daily portfolio values
    title : str, optional
        Plot title. Default is 'Portfolio Performance'

    Raises
    ------
    ValueError
        If required columns are missing from results_df

    Examples
    --------
    >>> results = backtester.run()
    >>> plot_pnl(results, title='My Strategy Performance')

    Notes
    -----
    This function uses matplotlib's pyplot interface and will display
    the plot immediately in Jupyter notebooks or create a new figure window
    in scripts.
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
