You asked for the complete markdown content of the revised `README.md` file so you can directly copy and paste it.

Here is the content:

````markdown
# opstrat_backtester

[![PyPI Version](https://badge.fury.io/py/opstrat_backtester.svg)](https://badge.fury.io/py/opstrat_backtester)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modular Python library for backtesting options strategies using data from the Brazilian market.

`opstrat_backtester` provides a straightforward, event-driven engine to help you test your trading ideas without getting bogged down in data fetching and portfolio management boilerplate.

---

## Core Features

* **Efficient Data Handling**: Implements an intelligent local caching system using Parquet files. Data is fetched from the API only once, making subsequent backtests significantly faster.
* **Modular Architecture**: Built with a `DataSource` abstraction, allowing for future integration of other data providers beyond the current Oplab implementation.
* **Event-Driven Engine**: The backtesting engine processes each day sequentially, handling trades, market events (like expirations), and portfolio valuation in a clear and logical order.
* **Simple Strategy Interface**: To create a new strategy, you only need to subclass the `Strategy` class and implement your logic in the `generate_signals` method.
* **Analytics Included**: Comes with basic functions to plot profit-and-loss curves and calculate common performance statistics.

---

## Prerequisites

**IMPORTANT**: This library is designed to work with the Oplab API. You **must** have a valid Oplab API access key with permission to retrieve **historical data**.

---

## Installation

Install the package directly from PyPI:

```bash
pip install opstrat_backtester
````

To set up for development, clone the repository and install with the `dev` dependencies:

```bash
pip install -e .[dev]
```

-----

## How to Use

### 1\. Set Your API Token

The backtester requires your Oplab API token to be set as an environment variable.

```bash
export OPLAB_ACCESS_TOKEN="your_token_here"
```

### 2\. Define Your Strategy

Create a class that inherits from `Strategy` and implements your trading logic.

```python
from opstrat_backtester.core.strategy import Strategy
import pandas as pd

class MyStrategy(Strategy):
    def generate_signals(self, date: pd.Timestamp, daily_options_data: pd.DataFrame, stock_history: pd.DataFrame, portfolio):
        # Your trading logic goes here.
        # This method should return a list of trade signals.
        # Example: [{'ticker': 'PETRA123', 'quantity': 10}]
        signals = []
        custom_indicators = {} # Optional dictionary for logging custom data
        return signals, custom_indicators
```

### 3\. Run the Backtest

Instantiate the `OplabDataSource`, your strategy, and the `Backtester`, then run the simulation.

```python
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.data_loader import OplabDataSource
from opstrat_backtester.analytics.plots import plot_pnl

# --- Configuration ---
SPOT_SYMBOL = "BOVA11"
START_DATE = "2023-01-01"
END_DATE = "2024-03-31"

# 1. Instantiate the Data Source
data_source = OplabDataSource()

# 2. Instantiate your Strategy
my_strategy = MyStrategy()

# 3. Set up and run the Backtester
backtester = Backtester(
    strategy=my_strategy,
    start_date=START_DATE,
    end_date=END_DATE,
    spot_symbol=SPOT_SYMBOL
)
backtester.set_data_source(data_source)

# 4. Get the results
daily_summary_df, trades_df = backtester.run()

# 5. Plot the performance
plot_pnl(daily_summary_df, title=f"{SPOT_SYMBOL} Strategy Performance")
```

For complete, runnable examples, please see the Jupyter notebooks provided in the repository, such as `example_delta_hedging.ipynb` and `example_vol_trading.ipynb`.

-----

## Architecture Overview

  * **`api_client.py`**: Handles all communication with the Oplab API.
  * **`cache_manager.py`**: Manages both in-memory and on-disk (Parquet) caching to minimize API calls.
  * **`data_loader.py`**: Orchestrates data fetching and caching, acting as the bridge between the API client and the backtesting engine.
  * **`core/engine.py`**: The main backtesting engine that drives the simulation.
  * **`core/strategy.py`**: Contains the abstract `Strategy` class that you must implement.
  * **`core/portfolio.py`**: Manages portfolio state, including cash, positions, and trade history.

-----

## Contributing

Pull requests and issues are welcome. If you'd like to contribute, please feel free to fork the repository and submit a pull request with your changes.

-----

## License

This project is licensed under the MIT License.

```
```