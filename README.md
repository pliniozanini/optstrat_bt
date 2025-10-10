# opstrat_backtester

A robust, modular Python library for backtesting options strategies using Brazilian market data. Designed for speed, reliability, and ease of use.

## Features
- Efficient data loading and caching (API + local cache)
- Abstract strategy interface for custom trading logic
- Portfolio management with P&L tracking
- Analytics: performance metrics and plotting
- Extensible architecture for new strategies and analytics

## Installation

```bash
pip install -e .
```
Or use Poetry/Hatch for modern dependency management.

## Quickstart Example

```python
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.core.strategy import Strategy
from opstrat_backtester.analytics.plots import plot_pnl
from opstrat_backtester.data_loader import load_and_cache_options_data, load_and_cache_stock_data

class MyStrategy(Strategy):
    def generate_signals(self, date, daily_options_data, stock_history, portfolio):
        # Your trading logic here
        return []

SPOT = "PETR4"
YEAR = 2023

options_data = load_and_cache_options_data(SPOT, YEAR)
stock_data = load_and_cache_stock_data(SPOT, YEAR)

my_strategy = MyStrategy()
backtester = Backtester(
    spot_symbol=SPOT,
    strategy=my_strategy,
    start_date=f"{YEAR}-01-01",
    end_date=f"{YEAR}-12-31"
)
results = backtester.run()
plot_pnl(results, title=f"{SPOT} Strategy Performance")
```

## Data Layer Architecture
- **api_client.py**: Handles all API communication, authentication, and error handling.
- **cache_manager.py**: Manages deterministic cache keys, in-memory and disk caching (Parquet).
- **data_loader.py**: Orchestrates cache checks, API calls, and aggregation for options and stock data.

## Strategy Development
Subclass `Strategy` and implement `generate_signals`:
```python
class MyStrategy(Strategy):
    def generate_signals(self, date, daily_options_data, stock_history, portfolio):
        # Example: Buy if price up, sell if price down
        signals = []
        # ...
        return signals
```

## Analytics
- `analytics/plots.py`: Plot portfolio value over time
- `analytics/stats.py`: Calculate Sharpe ratio, max drawdown, etc.

## Configuration
- Set your OpLab API token in the environment:
  ```bash
  export OPLAB_ACCESS_TOKEN=your_token_here
  ```

## Contributing
Pull requests and issues are welcome!

## License
MIT