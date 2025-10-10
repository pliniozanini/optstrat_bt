# Examples

This directory contains example scripts and notebooks that demonstrate how to use the opstrat-backtester library.

## Files

- `../run_backtest.py`: A complete example showing how to implement a simple delta-hedging strategy and run a backtest. This example demonstrates:
  - Strategy definition and implementation
  - Backtester configuration and execution
  - Data streaming and caching
  - Results analysis

## Running the Examples

1. First, ensure you have installed the library in development mode:
```bash
# From project root
poetry install
```

2. Run the backtest example:
```bash
# From project root
poetry run python run_backtest.py
```

## Expected Output

### First Run (Cache Miss)
The first time you run the example, it will need to fetch data from the API and cache it:

```
--- Starting Backtest Execution ---
Processing Data Months: 0%|          | 0/3 [00:00]
Cache miss for options/PETR4/2023-01. Fetching full month from API...
Downloading data for PETR4 for month 2023-01...
... API calls for Jan 2023 ...
Saving data for 2023-01 to cache.
Processing Data Months: 33%|███▎      | 1/3 [00:15]
... [Engine processes all days in Jan] ...
...
--- Backtest Execution Complete ---
```

### Second Run (Cache Hit)
Subsequent runs will be much faster as they use cached data:

```
--- Starting Backtest Execution ---
Processing Data Months: 0%|          | 0/3 [00:00]
Loaded options data for PETR4 for 2023-01 from cache.
Processing Data Months: 33%|███▎      | 1/3 [00:00]
...
--- Backtest Execution Complete ---
```