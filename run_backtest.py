# run_backtest.py

import pandas as pd
from typing import List, Dict, Any, Optional

# --- Import from the installed library ---
from opstrat_backtester.core.strategy import Strategy
from opstrat_backtester.core.engine import Backtester
from opstrat_backtester.analytics.plots import plot_pnl  # This is implemented now  

# ==============================================================================
# 1. Define the Strategy (User-facing code)
# ==============================================================================

class SimpleDeltaHedgeStrategy(Strategy):
    """
    A simple, illustrative strategy:
    1. Buy a Call Option 60 DTE.
    2. Sell (Hedge) the delta amount of the underlying stock.
    3. Hold until 30 DTE, then close the position.
    
    NOTE: This is a highly simplified example. Real strategies are more complex.
    """
    def __init__(self, initial_dte: int = 60, exit_dte: int = 30):
        super().__init__()
        self.initial_dte = initial_dte
        self.exit_dte = exit_dte

    def generate_signals(
        self, 
        date: pd.Timestamp, 
        daily_options_data: pd.DataFrame, 
        stock_history: pd.DataFrame, 
        portfolio
    ) -> List[Dict[str, Any]]:
        
        signals = []
        
        # --- 1. Position Management (Exit/Closing Logic) ---
        positions = portfolio.get_positions()
        for ticker, pos in positions.items():
            # Check if this is an options position using metadata
            if pos['metadata'].get('type') == 'option':
                # Close position after holding period
                if (date - pos['metadata']['open_date']).days >= self.initial_dte - self.exit_dte:
                    # Close the option position
                    close_price = daily_options_data.loc[
                        daily_options_data['ticker'] == ticker, 'close'
                    ].iloc[0]
                    signals.append({
                        'ticker': ticker,
                        'quantity': -pos['quantity'],
                        'price': close_price,
                        'metadata': {
                            'type': 'option',
                            'action': 'SELL_TO_CLOSE'
                        }
                    })
                    
                    # Close the corresponding hedge position
                    hedge_ticker = pos['metadata']['hedged_stock_ticker']
                    if hedge_ticker in positions:
                        hedge_pos = positions[hedge_ticker]
                        signals.append({
                            'ticker': hedge_ticker,
                            'quantity': -hedge_pos['quantity'],
                            'price': stock_history.loc[stock_history.index[-1], 'close'],
                            'metadata': {
                                'type': 'stock',
                                'action': 'BUY_TO_CLOSE'
                            }
                        })

        # --- 2. New Trade Entry Logic ---
        # Only enter a new position if no options are held
        if not any(pos['metadata'].get('type') == 'option' for pos in positions.values()):
            # Find the target call option
            valid_options = daily_options_data[
                (daily_options_data['option_type'] == 'Call') &
                (daily_options_data['dte'].between(self.initial_dte - 5, self.initial_dte + 5))
            ]
            
            if not valid_options.empty:
                target_call = valid_options.sort_values(by='delta', ascending=False).iloc[0]
                
                # 2a. Buy the option (e.g., 1 contract)
                option_quantity = 1  # 1 contract = 100 shares equivalent
                signals.append({
                    'ticker': target_call['ticker'],
                    'quantity': option_quantity,
                    'price': target_call['close'],
                    'metadata': {
                        'type': 'option',
                        'action': 'BUY_TO_OPEN',
                        'option_type': 'Call',
                        'strike': target_call['strike'],
                        'expiry_date': target_call['expiry_date'],
                        'delta': target_call['delta'],
                        'open_date': date,
                        'hedged_stock_ticker': 'PETR4'
                    }
                })
                
                # 2b. Hedge the position by selling the underlying stock
                hedge_quantity = int(option_quantity * 100 * target_call['delta'])
                signals.append({
                    'ticker': 'PETR4',
                    'quantity': -hedge_quantity,
                    'price': stock_history.loc[stock_history.index[-1], 'close'],
                    'metadata': {
                        'type': 'stock',
                        'action': 'SELL_TO_OPEN',
                        'hedge_quantity': hedge_quantity,
                        'hedged_option': target_call['ticker']
                    }
                })
        
        return signals

# ==============================================================================
# 2. Configure and Run the Backtest (User-facing code)
# ==============================================================================

if __name__ == "__main__":
    # --- Configuration ---
    SPOT_SYMBOL = "PETR4"
    START_DATE = "2023-01-01"
    END_DATE = "2023-03-31"
    INITIAL_CASH = 100_000.00
    
    # Optional: Override the default cache location using an Environment Variable
    # os.environ['OPSTRAT_CACHE_DIR'] = '/tmp/my_custom_cache' 
    
    # 1. Instantiate the Strategy
    my_strategy = SimpleDeltaHedgeStrategy(initial_dte=60, exit_dte=30)

    # 2. Instantiate the Backtester
    # NOTE: The Backtester instance handles ALL data loading/caching/streaming internally.
    backtester = Backtester(
        spot_symbol=SPOT_SYMBOL,
        strategy=my_strategy,
        start_date=START_DATE,
        end_date=END_DATE,
        initial_cash=INITIAL_CASH,
        # cache_dir='/mnt/large_drive/my_cache' # Can also be passed directly
    )

    # 3. Run the Backtest (Streaming happens here)
    print("\n--- Starting Backtest Execution ---")
    results_df = backtester.run()
    print("--- Backtest Execution Complete ---")

    # 4. Analyze Results
    print("\n--- Final Portfolio History (last 5 days) ---")
    print(results_df.tail())
    
    # Plot the performance using the analytics module
    plot_pnl(results_df, title=f"{SPOT_SYMBOL} Delta Hedge Strategy Performance")