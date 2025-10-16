# run_backtest_new.py
import os
import pandas as pd
from typing import List, Dict, Any, Tuple

# --- Environment Setup ---
# IMPORTANT: Replace with your actual Oplab API token if needed
os.environ['OPLAB_ACCESS_TOKEN'] = 'uLLxDL4kBs6QvP/hRulMIuhZ1GtHwM8ypI0pnpAw/FgBbfpp8o3VwvDgQa2OzzVe--oREjDB52hEftOK22LlBspw==--ZThiZWM3ODFjOTkyYjRlNTA2MDg1NTU0N2NlOWRmMzg=' 
os.environ['OPSTRAT_CACHE_DIR'] = '/workspaces/optstrat_bt/.cache'

# --- Import from the library ---
from opstrat_backtester.core.strategy import Strategy
from opstrat_backtester.core.engine import Backtester2
from opstrat_backtester.analytics.plots import plot_pnl
from opstrat_backtester.data_loader import OplabDataSource

## ==============================================================================
## 1. Define the Strategy (New, Simplified Version)
## ==============================================================================

class NewDeltaHedgeStrategy(Strategy):
    """
    A simplified delta-hedging strategy using the new engine features.
    
    Logic:
    1. If no option position is open, buy an ATM Call Option with ~60 DTE and 
       sell the underlying stock to hedge the delta.
    2. If an option position's DTE drops to 30 or less, close both the 
       option and the stock hedge.
       
    Key Simplifications:
    - No need to manually track 'open_date' in metadata. We check the DTE 
      from the daily data feed.
    - No need to specify price or metadata in signals. The engine handles it.
    - Returns optional custom indicators for richer analysis.
    """
    def __init__(self, spot_symbol: str, initial_dte: int = 60, exit_dte: int = 30):
        super().__init__()
        self.spot_symbol = spot_symbol
        self.initial_dte = initial_dte
        self.exit_dte = exit_dte

    def generate_signals(
        self,
        date: pd.Timestamp,
        daily_options_data: pd.DataFrame,
        stock_history: pd.DataFrame,
        portfolio
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:

        signals = []
        custom_indicators = {}
        positions = portfolio.get_positions()
        has_option_position = any(ticker != self.spot_symbol for ticker in positions)

        # --- 1. Position Exit Logic ---
        if has_option_position:
            for ticker, pos in positions.items():
                if ticker == self.spot_symbol:
                    continue # Skip the stock hedge for now

                # Find the current data for our held option
                option_data = daily_options_data[daily_options_data['symbol'] == ticker]
                
                if not option_data.empty:
                    current_dte = option_data['days_to_maturity'].iloc[0]
                    # Check if it's time to exit
                    if current_dte <= self.exit_dte:
                        print(f"INFO [{date.date()}]: Closing {ticker} (DTE: {current_dte})")
                        # Signal to close the option
                        signals.append({'ticker': ticker, 'quantity': -pos['quantity']})
                        
                        # Signal to close the corresponding stock hedge
                        if self.spot_symbol in positions:
                            signals.append({
                                'ticker': self.spot_symbol, 
                                'quantity': -positions[self.spot_symbol]['quantity']
                            })
                        break # Exit loop after finding one position to close

        # --- 2. New Position Entry Logic ---
        elif not has_option_position:
            # Find a suitable call option to buy
            valid_options = daily_options_data[
                (daily_options_data['type'] == 'CALL') &
                (daily_options_data['days_to_maturity'].between(self.initial_dte - 7, self.initial_dte + 7))
            ]
            
            if not valid_options.empty:
                # Select the call closest to at-the-money (delta near 0.5)
                target_call = valid_options.iloc[(valid_options['delta'] - 0.5).abs().argsort()[:1]]
                
                if not target_call.empty:
                    target_call = target_call.iloc[0]
                    print(f"INFO [{date.date()}]: Opening new position with {target_call['symbol']}")

                    # a) Signal to buy the option (e.g., 1 contract)
                    option_quantity = 1
                    signals.append({'ticker': target_call['symbol'], 'quantity': option_quantity})
                    
                    # b) Signal to hedge by selling the underlying stock
                    hedge_quantity = int(option_quantity * 100 * target_call['delta'])
                    signals.append({'ticker': self.spot_symbol, 'quantity': -hedge_quantity})

                    # Add custom indicator for this day's log
                    custom_indicators['entry_option_delta'] = target_call['delta']

        return signals, custom_indicators

## ==============================================================================
## 2. Configure and Run the Backtest
## ==============================================================================

if __name__ == "__main__":
    # --- Configuration ---
    SPOT_SYMBOL = "BOVA11"
    START_DATE = "2024-01-01"
    END_DATE = "2024-03-31"
    INITIAL_CASH = 100_000.00

    

    # 2. Instantiate the NEW Strategy
    my_new_strategy = NewDeltaHedgeStrategy(spot_symbol=SPOT_SYMBOL, initial_dte=60, exit_dte=30)

    # 3. Instantiate the Backtester
    backtester = Backtester2(
        strategy=my_new_strategy,
        start_date=START_DATE,
        end_date=END_DATE,
        spot_symbol=SPOT_SYMBOL,
        initial_cash=INITIAL_CASH,
    )
    backtester.set_data_source(oplab_data_source)

    # 4. Run the Backtest - IT NOW RETURNS TWO DATAFRAMES!
    print("\n--- Starting Backtest ---")
    daily_summary_df, trades_df = backtester.run()
    print("--- Backtest Complete ---")

    # 5. Analyze Results
    print("\n--- Daily Portfolio Summary (last 5 days) ---")
    print(daily_summary_df.tail())

    print("\n\n--- Detailed Trade Log ---")
    # Set display options to see all columns of the detailed log
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    print(trades_df)

    # 6. Plot the results using the daily summary
    plot_pnl(daily_summary_df, title=f"{SPOT_SYMBOL} New Delta Hedge Strategy")