import pandas as pd
from opstrat_backtester.core.portfolio import Portfolio

def test_portfolio_initialization():
    """Tests that the portfolio initializes with the correct cash."""
    portfolio = Portfolio(initial_cash=50000)
    assert portfolio.cash == 50000
    assert portfolio.get_positions() == {}

def test_add_buy_trade():
    """Tests adding a simple buy trade."""
    portfolio = Portfolio(initial_cash=10000)
    trade_date = pd.Timestamp('2023-01-02')
    
    portfolio.add_trade(trade_date, 'AAPL', 10, 150.0)
    
    assert portfolio.cash == 10000 - (10 * 150.0)
    position = portfolio.get_positions()['AAPL']
    assert position['quantity'] == 10
    assert position['cost_basis'] == 150.0

def test_cost_basis_averages_up():
    """Tests that cost basis is correctly averaged on subsequent buys."""
    portfolio = Portfolio(initial_cash=20000)
    trade_date = pd.Timestamp('2023-01-02')
    
    portfolio.add_trade(trade_date, 'AAPL', 10, 150.0) # Cost: 1500
    portfolio.add_trade(trade_date, 'AAPL', 10, 160.0) # Cost: 1600
    
    # Total cost = 1500 + 1600 = 3100. Total quantity = 20.
    # Expected cost basis = 3100 / 20 = 155.0
    position = portfolio.get_positions()['AAPL']
    assert position['quantity'] == 20
    assert position['cost_basis'] == 155.0

def test_sell_trade_does_not_change_cost_basis():
    """Tests that selling a partial position does not alter the cost basis."""
    portfolio = Portfolio(initial_cash=10000)
    trade_date = pd.Timestamp('2023-01-02')
    
    portfolio.add_trade(trade_date, 'AAPL', 20, 150.0)
    portfolio.add_trade(trade_date, 'AAPL', -5, 160.0) # Sell 5 shares
    
    assert portfolio.cash == 10000 - (20 * 150.0) + (5 * 160.0)
    position = portfolio.get_positions()['AAPL']
    assert position['quantity'] == 15
    assert position['cost_basis'] == 150.0 # Should remain unchanged

def test_closing_position():
    """Tests that a position is removed when fully closed."""
    portfolio = Portfolio(initial_cash=10000)
    trade_date = pd.Timestamp('2023-01-02')
    
    portfolio.add_trade(trade_date, 'AAPL', 10, 150.0)
    portfolio.add_trade(trade_date, 'AAPL', -10, 160.0)
    
    assert 'AAPL' not in portfolio.get_positions()

def test_mark_to_market():
    """Tests the daily mark-to-market valuation."""
    portfolio = Portfolio(initial_cash=1000)
    trade_date = pd.Timestamp('2023-01-02')
    portfolio.add_trade(trade_date, 'TEST', 10, 100.0) # Cash is now 0
    
    market_data = pd.DataFrame([{'ticker': 'TEST', 'close': 110.0}])
    portfolio.mark_to_market(trade_date, market_data)
    
    # Expected portfolio value = cash (0) + market value (10 * 110) = 1100
    assert portfolio.history[-1]['portfolio_value'] == 1100