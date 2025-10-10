# opstrat_backtester package init
from . import data_loader
from .api_client import OplabClient
from .core.engine import Backtester
from .core.portfolio import Portfolio
from .core.strategy import Strategy
from .analytics import plots, stats

__version__ = "0.1.0"

__all__ = [
    "DataLoader",
    "OplabClient",
    "BacktestEngine",
    "Portfolio",
    "Strategy",
    "plots",
    "stats",
]