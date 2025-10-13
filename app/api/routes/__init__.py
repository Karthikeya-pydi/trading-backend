"""
API Routes Module

This module contains all API route handlers organized by domain.
"""

from . import auth
from . import users
from . import trading
from . import market_data
from . import websocket
from . import iifl
from . import portfolio
from . import returns
from . import stock_analysis

__all__ = [
    "auth",
    "users",
    "trading",
    "market_data",
    "websocket",
    "iifl",
    "portfolio",
    "returns",
    "stock_analysis",
]

