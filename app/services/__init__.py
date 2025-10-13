"""
Services Module

This module contains all business logic services for the trading platform.
"""

# Service classes
from .auth_service import AuthService
from .bhavcopy_service import BhavcopyService
from .holdings_market_data import HoldingsMarketDataService
from .iifl_service import IIFLService, get_iifl_service
from .instrument_service import InstrumentService, InstrumentMappingService, get_instrument_service
from .market_analytics_service import MarketAnalyticsService
from .nifty_service import NiftyService
from .notification_service import NotificationService, notification_service
from .optimized_h5_service import OptimizedH5Service
from .portfolio_service import PortfolioService, get_portfolio_service
from .realtime_market_service import RealtimeMarketService
from .realtime_service import RealtimeService, realtime_service
from .s3_service import S3Service
from .s3_stock_analysis_service import S3StockAnalysisService
from .stock_returns_service import StockReturnsService
from .strategy_service import StrategyService, get_strategy_service

# IIFL Connect
from .iifl_connect import IIFLConnect

__all__ = [
    # Auth
    "AuthService",
    
    # IIFL Services
    "IIFLService",
    "get_iifl_service",
    "IIFLConnect",
    
    # Portfolio & Trading
    "PortfolioService",
    "get_portfolio_service",
    "StrategyService",
    "get_strategy_service",
    
    # Market Data & Analytics
    "MarketAnalyticsService",
    "RealtimeMarketService",
    "HoldingsMarketDataService",
    "NiftyService",
    "BhavcopyService",
    
    # Instruments
    "InstrumentService",
    "InstrumentMappingService",
    "get_instrument_service",
    
    # Data Services
    "OptimizedH5Service",
    "S3Service",
    "S3StockAnalysisService",
    "StockReturnsService",
    
    # Real-time & Notifications (singletons)
    "RealtimeService",
    "realtime_service",
    "NotificationService",
    "notification_service",
]

