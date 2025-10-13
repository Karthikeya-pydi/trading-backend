"""
Schemas Module

This module contains all Pydantic schemas for request/response validation.
"""

# Auth schemas
from .auth import Token, UserProfile

# User schemas
from .user import UserCreate, UserUpdate, User, IIFLMarketCredentials, IIFLInteractiveCredentials, IIFLCredentials

# Trading schemas
from .trading import (
    TradeRequest,
    TradeResponse,
    PositionResponse,
    MarketDataRequest,
    MarketDataResponse,
    StockSearchResult,
    StockSearchResponse,
    BuyStockRequest,
    BuyStockResponse,
    StockQuoteResponse,
    EnhancedOrderBookResponse,
)

# Portfolio schemas
from .portfolio import (
    PortfolioPosition,
    PortfolioSummary,
    PnLData,
    DailyPnL,
    RiskMetrics,
)

# Returns schemas
from .returns import (
    StockReturnsData,
    StockReturnsResponse,
    StockReturnsListResponse,
    ReturnsFileInfo,
    ReturnsFilesListResponse,
    ReturnsFileDataResponse,
)

# Bhavcopy schemas
from .bhavcopy import (
    BhavcopyFilesListResponse,
    BhavcopyFileDataResponse,
    BhavcopyStockData,
)

# Stock Analysis schemas
from .stock_analysis import (
    DescriptiveStats,
    GlobalAnalysis,
    RollingAnalysis,
    PerStockAnalysis,
    StockAnalysisSummary,
    StockAnalysisDetailed,
    AnalysisSummary,
    StockAnalysisResponse,
    SingleStockAnalysisResponse,
)

__all__ = [
    # Auth
    "Token",
    "UserProfile",
    
    # User
    "UserCreate",
    "UserUpdate",
    "User",
    "IIFLMarketCredentials",
    "IIFLInteractiveCredentials",
    "IIFLCredentials",
    
    # Trading
    "TradeRequest",
    "TradeResponse",
    "PositionResponse",
    "MarketDataRequest",
    "MarketDataResponse",
    "StockSearchResult",
    "StockSearchResponse",
    "BuyStockRequest",
    "BuyStockResponse",
    "StockQuoteResponse",
    "EnhancedOrderBookResponse",
    
    # Portfolio
    "PortfolioPosition",
    "PortfolioSummary",
    "PnLData",
    "DailyPnL",
    "RiskMetrics",
    
    # Returns
    "StockReturnsData",
    "StockReturnsResponse",
    "StockReturnsListResponse",
    "ReturnsFileInfo",
    "ReturnsFilesListResponse",
    "ReturnsFileDataResponse",
    
    # Bhavcopy
    "BhavcopyFilesListResponse",
    "BhavcopyFileDataResponse",
    "BhavcopyStockData",
    
    # Stock Analysis
    "DescriptiveStats",
    "GlobalAnalysis",
    "RollingAnalysis",
    "PerStockAnalysis",
    "StockAnalysisSummary",
    "StockAnalysisDetailed",
    "AnalysisSummary",
    "StockAnalysisResponse",
    "SingleStockAnalysisResponse",
]

