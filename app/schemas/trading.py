from pydantic import BaseModel
from typing import Optional, Literal, List, Dict, Any
from datetime import datetime, date

class TradeRequest(BaseModel):
    underlying_instrument: str  # NIFTY, BANKNIFTY, etc.
    option_type: Optional[Literal["CALL", "PUT"]] = None
    strike_price: Optional[float] = None
    expiry_date: Optional[date] = None
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: Optional[float] = None  # None for market orders
    stop_loss_price: Optional[float] = None

class TradeResponse(BaseModel):
    id: int
    order_id: str
    status: str
    message: str
    
class PositionResponse(BaseModel):
    id: int
    underlying_instrument: str
    option_type: Optional[str] = None
    strike_price: Optional[float] = None
    expiry_date: Optional[date] = None
    quantity: int
    average_price: float
    current_price: Optional[float] = None
    unrealized_pnl: float
    stop_loss_price: Optional[float] = None
    stop_loss_active: bool
    
    class Config:
        from_attributes = True

class MarketDataRequest(BaseModel):
    instruments: list[str]  # List of instrument symbols

class MarketDataResponse(BaseModel):
    symbol: str
    ltp: float
    change: float
    change_percent: float
    timestamp: datetime

# New schemas for simplified stock trading
class StockSearchResult(BaseModel):
    symbol: str
    name: str
    exchange_segment: int
    instrument_id: int
    series: str
    isin: str
    lot_size: int
    tick_size: float
    current_price: Optional[float] = None
    market_data: Dict[str, Any] = {}

class StockSearchResponse(BaseModel):
    type: str
    query: str
    total_found: int
    returned: int
    results: List[StockSearchResult]
    message: str

class BuyStockRequest(BaseModel):
    stock_symbol: str
    quantity: int
    price: Optional[float] = None  # None for market orders
    order_type: Literal["BUY", "SELL"] = "BUY"

class BuyStockResponse(BaseModel):
    status: str
    message: str
    order_id: str
    trade_id: int
    stock_info: Dict[str, Any]
    order_details: Dict[str, Any]
    timestamp: str

class StockQuoteResponse(BaseModel):
    type: str
    stock_info: Dict[str, Any]
    market_data: Dict[str, Any]
    timestamp: str

class EnhancedOrderBookResponse(BaseModel):
    type: str
    result: List[Dict[str, Any]]
    enhanced: bool
    message: str
