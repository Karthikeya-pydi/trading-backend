from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class BhavcopyStockData(BaseModel):
    """Schema for individual stock bhavcopy data"""
    symbol: str
    series: str
    date: str
    prev_close: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    last_price: Optional[float] = None
    close_price: Optional[float] = None
    avg_price: Optional[float] = None
    total_traded_qty: Optional[int] = None
    turnover_lacs: Optional[float] = None
    no_of_trades: Optional[int] = None
    delivery_qty: Optional[int] = None
    delivery_percentage: Optional[float] = None

class BhavcopyStockResponse(BaseModel):
    """Schema for bhavcopy stock data response"""
    status: str
    symbol: str
    data: List[BhavcopyStockData]
    count: int
    source_file: str
    timestamp: str

class BhavcopySymbolsResponse(BaseModel):
    """Schema for available symbols response"""
    status: str
    symbols: List[str]
    count: int
    source_file: str
    timestamp: str

class BhavcopyFileInfo(BaseModel):
    """Schema for bhavcopy file information"""
    filename: str
    size_mb: float
    modified: str
    path: str

class BhavcopySummaryResponse(BaseModel):
    """Schema for bhavcopy summary response"""
    status: str
    files: List[BhavcopyFileInfo]
    total_files: int
    timestamp: str

class BhavcopyErrorResponse(BaseModel):
    """Schema for bhavcopy error responses"""
    status: str
    message: str
    symbol: Optional[str] = None
