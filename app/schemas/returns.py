from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class StockReturnsData(BaseModel):
    """Schema for individual stock returns data"""
    symbol: str
    fincode: str
    isin: str
    latest_date: datetime
    latest_close: float
    latest_volume: int
    turnover: Optional[float] = None
    returns_1_week: Optional[float] = None
    returns_1_month: Optional[float] = None
    returns_3_months: Optional[float] = None
    returns_6_months: Optional[float] = None
    returns_9_months: Optional[float] = None
    returns_1_year: Optional[float] = None
    returns_3_years: Optional[float] = None
    returns_5_years: Optional[float] = None
    raw_score: Optional[float] = None
    normalized_score: Optional[float] = None

class StockReturnsResponse(BaseModel):
    """Schema for stock returns data response"""
    status: str
    symbol: str
    data: StockReturnsData
    source_file: str
    timestamp: str

class StockReturnsListResponse(BaseModel):
    """Schema for list of stock returns response"""
    status: str
    data: List[StockReturnsData]
    total_count: int
    source_file: str
    timestamp: str

class StockReturnsSummaryResponse(BaseModel):
    """Schema for stock returns summary response"""
    status: str
    summary: dict
    total_symbols: int
    source_file: str
    timestamp: str

class ReturnsFileInfo(BaseModel):
    """Schema for returns file information"""
    filename: str
    s3_key: str
    size_mb: float
    last_modified: str
    source: str

class ReturnsFilesListResponse(BaseModel):
    """Schema for returns files list response"""
    message: str
    files: List[ReturnsFileInfo]
    total_files: int
    source: str
    timestamp: str

class ReturnsFileDataResponse(BaseModel):
    """Schema for returns file data response"""
    status: str
    message: str
    data: List[StockReturnsData]
    total_count: int
    source_file: str
    file_size_mb: float
    last_modified: str
    source: str
    timestamp: str

class StockReturnsErrorResponse(BaseModel):
    """Schema for stock returns error responses"""
    status: str
    message: str
    symbol: Optional[str] = None
