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
    
    # Historical Raw Scores (from scripts/calculate_returns.py)
    raw_score_1_week_ago: Optional[float] = None
    raw_score_1_month_ago: Optional[float] = None
    raw_score_3_months_ago: Optional[float] = None
    raw_score_6_months_ago: Optional[float] = None
    raw_score_9_months_ago: Optional[float] = None
    raw_score_1_year_ago: Optional[float] = None
    
    # Percentage Changes in Scores
    score_change_1_week: Optional[float] = None
    score_change_1_month: Optional[float] = None
    score_change_3_months: Optional[float] = None
    score_change_6_months: Optional[float] = None
    score_change_9_months: Optional[float] = None
    score_change_1_year: Optional[float] = None
    
    # Sign Pattern Comparisons
    sign_pattern_1_week: Optional[str] = None
    sign_pattern_1_month: Optional[str] = None
    sign_pattern_3_months: Optional[str] = None
    sign_pattern_6_months: Optional[str] = None
    sign_pattern_9_months: Optional[str] = None
    sign_pattern_1_year: Optional[str] = None
    
    # Additional Company Information
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap_crore: Optional[float] = None
    roe_percent: Optional[float] = None
    roce_percent: Optional[float] = None

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
