from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime

class StockScreeningRequest(BaseModel):
    stock_symbol: str
    stock_name: Optional[str] = None

class StockSearchRequest(BaseModel):
    query: str

class StockScreeningResponse(BaseModel):
    id: int
    stock_symbol: str
    stock_name: Optional[str]
    company_url: Optional[str]
    
    # Financial Data
    quarters_data: Optional[Dict[str, Any]]
    peers_data: Optional[Dict[str, Any]]
    profit_loss_data: Optional[Dict[str, Any]]
    balance_sheet_data: Optional[Dict[str, Any]]
    ratios_data: Optional[Dict[str, Any]]
    cash_flow_data: Optional[Dict[str, Any]]
    shareholding_data: Optional[Dict[str, Any]]
    
    # Additional data sections
    overview_data: Optional[Dict[str, Any]]
    technical_data: Optional[Dict[str, Any]]
    valuation_data: Optional[Dict[str, Any]]
    growth_data: Optional[Dict[str, Any]]
    industry_data: Optional[Dict[str, Any]]
    
    # Metadata
    last_scraped_at: datetime
    scraping_status: str
    error_message: Optional[str]
    
    # File paths
    html_files: Optional[Dict[str, str]]
    pdf_files: Optional[Dict[str, str]]
    
    created_at: datetime
    updated_at: Optional[datetime]
    
    class Config:
        from_attributes = True

class StockScreeningListResponse(BaseModel):
    stocks: List[StockScreeningResponse]
    total_count: int
    message: Optional[str] = None

class ScrapingStatusResponse(BaseModel):
    stock_symbol: str
    status: str
    message: str
    last_scraped_at: Optional[datetime]
