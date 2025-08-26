from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Float, Boolean
from sqlalchemy.sql import func
from app.core.database import Base
from datetime import datetime, timezone

class StockScreening(Base):
    __tablename__ = "stock_screenings"
    
    id = Column(Integer, primary_key=True, index=True)
    stock_symbol = Column(String, nullable=False, index=True)
    stock_name = Column(String, nullable=True)
    company_url = Column(String, nullable=True)
    
    # Financial Data
    quarters_data = Column(JSON, nullable=True)  # Quarterly results
    peers_data = Column(JSON, nullable=True)     # Peer comparison
    profit_loss_data = Column(JSON, nullable=True)  # P&L statements
    balance_sheet_data = Column(JSON, nullable=True)  # Balance sheets
    ratios_data = Column(JSON, nullable=True)    # Financial ratios
    cash_flow_data = Column(JSON, nullable=True)  # Cash flow statements
    shareholding_data = Column(JSON, nullable=True)  # Shareholding patterns
    
    # Additional data sections
    overview_data = Column(JSON, nullable=True)  # Company overview
    technical_data = Column(JSON, nullable=True)  # Technical indicators
    valuation_data = Column(JSON, nullable=True)  # Valuation metrics
    growth_data = Column(JSON, nullable=True)    # Growth metrics
    industry_data = Column(JSON, nullable=True)  # Industry comparison
    
    # Metadata
    last_scraped_at = Column(DateTime, server_default=func.now())
    scraping_status = Column(String, default="pending")  # pending, success, failed
    error_message = Column(Text, nullable=True)
    
    # HTML and PDF file paths (optional)
    html_files = Column(JSON, nullable=True)  # Store paths to HTML files
    pdf_files = Column(JSON, nullable=True)   # Store paths to PDF files
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
