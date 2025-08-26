from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.models.stock_screening import StockScreening
from app.schemas.stock_screening import (
    StockScreeningRequest, 
    StockSearchRequest,
    StockScreeningResponse, 
    StockScreeningListResponse,
    ScrapingStatusResponse
)
from app.services.stock_screening_service import StockScreeningService

router = APIRouter()
screening_service = StockScreeningService()

@router.post("/search", response_model=StockScreeningListResponse)
async def search_stocks(
    request: StockSearchRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """ONE ENDPOINT: Search existing stocks OR scrape immediately if not found"""
    try:
        # First, search existing database
        existing_stocks = screening_service.search_stocks(request.query)
        
        # If stocks found, return them
        if existing_stocks:
            stock_responses = []
            for stock in existing_stocks:
                stock_response = StockScreeningResponse.from_orm(stock)
                stock_responses.append(stock_response)
            
            return StockScreeningListResponse(
                stocks=stock_responses,
                total_count=len(stock_responses),
                message=f"Found {len(stock_responses)} existing stocks"
            )
        
        # If no stocks found, SCRAPE IMMEDIATELY (not in background)
        stock_symbol = request.query.upper()
        
        # Scrape the data immediately
        scraped_data = screening_service.scrape_stock_data(stock_symbol)
        
        if "error" in scraped_data:
            # Scraping failed
            return StockScreeningListResponse(
                stocks=[],
                total_count=0,
                message=f"Failed to scrape {stock_symbol}: {scraped_data['error']}"
            )
        
        # Save to database
        saved_stock = screening_service.save_to_database(stock_symbol, scraped_data)
        
        # Convert to response and return
        stock_response = StockScreeningResponse.from_orm(saved_stock)
        
        return StockScreeningListResponse(
            stocks=[stock_response],
            total_count=1,
            message=f"Successfully scraped and retrieved data for {stock_symbol}"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search/scrape stocks: {str(e)}"
        )

@router.post("/scrape", response_model=ScrapingStatusResponse)
async def scrape_stock_data(
    request: StockScreeningRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Scrape stock data from screener.in (runs in background)"""
    try:
        # Check if stock already exists and was recently scraped
        existing_stock = screening_service.get_stock_data(request.stock_symbol)
        if existing_stock and existing_stock.scraping_status == "success":
            return ScrapingStatusResponse(
                stock_symbol=request.stock_symbol,
                status="already_exists",
                message="Stock data already exists and is up to date",
                last_scraped_at=existing_stock.last_scraped_at
            )
        
        # Scrape immediately (not in background)
        scraped_data = screening_service.scrape_stock_data(request.stock_symbol, request.stock_name)
        
        if "error" in scraped_data:
            return ScrapingStatusResponse(
                stock_symbol=request.stock_symbol,
                status="failed",
                message=f"Failed to scrape: {scraped_data['error']}",
                last_scraped_at=None
            )
        
        # Save to database
        saved_stock = screening_service.save_to_database(request.stock_symbol, scraped_data, request.stock_name)
        
        return ScrapingStatusResponse(
            stock_symbol=request.stock_symbol,
            status="success",
            message="Stock data scraped and saved successfully",
            last_scraped_at=saved_stock.last_scraped_at
        )
        
        # This will never be reached now, but keeping for safety
        pass
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start scraping: {str(e)}"
        )

# Background task function removed - now scraping happens immediately

@router.get("/{stock_symbol}", response_model=StockScreeningResponse)
async def get_stock_data(
    stock_symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get stock data by symbol"""
    try:
        stock = screening_service.get_stock_data(stock_symbol)
        
        if not stock:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock {stock_symbol} not found"
            )
        
        return StockScreeningResponse.from_orm(stock)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stock data: {str(e)}"
        )

@router.get("/{stock_symbol}/status", response_model=ScrapingStatusResponse)
async def get_stock_status(
    stock_symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get scraping status for a stock"""
    try:
        stock = screening_service.get_stock_data(stock_symbol)
        if not stock:
            return ScrapingStatusResponse(
                stock_symbol=stock_symbol,
                status="not_found",
                message="Stock not found in database. Use POST /search to start scraping.",
                last_scraped_at=None
            )
        
        return ScrapingStatusResponse(
            stock_symbol=stock_symbol,
            status=stock.scraping_status,
            message=f"Stock status: {stock.scraping_status}",
            last_scraped_at=stock.last_scraped_at
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stock status: {str(e)}"
        )

@router.get("/", response_model=StockScreeningListResponse)
async def get_all_stocks(
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all stocks with pagination"""
    try:
        stocks = screening_service.get_all_stocks(skip=skip, limit=limit)
        
        # Convert to response models
        stock_responses = []
        for stock in stocks:
            stock_response = StockScreeningResponse.from_orm(stock)
            stock_responses.append(stock_response)
        
        return StockScreeningListResponse(
            stocks=stock_responses,
            total_count=len(stock_responses),
            message=f"Retrieved {len(stock_responses)} stocks"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stocks: {str(e)}"
        )

@router.delete("/{stock_symbol}")
async def delete_stock_data(
    stock_symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete stock data"""
    try:
        stock = db.query(StockScreening).filter(
            StockScreening.stock_symbol == stock_symbol
        ).first()
        
        if not stock:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock {stock_symbol} not found"
            )
        
        db.delete(stock)
        db.commit()
        
        return {"message": f"Stock {stock_symbol} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete stock: {str(e)}"
        )

@router.post("/{stock_symbol}/refresh")
async def refresh_stock_data(
    stock_symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Refresh stock data by re-scraping"""
    try:
        # Get existing stock to get the name
        existing_stock = screening_service.get_stock_data(stock_symbol)
        stock_name = existing_stock.stock_name if existing_stock else None
        
        # Re-scrape immediately (not in background)
        scraped_data = screening_service.scrape_stock_data(stock_symbol, stock_name)
        
        if "error" in scraped_data:
            return ScrapingStatusResponse(
                stock_symbol=stock_symbol,
                status="failed",
                message=f"Failed to refresh: {scraped_data['error']}",
                last_scraped_at=existing_stock.last_scraped_at if existing_stock else None
            )
        
        # Save to database
        saved_stock = screening_service.save_to_database(stock_symbol, scraped_data, stock_name)
        
        return ScrapingStatusResponse(
            stock_symbol=stock_symbol,
            status="success",
            message="Stock data refreshed successfully",
            last_scraped_at=saved_stock.last_scraped_at
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start refresh: {str(e)}"
        )
