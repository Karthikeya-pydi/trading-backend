from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
from loguru import logger

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.stock_returns_service import StockReturnsService
from app.schemas.returns import (
    StockReturnsResponse, 
    StockReturnsListResponse, 
    StockReturnsSummaryResponse,
    StockReturnsErrorResponse
)

router = APIRouter()

@router.get("/stock/{symbol}", response_model=StockReturnsResponse)
async def get_stock_returns(
    symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get stock returns data for a specific stock symbol
    Returns performance data across different time periods
    """
    try:
        returns_service = StockReturnsService()
        result = returns_service.get_stock_returns(symbol)
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result.get("message", "Stock returns data not found")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch stock returns: {str(e)}"
        )

@router.get("/all", response_model=StockReturnsListResponse)
async def get_all_stock_returns(
    limit: Optional[int] = Query(None, description="Maximum number of records to return"),
    sort_by: str = Query("1_Year", description="Column to sort by (1_Week, 1_Month, 3_Months, 6_Months, 9_Months, 1_Year, 3_Years, 5_Years, turnover, raw_score, normalized_score)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get all stock returns data with optional filtering and sorting
    Useful for creating leaderboards and performance tables
    """
    try:
        returns_service = StockReturnsService()
        result = returns_service.get_all_returns(limit, sort_by, sort_order)
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch stock returns")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch all stock returns: {str(e)}"
        )

@router.get("/summary", response_model=StockReturnsSummaryResponse)
async def get_returns_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get summary statistics of stock returns data
    Includes mean, median, min, max, and top/bottom performers
    """
    try:
        returns_service = StockReturnsService()
        result = returns_service.get_returns_summary()
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch returns summary")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch returns summary: {str(e)}"
        )

@router.get("/search")
async def search_stock_symbols(
    query: str = Query(..., description="Search query for stock symbols"),
    limit: int = Query(20, description="Maximum number of results to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Search for stock symbols by partial match
    Useful for autocomplete and search functionality
    """
    try:
        returns_service = StockReturnsService()
        result = returns_service.search_symbols(query, limit)
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to search symbols")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search symbols: {str(e)}"
        )

@router.post("/refresh")
async def refresh_returns_data(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Refresh stock returns data by reloading from the CSV file
    Useful after running returnsCalculation.py to update the data
    """
    try:
        returns_service = StockReturnsService()
        result = returns_service.refresh_data()
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to refresh data")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to refresh returns data: {str(e)}"
        )

@router.get("/top-performers")
async def get_top_performers(
    period: str = Query("1_Year", description="Time period for performance (1_Week, 1_Month, 3_Months, 6_Months, 1_Year, 3_Years, 5_Years)"),
    limit: int = Query(10, description="Number of top performers to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get top performing stocks for a specific time period
    Useful for creating performance leaderboards
    """
    try:
        returns_service = StockReturnsService()
        
        # Get all returns sorted by the specified period
        result = returns_service.get_all_returns(limit, period, "desc")
        
        if result.get("status") == "success":
            return {
                "status": "success",
                "period": period,
                "top_performers": result["data"],
                "count": len(result["data"]),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch top performers")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch top performers: {str(e)}"
        )

@router.get("/bottom-performers")
async def get_bottom_performers(
    period: str = Query("1_Year", description="Time period for performance (1_Week, 1_Month, 3_Months, 6_Months, 1_Year, 3_Years, 5_Years)"),
    limit: int = Query(10, description="Number of bottom performers to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get bottom performing stocks for a specific time period
    Useful for identifying underperforming stocks
    """
    try:
        returns_service = StockReturnsService()
        
        # Get all returns sorted by the specified period in ascending order
        result = returns_service.get_all_returns(limit, period, "asc")
        
        if result.get("status") == "success":
            return {
                "status": "success",
                "period": period,
                "bottom_performers": result["data"],
                "count": len(result["data"]),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch bottom performers")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch bottom performers: {str(e)}"
        )
