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





