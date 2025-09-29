from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
from loguru import logger
import pandas as pd

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.stock_returns_service import StockReturnsService
from app.schemas.returns import (
    StockReturnsResponse, 
    StockReturnsListResponse, 
    StockReturnsSummaryResponse,
    StockReturnsErrorResponse,
    ReturnsFilesListResponse,
    ReturnsFileDataResponse
)

router = APIRouter()


@router.get("/files", response_model=ReturnsFilesListResponse)
async def get_returns_files(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get list of all available returns files from S3
    """
    try:
        returns_service = StockReturnsService()
        result = returns_service.get_available_files()
        
        if result.get("status") != "success":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch returns files")
            )
        
        return {
            "message": "Returns files retrieved successfully from S3",
            "files": result.get("files", []),
            "total_files": result.get("total_files", 0),
            "source": "S3",
            "timestamp": result.get("timestamp")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch returns files: {str(e)}"
        )

@router.get("/file/{filename}", response_model=ReturnsFileDataResponse)
async def get_returns_file_data(
    filename: str,
    limit: Optional[int] = Query(None, description="Maximum number of records to return"),
    sort_by: str = Query("1_Year", description="Column to sort by (1_Week, 1_Month, 3_Months, 6_Months, 9_Months, 1_Year, 3_Years, 5_Years, turnover, raw_score, normalized_score)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get returns data from a specific file
    """
    try:
        from app.services.s3_service import S3Service
        
        s3_service = S3Service()
        
        # Get all returns files to find the specific one
        summary = s3_service.get_adjusted_eq_summary()
        if summary.get('status') != 'success':
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch returns files list"
            )
        
        # Find the specific file
        target_file = None
        for file_info in summary.get('files', []):
            if file_info['filename'] == filename:
                target_file = file_info
                break
        
        if not target_file:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Returns file '{filename}' not found"
            )
        
        # Get data from S3
        df = s3_service.get_adjusted_eq_data(target_file['s3_key'])
        if df is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to load returns data from S3"
            )
        
        # Convert date columns
        df['Latest_Date'] = pd.to_datetime(df['Latest_Date'])
        
        # Create a copy for processing
        processed_data = df.copy()
        
        # Sort the data
        if sort_by in processed_data.columns:
            processed_data = processed_data.sort_values(
                by=sort_by, 
                ascending=(sort_order == 'asc'),
                na_position='last'
            )
        
        # Apply limit if specified
        if limit:
            processed_data = processed_data.head(limit)
        
        # Convert to list of dictionaries
        records = []
        for _, row in processed_data.iterrows():
            record = {
                "symbol": row['Symbol'],
                "fincode": str(row['Fincode']),
                "isin": row['ISIN'],
                "latest_date": row['Latest_Date'].isoformat() if pd.notna(row['Latest_Date']) else None,
                "latest_close": float(row['Latest_Close']) if pd.notna(row['Latest_Close']) else None,
                "latest_volume": int(row['Latest_Volume']) if pd.notna(row['Latest_Volume']) else None,
                "turnover": float(row['Turnover']) if pd.notna(row['Turnover']) else None,
                "returns_1_week": float(row['1_Week']) if pd.notna(row['1_Week']) else None,
                "returns_1_month": float(row['1_Month']) if pd.notna(row['1_Month']) else None,
                "returns_3_months": float(row['3_Months']) if pd.notna(row['3_Months']) else None,
                "returns_6_months": float(row['6_Months']) if pd.notna(row['6_Months']) else None,
                "returns_9_months": float(row['9_Months']) if pd.notna(row['9_Months']) else None,
                "returns_1_year": float(row['1_Year']) if pd.notna(row['1_Year']) else None,
                "returns_3_years": float(row['3_Years']) if pd.notna(row['3_Years']) else None,
                "returns_5_years": float(row['5_Years']) if pd.notna(row['5_Years']) else None,
                "raw_score": float(row['Raw_Score']) if pd.notna(row['Raw_Score']) else None,
                "normalized_score": float(row['Normalized_Score']) if pd.notna(row['Normalized_Score']) else None
            }
            records.append(record)
        
        return {
            "status": "success",
            "message": f"Returns data retrieved successfully from {filename}",
            "data": records,
            "total_count": len(records),
            "source_file": filename,
            "file_size_mb": target_file['size_mb'],
            "last_modified": target_file['last_modified'],
            "source": "S3",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch returns file data: {str(e)}"
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





