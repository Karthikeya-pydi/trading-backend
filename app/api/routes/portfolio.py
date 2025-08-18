from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
from loguru import logger

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.portfolio_service import PortfolioService
from app.services.iifl_service_fixed import IIFLServiceFixed
from app.services.holdings_market_data import HoldingsMarketDataService
from app.services.bhavcopy_service import BhavcopyService
from app.services.stock_returns_service import StockReturnsService
from app.schemas.bhavcopy import (
    BhavcopyStockResponse, 
    BhavcopySymbolsResponse, 
    BhavcopySummaryResponse,
    BhavcopyErrorResponse
)

router = APIRouter()

@router.get("/summary")
async def get_portfolio_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get comprehensive portfolio summary"""
    portfolio_service = PortfolioService(db)
    
    try:
        summary = portfolio_service.get_portfolio_summary(current_user.id)
        return summary
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch portfolio summary: {str(e)}"
        )

@router.get("/pnl")
async def get_pnl(
    start_date: Optional[datetime] = Query(None, description="Start date for P&L calculation"),
    end_date: Optional[datetime] = Query(None, description="End date for P&L calculation"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get P&L calculation for specified date range"""
    portfolio_service = PortfolioService(db)
    
    try:
        pnl_data = portfolio_service.calculate_pnl(current_user.id, start_date, end_date)
        return pnl_data
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate P&L: {str(e)}"
        )

@router.post("/update-prices")
async def update_position_prices(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update current prices for all positions"""
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market credentials not configured"
        )
    
    portfolio_service = PortfolioService(db)
    
    try:
        result = await portfolio_service.update_position_prices(current_user.id)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update position prices: {str(e)}"
        )

@router.get("/risk-metrics")
async def get_risk_metrics(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get portfolio risk metrics"""
    portfolio_service = PortfolioService(db)
    
    try:
        risk_metrics = portfolio_service.get_risk_metrics(current_user.id)
        return risk_metrics
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate risk metrics: {str(e)}"
        )

@router.get("/daily-pnl")
async def get_daily_pnl(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get today's P&L"""
    portfolio_service = PortfolioService(db)
    
    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        pnl_data = portfolio_service.calculate_pnl(current_user.id, today)
        return {
            "date": today.date(),
            "daily_pnl": pnl_data["total_pnl"],
            "realized_pnl": pnl_data["total_realized_pnl"],
            "unrealized_pnl": pnl_data["total_unrealized_pnl"],
            "trades_count": pnl_data["total_trades"],
            "win_rate": pnl_data["win_rate"]
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate daily P&L: {str(e)}"
        )

@router.get("/holdings")
async def get_holdings(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user's long-term holdings from IIFL"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Use the fixed IIFL service
        iifl_service = IIFLServiceFixed(db)
        holdings_result = iifl_service.get_holdings(db, current_user.id)
        
        return {
            "status": "success",
            "holdings": holdings_result,
            "message": f"Retrieved holdings for user {current_user.email}"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch holdings: {str(e)}"
        )

@router.get("/holdings-summary")
async def get_holdings_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user-friendly holdings summary from IIFL with live prices"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market credentials not configured"
        )
    
    try:
        # Use the new holdings market data service
        service = HoldingsMarketDataService(current_user, db)
        result = service.get_holdings_with_current_prices()
        
        if result.get("status") == "success":
            # Format the response to match the existing structure
            holdings = result.get("holdings", [])
            summary = result.get("summary", {})
            
            # Format holdings for the response
            formatted_holdings = []
            for holding in holdings:
                formatted_holding = {
                    "stock_name": holding.get("stock_name"),
                    "isin": holding.get("isin"),
                    "quantity": holding.get("quantity"),
                    "average_price": holding.get("avg_price"),
                    "current_price": holding.get("current_price"),
                    "investment_value": holding.get("invested_value"),
                    "current_value": holding.get("market_value"),
                    "unrealized_pnl": holding.get("pnl"),
                    "unrealized_pnl_percent": holding.get("pnl_percent"),
                    "type": holding.get("type"),
                    "purchase_date": None,  # Not available in the new service
                    "is_collateral": holding.get("type") == "Collateral",
                    "nse_instrument_id": holding.get("nse_instrument_id")
                }
                formatted_holdings.append(formatted_holding)
            
            return {
                "status": "success",
                "summary": {
                    "total_holdings": summary.get("total_holdings", 0),
                    "total_investment": summary.get("total_investment", 0),
                    "total_current_value": summary.get("total_current_value", 0),
                    "unrealized_pnl": summary.get("total_pnl", 0),
                    "unrealized_pnl_percent": summary.get("total_pnl_percent", 0),
                    "holdings": formatted_holdings
                },
                "message": f"Holdings summary with live prices for {current_user.email}",
                "market_data_timestamp": result.get("timestamp")
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("error", "Failed to fetch holdings with current prices")
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch holdings summary: {str(e)}"
        ) 

@router.get("/holdings-with-prices")
async def get_holdings_with_current_prices(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get holdings with current market prices and calculate P&L
    P&L = (current_price - avg_price) * quantity
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market credentials not configured"
        )
    
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        service = HoldingsMarketDataService(current_user, db)
        result = service.get_holdings_with_current_prices()
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("error", "Failed to fetch holdings with current prices")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch holdings with current prices: {str(e)}"
        ) 

# Bhavcopy Data Endpoints
@router.get("/bhavcopy/stock/{symbol}", response_model=BhavcopyStockResponse)
async def get_stock_bhavcopy_data(
    symbol: str,
    date: Optional[str] = Query(None, description="Date filter (format: DD-MMM-YYYY)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get BSE bhavcopy data for a specific stock symbol
    This endpoint can be used to show detailed market data for each holding
    """
    try:
        bhavcopy_service = BhavcopyService()
        result = bhavcopy_service.get_stock_bhavcopy_data(symbol, date)
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result.get("message", "Stock data not found")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch bhavcopy data: {str(e)}"
        )

@router.get("/bhavcopy/symbols", response_model=BhavcopySymbolsResponse)
async def get_available_symbols(
    limit: int = Query(100, description="Maximum number of symbols to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get list of available symbols from bhavcopy data
    Useful for autocomplete/search functionality
    """
    try:
        bhavcopy_service = BhavcopyService()
        result = bhavcopy_service.get_available_symbols(limit)
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch symbols")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch available symbols: {str(e)}"
        )

@router.get("/bhavcopy/summary", response_model=BhavcopySummaryResponse)
async def get_bhavcopy_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get summary of available bhavcopy data files
    Useful for monitoring data availability
    """
    try:
        bhavcopy_service = BhavcopyService()
        result = bhavcopy_service.get_bhavcopy_summary()
        
        if result.get("status") == "success":
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to fetch bhavcopy summary")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch bhavcopy summary: {str(e)}"
        )

# Stock Returns Data Endpoints
@router.get("/returns/stock/{symbol}")
async def get_stock_returns_data(
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

@router.get("/returns/summary")
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

@router.get("/returns/top-performers")
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