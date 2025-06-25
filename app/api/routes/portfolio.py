from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.portfolio_service import PortfolioService
from app.services.iifl_service_fixed import IIFLServiceFixed

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
    
    try:
        # Use the fixed IIFL service
        iifl_service = IIFLServiceFixed(db)
        holdings_result = iifl_service.get_holdings(db, current_user.id)
        
        # Format the holdings data
        formatted_holdings = []
        total_investment = 0
        total_current_value = 0
        
        if holdings_result.get("type") == "success":
            rms_holdings = holdings_result.get("result", {}).get("RMSHoldings", {}).get("Holdings", {})
            
            # ISIN to stock name mapping (you can expand this)
            isin_to_name = {
                "INE548A01028": "HFCL Limited",
                "INE002A01018": "Reliance Industries",
                "INE467B01029": "TCS Limited",
                "INE040A01034": "HDFC Bank",
                "INE009A01021": "Infosys Limited"
                # Add more mappings as needed
            }
            
            # Prepare instruments for live price fetch
            instruments_for_ltp = []
            holdings_data = []
            
            for isin, holding_data in rms_holdings.items():
                stock_name = isin_to_name.get(isin, f"Stock-{isin[:6]}")
                quantity = holding_data.get("HoldingQuantity", 0)
                avg_price = holding_data.get("BuyAvgPrice", 0)
                nse_instrument_id = holding_data.get("ExchangeNSEInstrumentId", 0)
                
                investment_value = quantity * avg_price
                total_investment += investment_value
                
                # Add to LTP fetch list
                if nse_instrument_id:
                    instruments_for_ltp.append({
                        "exchangeSegment": "NSECM",  # NSE Cash Market
                        "exchangeInstrumentID": nse_instrument_id
                    })
                
                holdings_data.append({
                    "stock_name": stock_name,
                    "isin": isin,
                    "quantity": quantity,
                    "average_price": avg_price,
                    "investment_value": investment_value,
                    "purchase_date": holding_data.get("CreatedOn"),
                    "is_collateral": holding_data.get("IsCollateralHolding", False),
                    "nse_instrument_id": nse_instrument_id
                })
            
            # Fetch live prices if we have instruments
            current_prices = {}
            if instruments_for_ltp:
                try:
                    ltp_result = iifl_service.get_ltp(db, current_user.id, instruments_for_ltp)
                    if isinstance(ltp_result, dict):
                        current_prices = ltp_result
                except Exception as e:
                    logger.warning(f"Failed to fetch live prices: {e}")
            
            # Calculate current values and P&L
            for holding in holdings_data:
                nse_id = holding["nse_instrument_id"]
                current_price = current_prices.get(nse_id, holding["average_price"])
                
                current_value = holding["quantity"] * current_price
                unrealized_pnl = current_value - holding["investment_value"]
                
                holding.update({
                    "current_price": current_price,
                    "current_value": current_value,
                    "unrealized_pnl": unrealized_pnl,
                    "unrealized_pnl_percent": (unrealized_pnl / holding["investment_value"] * 100) if holding["investment_value"] > 0 else 0
                })
                
                total_current_value += current_value
                formatted_holdings.append(holding)
        
        total_unrealized_pnl = total_current_value - total_investment
        
        return {
            "status": "success",
            "summary": {
                "total_holdings": len(formatted_holdings),
                "total_investment": round(total_investment, 2),
                "total_current_value": round(total_current_value, 2),
                "unrealized_pnl": round(total_unrealized_pnl, 2),
                "unrealized_pnl_percent": round((total_unrealized_pnl / total_investment * 100) if total_investment > 0 else 0, 2),
                "holdings": formatted_holdings
            },
            "message": f"Holdings summary with live prices for {current_user.email}"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch holdings summary: {str(e)}"
        ) 