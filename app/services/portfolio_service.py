import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from loguru import logger
from fastapi import Depends

from app.models.trade import Trade, Position
from app.models.user import User
from app.services.iifl_service import IIFLService
from app.core.database import get_db

class PortfolioService:
    def __init__(self, db: Session):
        self.db = db
        self.iifl_service = IIFLService(db)

    def calculate_pnl(self, user_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict:
        """Calculate P&L for a user within a date range"""
        try:
            query = self.db.query(Trade).filter(Trade.user_id == user_id)
            
            if start_date:
                query = query.filter(Trade.executed_at >= start_date)
            if end_date:
                query = query.filter(Trade.executed_at <= end_date)
            
            trades = query.all()
            
            total_realized_pnl = 0.0
            total_charges = 0.0
            winning_trades = 0
            losing_trades = 0
            total_trades = len(trades)
            
            for trade in trades:
                if trade.order_status == "FILLED" and trade.average_price:
                    # Calculate realized P&L (simplified)
                    if trade.order_type == "SELL":
                        realized_pnl = (trade.average_price - (trade.price or 0)) * trade.filled_quantity
                        total_realized_pnl += realized_pnl
                        
                        if realized_pnl > 0:
                            winning_trades += 1
                        else:
                            losing_trades += 1
            
            # Calculate unrealized P&L from current positions
            positions = self.db.query(Position).filter(Position.user_id == user_id).all()
            total_unrealized_pnl = sum(pos.unrealized_pnl for pos in positions)
            
            return {
                "total_realized_pnl": total_realized_pnl,
                "total_unrealized_pnl": total_unrealized_pnl,
                "total_pnl": total_realized_pnl + total_unrealized_pnl,
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades,
                "win_rate": (winning_trades / total_trades * 100) if total_trades > 0 else 0,
                "total_charges": total_charges
            }
            
        except Exception as e:
            logger.error(f"P&L calculation failed for user {user_id}: {e}")
            raise

    def get_portfolio_summary(self, user_id: int) -> Dict:
        """Get comprehensive portfolio summary"""
        try:
            # Get current positions
            positions = self.db.query(Position).filter(Position.user_id == user_id).all()
            
            # Calculate total portfolio value
            total_investment = sum(pos.average_price * abs(pos.quantity) for pos in positions)
            total_current_value = sum((pos.current_price or pos.average_price) * abs(pos.quantity) for pos in positions)
            total_unrealized_pnl = total_current_value - total_investment
            
            # Get daily P&L
            today = datetime.now().date()
            daily_pnl = self.calculate_pnl(user_id, datetime.combine(today, datetime.min.time()))
            
            # Get monthly P&L
            month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            monthly_pnl = self.calculate_pnl(user_id, month_start)
            
            # Position breakdown
            long_positions = [pos for pos in positions if pos.quantity > 0]
            short_positions = [pos for pos in positions if pos.quantity < 0]
            
            return {
                "total_positions": len(positions),
                "long_positions": len(long_positions),
                "short_positions": len(short_positions),
                "total_investment": total_investment,
                "current_value": total_current_value,
                "unrealized_pnl": total_unrealized_pnl,
                "daily_pnl": daily_pnl["total_pnl"],
                "monthly_pnl": monthly_pnl["total_pnl"],
                "positions": [
                    {
                        "underlying": pos.underlying_instrument,
                        "option_type": pos.option_type,
                        "strike_price": pos.strike_price,
                        "quantity": pos.quantity,
                        "average_price": pos.average_price,
                        "current_price": pos.current_price,
                        "unrealized_pnl": pos.unrealized_pnl,
                        "position_type": "LONG" if pos.quantity > 0 else "SHORT"
                    }
                    for pos in positions
                ]
            }
            
        except Exception as e:
            logger.error(f"Portfolio summary failed for user {user_id}: {e}")
            raise

    async def update_position_prices(self, user_id: int) -> Dict:
        """Update current prices for all positions"""
        try:
            positions = self.db.query(Position).filter(Position.user_id == user_id).all()
            
            if not positions:
                return {"message": "No positions to update"}
            
            # Prepare instruments for LTP fetch
            instruments = []
            for pos in positions:
                # Create instrument identifier for IIFL API
                instrument = {
                    "exchangeSegment": 2 if pos.option_type else 1,  # F&O or Cash
                    "exchangeInstrumentID": self._get_position_instrument_id(pos)
                }
                instruments.append(instrument)
            
            # Fetch LTP data
            ltp_data = self.iifl_service.get_ltp(self.db, user_id, instruments)
            
            # Update position prices
            updated_count = 0
            for pos in positions:
                instrument_id = self._get_position_instrument_id(pos)
                if instrument_id in ltp_data:
                    old_price = pos.current_price
                    pos.current_price = ltp_data[instrument_id]
                    
                    # Recalculate unrealized P&L
                    pos.unrealized_pnl = (pos.current_price - pos.average_price) * pos.quantity
                    updated_count += 1
            
            self.db.commit()
            
            return {
                "message": f"Updated prices for {updated_count} positions",
                "total_positions": len(positions),
                "updated_positions": updated_count
            }
            
        except Exception as e:
            logger.error(f"Position price update failed for user {user_id}: {e}")
            raise

    def _get_position_instrument_id(self, position: Position) -> int:
        """Get instrument ID for a position (placeholder implementation)"""
        # This would need to map position details to IIFL instrument IDs
        # For now, using a simplified mapping
        base_id_map = {
            "NIFTY": 26000,
            "BANKNIFTY": 26001,
            "FINNIFTY": 26034,
            "MIDCPNIFTY": 26121
        }
        return base_id_map.get(position.underlying_instrument, 26000)

    def get_risk_metrics(self, user_id: int) -> Dict:
        """Calculate risk metrics for the portfolio"""
        try:
            positions = self.db.query(Position).filter(Position.user_id == user_id).all()
            
            if not positions:
                return {"message": "No positions for risk calculation"}
            
            # Calculate portfolio exposure
            total_long_value = sum(
                pos.current_price * pos.quantity 
                for pos in positions 
                if pos.quantity > 0 and pos.current_price
            )
            
            total_short_value = sum(
                abs(pos.current_price * pos.quantity) 
                for pos in positions 
                if pos.quantity < 0 and pos.current_price
            )
            
            net_exposure = total_long_value - total_short_value
            gross_exposure = total_long_value + total_short_value
            
            # Calculate concentration risk
            position_values = [
                abs(pos.current_price * pos.quantity) 
                for pos in positions 
                if pos.current_price
            ]
            
            max_position_value = max(position_values) if position_values else 0
            concentration_risk = (max_position_value / gross_exposure * 100) if gross_exposure > 0 else 0
            
            # Calculate positions at risk (positions with unrealized loss)
            positions_at_risk = len([pos for pos in positions if pos.unrealized_pnl < 0])
            total_loss_positions = sum(pos.unrealized_pnl for pos in positions if pos.unrealized_pnl < 0)
            
            return {
                "net_exposure": net_exposure,
                "gross_exposure": gross_exposure,
                "long_exposure": total_long_value,
                "short_exposure": total_short_value,
                "concentration_risk_percent": concentration_risk,
                "positions_at_risk": positions_at_risk,
                "total_unrealized_loss": total_loss_positions,
                "portfolio_diversity": len(set(pos.underlying_instrument for pos in positions))
            }
            
        except Exception as e:
            logger.error(f"Risk metrics calculation failed for user {user_id}: {e}")
            raise

def get_portfolio_service(db: Session = Depends(get_db)) -> PortfolioService:
    """Dependency injection for PortfolioService"""
    return PortfolioService(db)