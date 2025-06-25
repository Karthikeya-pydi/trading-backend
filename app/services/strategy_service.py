import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Literal
from sqlalchemy.orm import Session
from loguru import logger

from app.models.trade import Trade, Position
from app.models.user import User
from app.schemas.trading import TradeRequest
from app.services.iifl_service import IIFLService

class StrategyService:
    def __init__(self, db: Session):
        self.db = db
        self.iifl_service = IIFLService(db)

    async def execute_stop_loss_strategy(self, user_id: int) -> Dict:
        """Monitor and execute stop loss orders"""
        try:
            # Get positions with active stop losses
            positions = self.db.query(Position).filter(
                Position.user_id == user_id,
                Position.stop_loss_active == True,
                Position.stop_loss_price.isnot(None)
            ).all()

            if not positions:
                return {"message": "No positions with active stop losses"}

            triggered_stops = []
            
            for position in positions:
                current_price = position.current_price or 0
                stop_price = position.stop_loss_price
                
                # Check if stop loss should trigger
                should_trigger = False
                if position.quantity > 0:  # Long position
                    should_trigger = current_price <= stop_price
                else:  # Short position
                    should_trigger = current_price >= stop_price
                
                if should_trigger:
                    # Create stop loss order
                    stop_order = TradeRequest(
                        underlying_instrument=position.underlying_instrument,
                        option_type=position.option_type,
                        strike_price=position.strike_price,
                        expiry_date=position.expiry_date,
                        order_type="SELL" if position.quantity > 0 else "BUY",
                        quantity=abs(position.quantity),
                        price=None  # Market order for stop loss
                    )
                    
                    # Place stop loss order
                    order_result = self.iifl_service.place_order(self.db, user_id, stop_order)
                    
                    # Deactivate stop loss
                    position.stop_loss_active = False
                    
                    triggered_stops.append({
                        "position_id": position.id,
                        "instrument": position.underlying_instrument,
                        "trigger_price": current_price,
                        "stop_price": stop_price,
                        "order_id": order_result.get("result", {}).get("AppOrderID", "")
                    })

            self.db.commit()
            
            return {
                "triggered_stops": len(triggered_stops),
                "details": triggered_stops
            }

        except Exception as e:
            logger.error(f"Stop loss execution failed for user {user_id}: {e}")
            raise

    async def bracket_order_strategy(
        self, 
        user_id: int, 
        trade_request: TradeRequest,
        target_price: float,
        stop_loss_price: float
    ) -> Dict:
        """Execute bracket order strategy"""
        try:
            # Place main order
            main_order = self.iifl_service.place_order(self.db, user_id, trade_request)
            
            if main_order.get("type") != "success":
                raise Exception(f"Main order failed: {main_order.get('description')}")
            
            main_order_id = main_order.get("result", {}).get("AppOrderID", "")
            
            # Create target order (opposite direction)
            target_order = TradeRequest(
                underlying_instrument=trade_request.underlying_instrument,
                option_type=trade_request.option_type,
                strike_price=trade_request.strike_price,
                expiry_date=trade_request.expiry_date,
                order_type="SELL" if trade_request.order_type == "BUY" else "BUY",
                quantity=trade_request.quantity,
                price=target_price
            )
            
            # Create stop loss order (opposite direction)
            stop_order = TradeRequest(
                underlying_instrument=trade_request.underlying_instrument,
                option_type=trade_request.option_type,
                strike_price=trade_request.strike_price,
                expiry_date=trade_request.expiry_date,
                order_type="SELL" if trade_request.order_type == "BUY" else "BUY",
                quantity=trade_request.quantity,
                stop_loss_price=stop_loss_price
            )
            
            # Place target and stop orders (OCO - One Cancels Other)
            target_result = self.iifl_service.place_order(self.db, user_id, target_order)
            stop_result = self.iifl_service.place_order(self.db, user_id, stop_order)
            
            return {
                "strategy": "bracket_order",
                "main_order_id": main_order_id,
                "target_order_id": target_result.get("result", {}).get("AppOrderID", ""),
                "stop_order_id": stop_result.get("result", {}).get("AppOrderID", ""),
                "status": "success"
            }

        except Exception as e:
            logger.error(f"Bracket order strategy failed for user {user_id}: {e}")
            raise

    async def momentum_strategy(
        self,
        user_id: int,
        instrument: str,
        price_threshold: float,
        quantity: int,
        direction: Literal["bullish", "bearish"]
    ) -> Dict:
        """Execute momentum-based strategy"""
        try:
            # Get current price
            instruments = [{
                "exchangeSegment": 2 if instrument in ["NIFTY", "BANKNIFTY"] else 1,
                "exchangeInstrumentID": self._get_instrument_id(instrument)
            }]
            
            ltp_data = self.iifl_service.get_ltp(self.db, user_id, instruments)
            current_price = list(ltp_data.values())[0] if ltp_data else 0
            
            # Check momentum condition
            should_execute = False
            if direction == "bullish" and current_price >= price_threshold:
                should_execute = True
                order_type = "BUY"
            elif direction == "bearish" and current_price <= price_threshold:
                should_execute = True
                order_type = "SELL"
            
            if not should_execute:
                return {
                    "strategy": "momentum",
                    "status": "waiting",
                    "current_price": current_price,
                    "threshold": price_threshold,
                    "direction": direction
                }
            
            # Execute momentum trade
            trade_request = TradeRequest(
                underlying_instrument=instrument,
                order_type=order_type,
                quantity=quantity,
                price=None  # Market order for momentum
            )
            
            order_result = self.iifl_service.place_order(self.db, user_id, trade_request)
            
            return {
                "strategy": "momentum",
                "status": "executed",
                "trigger_price": current_price,
                "threshold": price_threshold,
                "direction": direction,
                "order_id": order_result.get("result", {}).get("AppOrderID", "")
            }

        except Exception as e:
            logger.error(f"Momentum strategy failed for user {user_id}: {e}")
            raise

    async def trail_stop_strategy(self, user_id: int, position_id: int, trail_percent: float) -> Dict:
        """Execute trailing stop loss strategy"""
        try:
            position = self.db.query(Position).filter(
                Position.id == position_id,
                Position.user_id == user_id
            ).first()
            
            if not position:
                raise Exception("Position not found")
            
            current_price = position.current_price or position.average_price
            
            # Calculate trailing stop
            if position.quantity > 0:  # Long position
                trail_stop_price = current_price * (1 - trail_percent / 100)
                # Update stop only if it's higher than current stop
                if not position.stop_loss_price or trail_stop_price > position.stop_loss_price:
                    position.stop_loss_price = trail_stop_price
                    position.stop_loss_active = True
            else:  # Short position
                trail_stop_price = current_price * (1 + trail_percent / 100)
                # Update stop only if it's lower than current stop
                if not position.stop_loss_price or trail_stop_price < position.stop_loss_price:
                    position.stop_loss_price = trail_stop_price
                    position.stop_loss_active = True
            
            self.db.commit()
            
            return {
                "strategy": "trailing_stop",
                "position_id": position_id,
                "current_price": current_price,
                "new_stop_price": position.stop_loss_price,
                "trail_percent": trail_percent,
                "status": "updated"
            }

        except Exception as e:
            logger.error(f"Trailing stop strategy failed for user {user_id}: {e}")
            raise

    def _get_instrument_id(self, instrument: str) -> int:
        """Get instrument ID for strategy execution"""
        # Simplified mapping - in production, fetch from instrument master
        instrument_map = {
            "NIFTY": 26000,
            "BANKNIFTY": 26001,
            "FINNIFTY": 26034,
            "MIDCPNIFTY": 26121
        }
        return instrument_map.get(instrument, 26000) 