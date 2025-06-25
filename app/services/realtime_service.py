import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional
import redis.asyncio as redis
from loguru import logger
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import get_db
from app.models.user import User
from app.models.trade import Trade, Position

class RealtimeService:
    def __init__(self):
        self.redis_client = None
        self.is_running = False
        self.market_data_task = None
        self.order_monitoring_task = None
        
    async def start(self):
        """Start all real-time services"""
        if self.is_running:
            return
            
        try:
            self.redis_client = redis.from_url(settings.redis_url)
            self.is_running = True
            
            # Start background tasks
            self.market_data_task = asyncio.create_task(self.market_data_loop())
            self.order_monitoring_task = asyncio.create_task(self.order_monitoring_loop())
            
            logger.info("Real-time services started")
            
        except Exception as e:
            logger.error(f"Failed to start real-time services: {e}")
    
    async def stop(self):
        """Stop all real-time services"""
        self.is_running = False
        
        if self.market_data_task:
            self.market_data_task.cancel()
        if self.order_monitoring_task:
            self.order_monitoring_task.cancel()
            
        if self.redis_client:
            await self.redis_client.close()
            
        logger.info("Real-time services stopped")
    
    async def publish_market_data(self, symbol: str, data: Dict):
        """Publish market data update to Redis"""
        try:
            message = {
                "symbol": symbol,
                "ltp": data.get("ltp"),
                "change": data.get("change"),
                "change_percent": data.get("change_percent"),
                "volume": data.get("volume"),
                "high": data.get("high"),
                "low": data.get("low"),
                "open": data.get("open"),
                "timestamp": datetime.now().isoformat()
            }
            
            await self.redis_client.publish("market_data", json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error publishing market data for {symbol}: {e}")
    
    async def publish_order_update(self, user_id: int, order_data: Dict):
        """Publish order status update to Redis"""
        try:
            message = {
                "user_id": user_id,
                "order_id": order_data.get("order_id"),
                "status": order_data.get("status"),
                "filled_quantity": order_data.get("filled_quantity"),
                "average_price": order_data.get("average_price"),
                "timestamp": datetime.now().isoformat()
            }
            
            await self.redis_client.publish("order_updates", json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error publishing order update for user {user_id}: {e}")
    
    async def publish_position_update(self, user_id: int, position_data: Dict):
        """Publish position update to Redis"""
        try:
            message = {
                "user_id": user_id,
                "symbol": position_data.get("symbol"),
                "quantity": position_data.get("quantity"),
                "average_price": position_data.get("average_price"),
                "current_price": position_data.get("current_price"),
                "unrealized_pnl": position_data.get("unrealized_pnl"),
                "timestamp": datetime.now().isoformat()
            }
            
            await self.redis_client.publish("position_updates", json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error publishing position update for user {user_id}: {e}")
    
    async def publish_trade_alert(self, user_id: int, alert_data: Dict):
        """Publish trade alert to Redis"""
        try:
            message = {
                "user_id": user_id,
                "type": alert_data.get("type"),  # "stop_loss_triggered", "target_reached", etc.
                "symbol": alert_data.get("symbol"),
                "message": alert_data.get("message"),
                "price": alert_data.get("price"),
                "timestamp": datetime.now().isoformat()
            }
            
            await self.redis_client.publish("trade_alerts", json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error publishing trade alert for user {user_id}: {e}")
    
    async def publish_system_notification(self, notification_data: Dict):
        """Publish system-wide notification to Redis"""
        try:
            message = {
                "type": notification_data.get("type"),  # "maintenance", "market_hours", etc.
                "title": notification_data.get("title"),
                "message": notification_data.get("message"),
                "priority": notification_data.get("priority", "normal"),
                "timestamp": datetime.now().isoformat()
            }
            
            await self.redis_client.publish("system_notifications", json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error publishing system notification: {e}")
    
    async def market_data_loop(self):
        """Continuously fetch and publish market data"""
        while self.is_running:
            try:
                # Get list of symbols that users are subscribed to
                # This would come from your connection manager
                symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
                
                # Fetch market data for subscribed symbols
                for symbol in symbols:
                    try:
                        # This is a simplified example - you'd integrate with IIFL's real-time API
                        market_data = await self.fetch_market_data(symbol)
                        if market_data:
                            await self.publish_market_data(symbol, market_data)
                    except Exception as e:
                        logger.error(f"Error fetching market data for {symbol}: {e}")
                
                # Wait before next update (adjust based on your needs)
                await asyncio.sleep(1)  # 1 second updates
                
            except Exception as e:
                logger.error(f"Error in market data loop: {e}")
                await asyncio.sleep(5)  # Wait longer on error
    
    async def fetch_market_data(self, symbol: str) -> Optional[Dict]:
        """Fetch real-time market data for a symbol"""
        try:
            # This is where you'd integrate with IIFL's real-time market data API
            # For now, returning mock data
            import random
            base_price = {"NIFTY": 22000, "BANKNIFTY": 45000, "FINNIFTY": 19000, "MIDCPNIFTY": 9000}.get(symbol, 1000)
            
            # Simulate price movement
            change = random.uniform(-50, 50)
            ltp = base_price + change
            change_percent = (change / base_price) * 100
            
            return {
                "ltp": round(ltp, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 2),
                "volume": random.randint(100000, 1000000),
                "high": round(ltp + random.uniform(0, 20), 2),
                "low": round(ltp - random.uniform(0, 20), 2),
                "open": round(base_price + random.uniform(-10, 10), 2)
            }
            
        except Exception as e:
            logger.error(f"Error fetching market data for {symbol}: {e}")
            return None
    
    async def order_monitoring_loop(self):
        """Monitor orders for status changes"""
        while self.is_running:
            try:
                # Get all pending orders from database
                # This would be done with proper database session management
                # For now, this is a placeholder
                
                # Check order status with IIFL API
                # Update database if status changed
                # Publish updates via Redis
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in order monitoring loop: {e}")
                await asyncio.sleep(10)
    
    async def check_stop_loss_triggers(self):
        """Check if any stop loss orders should be triggered"""
        try:
            # Get all active positions with stop loss
            # Check current prices against stop loss levels
            # Trigger stop loss orders if needed
            # Send alerts to users
            pass
            
        except Exception as e:
            logger.error(f"Error checking stop loss triggers: {e}")
    
    async def calculate_portfolio_updates(self, user_id: int):
        """Calculate and publish portfolio updates for a user"""
        try:
            # Get user's positions
            # Calculate current portfolio value
            # Calculate P&L
            # Publish updates
            pass
            
        except Exception as e:
            logger.error(f"Error calculating portfolio updates for user {user_id}: {e}")

# Global real-time service instance
realtime_service = RealtimeService()
