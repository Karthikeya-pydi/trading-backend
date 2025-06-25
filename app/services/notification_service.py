from typing import Dict, List, Optional
from datetime import datetime
import asyncio
from loguru import logger

from app.services.realtime_service import realtime_service
from app.core.websocket_manager import manager

class NotificationService:
    def __init__(self):
        self.alert_rules = {}  # Store user alert rules
    
    async def create_price_alert(self, user_id: int, symbol: str, target_price: float, condition: str):
        """Create a price alert for a user"""
        alert_id = f"{user_id}_{symbol}_{target_price}_{condition}"
        
        self.alert_rules[alert_id] = {
            "user_id": user_id,
            "symbol": symbol,
            "target_price": target_price,
            "condition": condition,  # "above", "below"
            "created_at": datetime.now(),
            "triggered": False
        }
        
        logger.info(f"Created price alert for user {user_id}: {symbol} {condition} {target_price}")
    
    async def check_price_alerts(self, symbol: str, current_price: float):
        """Check if any price alerts should be triggered"""
        for alert_id, alert in self.alert_rules.items():
            if alert["symbol"] == symbol and not alert["triggered"]:
                should_trigger = False
                
                if alert["condition"] == "above" and current_price >= alert["target_price"]:
                    should_trigger = True
                elif alert["condition"] == "below" and current_price <= alert["target_price"]:
                    should_trigger = True
                
                if should_trigger:
                    await self.trigger_price_alert(alert_id, alert, current_price)
    
    async def trigger_price_alert(self, alert_id: str, alert: Dict, current_price: float):
        """Trigger a price alert"""
        alert["triggered"] = True
        
        await realtime_service.publish_trade_alert(alert["user_id"], {
            "type": "price_alert",
            "symbol": alert["symbol"],
            "message": f"{alert['symbol']} is now {alert['condition']} {alert['target_price']} (Current: {current_price})",
            "target_price": alert["target_price"],
            "current_price": current_price,
            "condition": alert["condition"]
        })
        
        logger.info(f"Triggered price alert {alert_id}")
    
    async def send_order_notification(self, user_id: int, order_data: Dict):
        """Send order status notification"""
        await realtime_service.publish_trade_alert(user_id, {
            "type": "order_notification",
            "symbol": order_data.get("symbol"),
            "message": f"Order {order_data.get('order_id')} is {order_data.get('status')}",
            "order_id": order_data.get("order_id"),
            "status": order_data.get("status")
        })
    
    async def send_position_alert(self, user_id: int, position_data: Dict):
        """Send position-related alert"""
        await realtime_service.publish_trade_alert(user_id, {
            "type": "position_alert",
            "symbol": position_data.get("symbol"),
            "message": position_data.get("message"),
            "pnl": position_data.get("pnl"),
            "pnl_percent": position_data.get("pnl_percent")
        })
    
    async def send_system_alert(self, alert_type: str, message: str, priority: str = "normal"):
        """Send system-wide alert"""
        await realtime_service.publish_system_notification({
            "type": alert_type,
            "title": "System Alert",
            "message": message,
            "priority": priority
        })

# Global notification service instance
notification_service = NotificationService()
