from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
import json
import asyncio
from loguru import logger
import redis.asyncio as redis
from app.core.config import settings

class ConnectionManager:
    def __init__(self):
        # Store active connections by user_id
        self.active_connections: Dict[int, List[WebSocket]] = {}
        # Store subscriptions by user_id -> set of symbols
        self.user_subscriptions: Dict[int, Set[str]] = {}
        # Store symbol subscribers symbol -> set of user_ids
        self.symbol_subscribers: Dict[str, Set[int]] = {}
        # Redis connection for pub/sub
        self.redis_client = None
        self.pubsub = None
        
    async def connect(self, websocket: WebSocket, user_id: int):
        """Accept a new WebSocket connection"""
        await websocket.accept()
        
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)
        
        if user_id not in self.user_subscriptions:
            self.user_subscriptions[user_id] = set()
            
        logger.info(f"User {user_id} connected via WebSocket")
        
        # Send welcome message
        await self.send_personal_message({
            "type": "connection",
            "message": "Connected to trading platform",
            "user_id": user_id
        }, websocket)
    
    def disconnect(self, websocket: WebSocket, user_id: int):
        """Remove a WebSocket connection"""
        if user_id in self.active_connections:
            if websocket in self.active_connections[user_id]:
                self.active_connections[user_id].remove(websocket)
            
            # Clean up empty connection lists
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
                
                # Clean up subscriptions
                if user_id in self.user_subscriptions:
                    for symbol in self.user_subscriptions[user_id]:
                        if symbol in self.symbol_subscribers:
                            self.symbol_subscribers[symbol].discard(user_id)
                            if not self.symbol_subscribers[symbol]:
                                del self.symbol_subscribers[symbol]
                    del self.user_subscriptions[user_id]
        
        logger.info(f"User {user_id} disconnected from WebSocket")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send a message to a specific WebSocket connection"""
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            logger.error(f"Error sending message to WebSocket: {e}")
    
    async def send_to_user(self, message: dict, user_id: int):
        """Send a message to all connections of a specific user"""
        if user_id in self.active_connections:
            disconnected_connections = []
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_text(json.dumps(message))
                except Exception as e:
                    logger.error(f"Error sending message to user {user_id}: {e}")
                    disconnected_connections.append(connection)
            
            # Clean up disconnected connections
            for conn in disconnected_connections:
                self.disconnect(conn, user_id)
    
    async def broadcast_to_subscribers(self, message: dict, symbol: str):
        """Broadcast a message to all users subscribed to a symbol"""
        if symbol in self.symbol_subscribers:
            for user_id in self.symbol_subscribers[symbol].copy():
                await self.send_to_user(message, user_id)
    
    async def subscribe_to_symbol(self, user_id: int, symbol: str):
        """Subscribe a user to real-time updates for a symbol"""
        if user_id not in self.user_subscriptions:
            self.user_subscriptions[user_id] = set()
        
        self.user_subscriptions[user_id].add(symbol)
        
        if symbol not in self.symbol_subscribers:
            self.symbol_subscribers[symbol] = set()
        self.symbol_subscribers[symbol].add(user_id)
        
        logger.info(f"User {user_id} subscribed to {symbol}")
        
        # Send confirmation
        await self.send_to_user({
            "type": "subscription",
            "action": "subscribed",
            "symbol": symbol,
            "message": f"Subscribed to {symbol} updates"
        }, user_id)
    
    async def unsubscribe_from_symbol(self, user_id: int, symbol: str):
        """Unsubscribe a user from a symbol"""
        if user_id in self.user_subscriptions:
            self.user_subscriptions[user_id].discard(symbol)
        
        if symbol in self.symbol_subscribers:
            self.symbol_subscribers[symbol].discard(user_id)
            if not self.symbol_subscribers[symbol]:
                del self.symbol_subscribers[symbol]
        
        logger.info(f"User {user_id} unsubscribed from {symbol}")
        
        # Send confirmation
        await self.send_to_user({
            "type": "subscription",
            "action": "unsubscribed",
            "symbol": symbol,
            "message": f"Unsubscribed from {symbol} updates"
        }, user_id)
    
    async def get_user_subscriptions(self, user_id: int) -> List[str]:
        """Get all symbols a user is subscribed to"""
        return list(self.user_subscriptions.get(user_id, set()))
    
    def get_all_subscribed_symbols(self) -> List[str]:
        """Get all symbols that have at least one subscriber"""
        return list(self.symbol_subscribers.keys())
    
    def get_subscription_count(self, symbol: str) -> int:
        """Get number of users subscribed to a symbol"""
        return len(self.symbol_subscribers.get(symbol, set()))
    
    async def start_redis_listener(self):
        """Start listening to Redis pub/sub for real-time updates"""
        try:
            self.redis_client = redis.from_url(settings.redis_url)
            self.pubsub = self.redis_client.pubsub()
            
            # Subscribe to channels
            await self.pubsub.subscribe(
                "market_data",
                "order_updates", 
                "position_updates",
                "trade_alerts",
                "system_notifications"
            )
            
            logger.info("Started Redis listener for real-time updates")
            
            async for message in self.pubsub.listen():
                if message["type"] == "message":
                    await self.handle_redis_message(message)
                    
        except Exception as e:
            logger.error(f"Redis listener error: {e}")
    
    async def handle_redis_message(self, message):
        """Handle incoming Redis pub/sub messages"""
        try:
            channel = message["channel"].decode()
            data = json.loads(message["data"].decode())
            
            if channel == "market_data":
                await self.handle_market_data_update(data)
            elif channel == "order_updates":
                await self.handle_order_update(data)
            elif channel == "position_updates":
                await self.handle_position_update(data)
            elif channel == "trade_alerts":
                await self.handle_trade_alert(data)
            elif channel == "system_notifications":
                await self.handle_system_notification(data)
                
        except Exception as e:
            logger.error(f"Error handling Redis message: {e}")
    
    async def handle_market_data_update(self, data):
        """Handle real-time market data updates"""
        symbol = data.get("symbol")
        if symbol and symbol in self.symbol_subscribers:
            message = {
                "type": "market_data",
                "symbol": symbol,
                "data": data,
                "timestamp": data.get("timestamp")
            }
            await self.broadcast_to_subscribers(message, symbol)
    
    async def handle_order_update(self, data):
        """Handle real-time order status updates"""
        user_id = data.get("user_id")
        if user_id:
            message = {
                "type": "order_update",
                "data": data,
                "timestamp": data.get("timestamp")
            }
            await self.send_to_user(message, user_id)
    
    async def handle_position_update(self, data):
        """Handle real-time position updates"""
        user_id = data.get("user_id")
        if user_id:
            message = {
                "type": "position_update",
                "data": data,
                "timestamp": data.get("timestamp")
            }
            await self.send_to_user(message, user_id)
    
    async def handle_trade_alert(self, data):
        """Handle trade alerts and notifications"""
        user_id = data.get("user_id")
        if user_id:
            message = {
                "type": "trade_alert",
                "data": data,
                "timestamp": data.get("timestamp")
            }
            await self.send_to_user(message, user_id)
    
    async def handle_system_notification(self, data):
        """Handle system-wide notifications"""
        message = {
            "type": "system_notification",
            "data": data,
            "timestamp": data.get("timestamp")
        }
        # Broadcast to all connected users
        for user_id in self.active_connections.keys():
            await self.send_to_user(message, user_id)

# Global connection manager instance
manager = ConnectionManager()
