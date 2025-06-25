from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from typing import Optional
import json
from loguru import logger

from app.core.websocket_manager import manager
from app.api.dependencies import get_current_user_websocket
from app.models.user import User

router = APIRouter()

@router.websocket("/ws/{user_id}")
async def websocket_endpoint(
    websocket: WebSocket, 
    user_id: int,
    token: Optional[str] = Query(None)
):
    """WebSocket endpoint for real-time communication"""
    
    # Authenticate user (simplified - in production, verify JWT token)
    if not token:
        await websocket.close(code=4001, reason="Authentication required")
        return
    
    try:
        # Connect user
        await manager.connect(websocket, user_id)
        
        while True:
            # Receive message from client
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Handle different message types
            message_type = message.get("type")
            
            if message_type == "subscribe":
                symbol = message.get("symbol")
                if symbol:
                    await manager.subscribe_to_symbol(user_id, symbol)
                    
            elif message_type == "unsubscribe":
                symbol = message.get("symbol")
                if symbol:
                    await manager.unsubscribe_from_symbol(user_id, symbol)
                    
            elif message_type == "get_subscriptions":
                subscriptions = await manager.get_user_subscriptions(user_id)
                await manager.send_to_user({
                    "type": "subscriptions_list",
                    "subscriptions": subscriptions
                }, user_id)
                
            elif message_type == "ping":
                await manager.send_to_user({
                    "type": "pong",
                    "timestamp": message.get("timestamp")
                }, user_id)
                
            else:
                await manager.send_to_user({
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                }, user_id)
                
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
        logger.info(f"User {user_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for user {user_id}: {e}")
        manager.disconnect(websocket, user_id)

@router.websocket("/ws/market-data")
async def market_data_websocket(websocket: WebSocket):
    """Public WebSocket endpoint for market data (no authentication required)"""
    await websocket.accept()
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "subscribe":
                symbols = message.get("symbols", [])
                # Handle public market data subscription
                await websocket.send_text(json.dumps({
                    "type": "subscribed",
                    "symbols": symbols,
                    "message": "Subscribed to market data"
                }))
                
    except WebSocketDisconnect:
        logger.info("Market data WebSocket disconnected")
    except Exception as e:
        logger.error(f"Market data WebSocket error: {e}")
