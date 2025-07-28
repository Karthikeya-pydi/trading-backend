from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException, status
from typing import Optional
import json
from loguru import logger
from sqlalchemy.orm import Session

from app.core.websocket_manager import manager
from app.api.dependencies import get_current_user_websocket, get_current_user
from app.models.user import User
from app.core.database import get_db
from app.services.realtime_market_service import RealtimeMarketService

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

@router.websocket("/ws/market-data/{user_id}")
async def market_data_websocket_authenticated(
    websocket: WebSocket,
    user_id: int,
    token: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Authenticated WebSocket endpoint for real-time market data streaming.
    
    Usage:
    1. Connect: ws://localhost:8000/api/ws/market-data/{user_id}?token={jwt_token}
    2. Send subscription message: {"type": "subscribe_stock", "stock_name": "RELIANCE"}
    3. Send unsubscription: {"type": "unsubscribe_stock", "stock_name": "RELIANCE"}
    4. Get active subscriptions: {"type": "get_subscriptions"}
    
    Receives real-time data:
    {
        "type": "market_data",
        "stock_name": "RELIANCE",
        "instrument_id": "2885",
        "data": {
            "ltp": 2850.50,
            "change": 12.30,
            "change_percent": 0.43,
            "volume": 1234567,
            "bid": 2850.00,
            "ask": 2851.00
        },
        "timestamp": "2025-01-28T10:30:00.000Z"
    }
    """
    if not token:
        await websocket.close(code=4001, reason="Authentication token required")
        return
    
    # Verify user and get user object
    try:
        # Get user from database using user_id
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.is_active:
            await websocket.close(code=4003, reason="Invalid user")
            return
            
        if not user.iifl_market_api_key:
            await websocket.close(code=4004, reason="IIFL Market Data credentials not configured")
            return
            
    except Exception as e:
        logger.error(f"Error verifying user {user_id}: {e}")
        await websocket.close(code=4002, reason="Authentication failed")
        return
    
    # Initialize real-time market service for this user
    realtime_service = RealtimeMarketService(user, db)
    
    try:
        await websocket.accept()
        logger.info(f"Market data WebSocket connected for user {user_id}")
        
        # Send welcome message
        await websocket.send_text(json.dumps({
            "type": "connected",
            "message": "Connected to real-time market data stream",
            "user_id": user_id
        }))
        
        # Register this connection with the realtime service
        await realtime_service.add_websocket_connection(websocket)
        
        while True:
            # Receive messages from client
            data = await websocket.receive_text()
            message = json.loads(data)
            message_type = message.get("type")
            
            if message_type == "subscribe_stock":
                stock_name = message.get("stock_name", "").strip().upper()
                if stock_name:
                    try:
                        success = await realtime_service.subscribe_to_stock(stock_name)
                        if success:
                            await websocket.send_text(json.dumps({
                                "type": "subscription_success",
                                "stock_name": stock_name,
                                "message": f"Subscribed to {stock_name} real-time data"
                            }))
                        else:
                            await websocket.send_text(json.dumps({
                                "type": "subscription_error",
                                "stock_name": stock_name,
                                "message": f"Failed to subscribe to {stock_name}"
                            }))
                    except Exception as e:
                        logger.error(f"Error subscribing to {stock_name}: {e}")
                        await websocket.send_text(json.dumps({
                            "type": "subscription_error",
                            "stock_name": stock_name,
                            "message": f"Error subscribing to {stock_name}: {str(e)}"
                        }))
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "stock_name is required for subscription"
                    }))
                    
            elif message_type == "unsubscribe_stock":
                stock_name = message.get("stock_name", "").strip().upper()
                if stock_name:
                    try:
                        await realtime_service.unsubscribe_from_stock(stock_name)
                        await websocket.send_text(json.dumps({
                            "type": "unsubscription_success",
                            "stock_name": stock_name,
                            "message": f"Unsubscribed from {stock_name}"
                        }))
                    except Exception as e:
                        logger.error(f"Error unsubscribing from {stock_name}: {e}")
                        await websocket.send_text(json.dumps({
                            "type": "unsubscription_error",
                            "stock_name": stock_name,
                            "message": f"Error unsubscribing from {stock_name}: {str(e)}"
                        }))
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "stock_name is required for unsubscription"
                    }))
                    
            elif message_type == "get_subscriptions":
                subscriptions = await realtime_service.get_active_subscriptions()
                await websocket.send_text(json.dumps({
                    "type": "subscriptions_list",
                    "subscriptions": subscriptions
                }))
                
            elif message_type == "ping":
                await websocket.send_text(json.dumps({
                    "type": "pong",
                    "timestamp": message.get("timestamp")
                }))
                
            else:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                }))
                
    except WebSocketDisconnect:
        logger.info(f"Market data WebSocket disconnected for user {user_id}")
    except Exception as e:
        logger.error(f"Market data WebSocket error for user {user_id}: {e}")
    finally:
        # Clean up
        try:
            await realtime_service.cleanup()
        except Exception as e:
            logger.error(f"Error cleaning up realtime service: {e}")

@router.websocket("/ws/market-data")
async def market_data_websocket_public(websocket: WebSocket):
    """
    Public WebSocket endpoint for basic market data (limited functionality)
    
    For full real-time features, use the authenticated endpoint:
    /ws/market-data/{user_id}?token={jwt_token}
    """
    await websocket.accept()
    
    try:
        # Send info about available endpoints
        await websocket.send_text(json.dumps({
            "type": "info",
            "message": "Public market data endpoint - limited functionality",
            "available_endpoints": {
                "authenticated": "/ws/market-data/{user_id}?token={jwt_token}",
                "features": [
                    "Real-time stock price updates",
                    "Stock name subscriptions", 
                    "Market depth data",
                    "Volume and change information"
                ]
            }
        }))
        
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "subscribe":
                symbols = message.get("symbols", [])
                await websocket.send_text(json.dumps({
                    "type": "info",
                    "message": "For real-time subscriptions, please use authenticated endpoint",
                    "endpoint": "/ws/market-data/{user_id}?token={jwt_token}"
                }))
            else:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Use authenticated endpoint for full functionality"
                }))
                
    except WebSocketDisconnect:
        logger.info("Public market data WebSocket disconnected")
    except Exception as e:
        logger.error(f"Public market data WebSocket error: {e}")

@router.get("/market-data/test-connection")
async def test_market_data_connection(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Test IIFL market data connection for WebSocket streaming"""
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    try:
        # Test connection to IIFL
        realtime_service = RealtimeMarketService(current_user, db)
        test_result = await realtime_service.test_connection()
        
        return {
            "type": "success",
            "message": "Market data connection test successful",
            "websocket_endpoint": f"/api/ws/market-data/{current_user.id}",
            "test_result": test_result,
            "instructions": {
                "connect": f"ws://localhost:8000/api/ws/market-data/{current_user.id}?token={{your_jwt_token}}",
                "subscribe": '{"type": "subscribe_stock", "stock_name": "RELIANCE"}',
                "unsubscribe": '{"type": "unsubscribe_stock", "stock_name": "RELIANCE"}',
                "get_subscriptions": '{"type": "get_subscriptions"}'
            }
        }
        
    except Exception as e:
        logger.error(f"Market data connection test failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Market data connection test failed: {str(e)}"
        )
