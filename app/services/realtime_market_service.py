import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Set
from fastapi import WebSocket
from loguru import logger
from sqlalchemy.orm import Session

from app.models.user import User
from app.services.iifl_connect import IIFLConnect, IIFLBinaryMarketDataClient
from app.core.database import get_db

class RealtimeMarketService:
    """
    Service for handling real-time market data streaming using IIFL's Binary Market Data API.
    
    Features:
    - Stock name to instrument ID mapping
    - Real-time price updates via WebSocket
    - Subscription management
    - IIFL Binary Market Data integration
    """
    
    def __init__(self, user: User, db: Session):
        self.user = user
        self.db = db
        self.iifl_client = None
        self.binary_client = None
        self.websocket_connections: Set[WebSocket] = set()
        self.active_subscriptions: Dict[str, Dict] = {}  # stock_name -> instrument_info
        self.instrument_cache: Dict[str, Dict] = {}  # Cache for stock name to instrument mapping
        self.is_connected = False
        self.streaming_task = None
        
    async def test_connection(self) -> Dict:
        """Test connection to IIFL Market Data API"""
        try:
            # Initialize IIFL Connect client
            self.iifl_client = IIFLConnect(self.user, api_type="market")
            
            # Test login
            login_response = self.iifl_client.marketdata_login()
            if login_response.get("type") != "success":
                return {
                    "status": "failed",
                    "error": "Failed to login to IIFL Market Data API",
                    "details": login_response
                }
            
            # Test a simple search
            test_search = self.iifl_client.search_by_scriptname("RELIANCE")
            if test_search.get("type") != "success":
                return {
                    "status": "failed", 
                    "error": "Failed to search instruments",
                    "details": test_search
                }
            
            # Logout after test
            self.iifl_client.marketdata_logout()
            
            return {
                "status": "success",
                "message": "IIFL Market Data API connection successful",
                "login_response": login_response,
                "search_test": test_search
            }
            
        except Exception as e:
            logger.error(f"Market data connection test failed: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def add_websocket_connection(self, websocket: WebSocket):
        """Add a WebSocket connection to receive real-time updates"""
        self.websocket_connections.add(websocket)
        logger.info(f"Added WebSocket connection, total: {len(self.websocket_connections)}")
        
        # Start streaming if this is the first connection
        if len(self.websocket_connections) == 1 and not self.is_connected:
            await self._initialize_iifl_connection()
    
    async def remove_websocket_connection(self, websocket: WebSocket):
        """Remove a WebSocket connection"""
        self.websocket_connections.discard(websocket)
        logger.info(f"Removed WebSocket connection, total: {len(self.websocket_connections)}")
        
        # Stop streaming if no more connections
        if len(self.websocket_connections) == 0:
            await self._cleanup_iifl_connection()
    
    async def subscribe_to_stock(self, stock_name: str) -> bool:
        """
        Subscribe to real-time data for a stock by name.
        
        Args:
            stock_name: Stock symbol/name (e.g., "RELIANCE", "TCS")
            
        Returns:
            bool: True if subscription successful, False otherwise
        """
        try:
            stock_name = stock_name.strip().upper()
            
            # Check if already subscribed
            if stock_name in self.active_subscriptions:
                logger.info(f"Already subscribed to {stock_name}")
                return True
            
            # Get instrument details for this stock
            instrument_info = await self._get_instrument_info(stock_name)
            if not instrument_info:
                logger.error(f"Could not find instrument info for {stock_name}")
                return False
            
            # Store subscription
            self.active_subscriptions[stock_name] = instrument_info
            
            # Subscribe to IIFL streaming if connected
            if self.iifl_client and self.is_connected:
                await self._subscribe_to_iifl_instrument(instrument_info)
            
            logger.info(f"Successfully subscribed to {stock_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to {stock_name}: {e}")
            return False
    
    async def unsubscribe_from_stock(self, stock_name: str):
        """Unsubscribe from real-time data for a stock"""
        try:
            stock_name = stock_name.strip().upper()
            
            if stock_name in self.active_subscriptions:
                instrument_info = self.active_subscriptions[stock_name]
                
                # Unsubscribe from IIFL streaming
                if self.iifl_client and self.is_connected:
                    await self._unsubscribe_from_iifl_instrument(instrument_info)
                
                # Remove from active subscriptions
                del self.active_subscriptions[stock_name]
                logger.info(f"Unsubscribed from {stock_name}")
            
        except Exception as e:
            logger.error(f"Error unsubscribing from {stock_name}: {e}")
    
    async def get_active_subscriptions(self) -> List[str]:
        """Get list of currently subscribed stock names"""
        return list(self.active_subscriptions.keys())
    
    async def _get_instrument_info(self, stock_name: str) -> Optional[Dict]:
        """Get instrument information for a stock name"""
        try:
            # Check cache first
            if stock_name in self.instrument_cache:
                return self.instrument_cache[stock_name]
            
            # Ensure IIFL client is available
            if not self.iifl_client:
                self.iifl_client = IIFLConnect(self.user, api_type="market")
                login_response = self.iifl_client.marketdata_login()
                if login_response.get("type") != "success":
                    logger.error("Failed to login to IIFL for instrument search")
                    return None
            
            # Search for the stock
            search_response = self.iifl_client.search_by_scriptname(stock_name)
            
            if search_response.get("type") != "success" or not search_response.get("result"):
                logger.error(f"Stock '{stock_name}' not found in IIFL")
                return None
            
            # Filter for equity stocks (prefer NSECM - Cash Market, series EQ)
            stocks = search_response["result"]
            equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
            
            if equity_stocks:
                stock_info = equity_stocks[0]  # Prefer equity stocks
            else:
                stock_info = stocks[0]  # Fallback to first result
            
            instrument_info = {
                "stock_name": stock_name,
                "exchange_segment": stock_info.get("ExchangeSegment", 1),
                "instrument_id": stock_info.get("ExchangeInstrumentID"),
                "display_name": stock_info.get("DisplayName", stock_info.get("Name")),
                "symbol": stock_info.get("Name"),
                "series": stock_info.get("Series"),
                "isin": stock_info.get("ISIN")
            }
            
            # Cache the result
            self.instrument_cache[stock_name] = instrument_info
            
            return instrument_info
            
        except Exception as e:
            logger.error(f"Error getting instrument info for {stock_name}: {e}")
            return None
    
    async def _initialize_iifl_connection(self):
        """Initialize connection to IIFL Binary Market Data API"""
        try:
            if self.is_connected:
                return
            
            # Initialize IIFL Connect client if not already done
            if not self.iifl_client:
                self.iifl_client = IIFLConnect(self.user, api_type="market")
            
            # Login to get token and user ID
            login_response = self.iifl_client.marketdata_login()
            if login_response.get("type") != "success":
                logger.error("Failed to login to IIFL Market Data API")
                return
            
            # Get token and user ID from login response
            token = self.iifl_client.token
            user_id = self.iifl_client.userID
            
            if not token or not user_id:
                logger.error("Missing token or user ID from IIFL login")
                return
            
            # For now, we'll just mark as connected and use polling instead of WebSocket
            # since the Binary Market Data WebSocket integration requires socketio library
            self.is_connected = True
            logger.info("Successfully connected to IIFL Market Data API")
            
            # Start polling for market data updates
            asyncio.create_task(self._start_polling_updates())
            
            # Subscribe to any pending stocks
            for stock_name, instrument_info in self.active_subscriptions.items():
                await self._subscribe_to_iifl_instrument(instrument_info)
                
        except Exception as e:
            logger.error(f"Error initializing IIFL connection: {e}")
            self.is_connected = False
    
    async def _start_polling_updates(self):
        """Start polling for market data updates (fallback if WebSocket not available)"""
        while self.is_connected and self.websocket_connections:
            try:
                # Get live data for all subscribed instruments
                for stock_name, instrument_info in self.active_subscriptions.items():
                    await self._poll_instrument_data(stock_name, instrument_info)
                
                # Wait before next poll (adjust based on your needs)
                await asyncio.sleep(1)  # Poll every second
                
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(5)  # Wait longer on error
    
    async def _poll_instrument_data(self, stock_name: str, instrument_info: Dict):
        """Poll market data for a specific instrument"""
        try:
            if not self.iifl_client:
                return
            
            # Get live quote data
            instruments = [{
                "exchangeSegment": instrument_info["exchange_segment"],
                "exchangeInstrumentID": instrument_info["instrument_id"]
            }]
            
            quote_response = self.iifl_client.get_quote(
                Instruments=instruments,
                xtsMessageCode=1501,  # Touchline data
                publishFormat="JSON"
            )
            
            if quote_response.get("type") == "success":
                # Parse and broadcast the data
                await self._handle_quote_data(stock_name, quote_response)
                
        except Exception as e:
            logger.error(f"Error polling data for {stock_name}: {e}")
    
    async def _handle_quote_data(self, stock_name: str, quote_response: Dict):
        """Handle quote data and broadcast to WebSocket clients"""
        try:
            result = quote_response.get("result", {})
            quotes = result.get("listQuotes", result.get("quotesList", []))
            
            if not quotes:
                return
            
            # Parse the first quote (should be our instrument)
            if isinstance(quotes[0], str):
                quote_data = json.loads(quotes[0])
            else:
                quote_data = quotes[0]
            
            # Format the data for WebSocket clients
            market_data = {
                "type": "market_data",
                "stock_name": stock_name,
                "instrument_id": str(quote_data.get("ExchangeInstrumentID", "")),
                "data_type": "touchline",
                "data": {
                    "ltp": float(quote_data.get("LastTradedPrice", 0)),
                    "change": float(quote_data.get("Change", 0)),
                    "change_percent": float(quote_data.get("PercentChange", 0)),
                    "volume": int(quote_data.get("Volume", 0)),
                    "high": float(quote_data.get("High", 0)),
                    "low": float(quote_data.get("Low", 0)),
                    "open": float(quote_data.get("Open", 0)),
                    "close": float(quote_data.get("Close", 0)),
                    "bid": float(quote_data.get("BidPrice", 0)),
                    "ask": float(quote_data.get("AskPrice", 0)),
                    "bid_size": int(quote_data.get("BidSize", 0)),
                    "ask_size": int(quote_data.get("AskSize", 0))
                },
                "timestamp": datetime.now().isoformat()
            }
            
            # Broadcast to all connected WebSocket clients
            await self._broadcast_to_websockets(market_data)
            
        except Exception as e:
            logger.error(f"Error handling quote data for {stock_name}: {e}")
    
    async def _broadcast_to_websockets(self, data: Dict):
        """Broadcast data to all connected WebSocket clients"""
        if not self.websocket_connections:
            return
        
        message = json.dumps(data)
        disconnected_connections = []
        
        for websocket in self.websocket_connections:
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.error(f"Error sending data to WebSocket: {e}")
                disconnected_connections.append(websocket)
        
        # Remove disconnected connections
        for websocket in disconnected_connections:
            self.websocket_connections.discard(websocket)
    
    async def _subscribe_to_iifl_instrument(self, instrument_info: Dict):
        """Subscribe to IIFL streaming for a specific instrument"""
        try:
            if not self.iifl_client:
                return
            
            # Subscribe to touchline data (basic market data)
            instruments = [{
                "exchangeSegment": instrument_info["exchange_segment"],
                "exchangeInstrumentID": instrument_info["instrument_id"]
            }]
            
            # Use the correct method name from IIFLConnect
            subscription_response = self.iifl_client.send_subscription(
                Instruments=instruments,
                xtsMessageCode=1501  # Touchline data
            )
            
            logger.info(f"Subscribed to IIFL streaming for {instrument_info['stock_name']}: {subscription_response}")
            
        except Exception as e:
            logger.error(f"Error subscribing to IIFL instrument {instrument_info['stock_name']}: {e}")
    
    async def _unsubscribe_from_iifl_instrument(self, instrument_info: Dict):
        """Unsubscribe from IIFL streaming for a specific instrument"""
        try:
            if not self.iifl_client:
                return
            
            instruments = [{
                "exchangeSegment": instrument_info["exchange_segment"],
                "exchangeInstrumentID": instrument_info["instrument_id"]
            }]
            
            # Use the correct method name from IIFLConnect
            unsubscription_response = self.iifl_client.send_unsubscription(
                Instruments=instruments,
                xtsMessageCode=1501
            )
            
            logger.info(f"Unsubscribed from IIFL streaming for {instrument_info['stock_name']}")
            
        except Exception as e:
            logger.error(f"Error unsubscribing from IIFL instrument {instrument_info['stock_name']}: {e}")
    
    async def _cleanup_iifl_connection(self):
        """Clean up IIFL connections"""
        try:
            if self.iifl_client:
                self.iifl_client.marketdata_logout()
                self.iifl_client = None
            
            self.is_connected = False
            logger.info("Cleaned up IIFL connections")
            
        except Exception as e:
            logger.error(f"Error cleaning up IIFL connections: {e}")
    
    async def cleanup(self):
        """Clean up all resources"""
        try:
            # Clear all subscriptions
            for stock_name in list(self.active_subscriptions.keys()):
                await self.unsubscribe_from_stock(stock_name)
            
            # Clear WebSocket connections
            self.websocket_connections.clear()
            
            # Clean up IIFL connections
            await self._cleanup_iifl_connection()
            
            logger.info("RealtimeMarketService cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}") 