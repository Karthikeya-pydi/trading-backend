from typing import Dict, List, Optional, Literal, Any
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session
from fastapi import HTTPException, Depends
import traceback

from app.models.user import User
from app.schemas.trading import TradeRequest, MarketDataRequest
from app.services.iifl_connect import IIFLConnect
from app.core.database import get_db

class IIFLServiceFixed:
    """
    Fixed IIFL Service that properly uses IIFLConnect wrapper
    instead of making raw HTTP requests
    """
    
    def __init__(self, db: Session):
        self.db = db
        # Cache IIFLConnect instances per user to avoid repeated logins
        self._client_cache = {}
        
    def _get_client(self, user_id: int, api_type: Literal["market", "interactive"]) -> IIFLConnect:
        """Get or create authenticated IIFLConnect client"""
        cache_key = f"{user_id}_{api_type}"
        
        # Return cached client if exists and still valid
        if cache_key in self._client_cache:
            client = self._client_cache[cache_key]
            # Check if client is still authenticated
            if client.token:
                return client
            else:
                # Remove invalid client from cache
                del self._client_cache[cache_key]
        
        # Get user and create new client
        user = self.db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ValueError("User not found")
        
        # Check if user has credentials for this API type
        if api_type == "interactive":
            if not user.iifl_interactive_api_key:
                raise ValueError("IIFL Interactive credentials not found for user")
        else:  # market
            if not user.iifl_market_api_key:
                raise ValueError("IIFL Market credentials not found for user")
        
        # Create IIFLConnect instance
        try:
            client = IIFLConnect(user, api_type)
            logger.info(f"Created IIFL client for user {user_id}, api_type: {api_type}")
        except Exception as e:
            logger.error(f"Failed to create IIFL client: {traceback.format_exc()}")
            raise ValueError(f"Failed to create IIFL client: {str(e)}")
        
        # Login and cache the client
        try:
            if api_type == "interactive":
                login_response = client.interactive_login()
                logger.info(f"IIFL Interactive login successful for user {user_id}: {login_response}")
            else:  # market
                login_response = client.marketdata_login()
                logger.info(f"IIFL Market Data login successful for user {user_id}: {login_response}")
            
            # Cache the authenticated client
            self._client_cache[cache_key] = client
            return client
            
        except Exception as e:
            logger.error(f"IIFL {api_type} login failed for user {user_id}: {traceback.format_exc()}")
            raise HTTPException(status_code=401, detail=f"IIFL {api_type} authentication failed: {str(e)}")

    def place_order(self, db: Session, user_id: int, trade_request: TradeRequest) -> Dict:
        """Place order through IIFL Interactive API using IIFLConnect"""
        try:
            # Get authenticated IIFLConnect client
            client = self._get_client(user_id, "interactive")
            
            # Determine exchange segment and instrument ID
            exchange_segment = self._get_exchange_segment(trade_request.underlying_instrument)
            instrument_id = self._get_instrument_id(trade_request)
            
            # Determine order type
            if trade_request.price is not None:
                order_type = "LIMIT"
                limit_price = trade_request.price
            else:
                order_type = "MARKET"
                limit_price = 0
            
            # Stop loss handling
            stop_price = trade_request.stop_loss_price or 0
            if stop_price > 0:
                order_type = "SL-M"  # Stop Loss Market
                limit_price = 0
            
            # Use IIFLConnect's place_order method
            order_result = client.place_order(
                exchangeSegment=exchange_segment,
                exchangeInstrumentID=instrument_id,
                productType="NRML",  # Can be made configurable
                orderType=order_type,
                orderSide=trade_request.order_type,  # BUY or SELL
                timeInForce="DAY",
                disclosedQuantity=0,
                orderQuantity=trade_request.quantity,
                limitPrice=limit_price,
                stopPrice=stop_price,
                orderUniqueIdentifier=f"user_{user_id}_{int(datetime.now().timestamp())}",
                apiOrderSource="WebAPI"
            )
            
            logger.info(f"Order placed for user {user_id}: {order_result}")
            return order_result
            
        except Exception as e:
            logger.error(f"Order placement failed for user {user_id}: {e}")
            # Clear cache on error to force re-authentication next time
            cache_key = f"{user_id}_interactive"
            if cache_key in self._client_cache:
                del self._client_cache[cache_key]
            raise

    def get_order_book(self, db: Session, user_id: int) -> Dict:
        """Get order book from IIFL Interactive API"""
        try:
            client = self._get_client(user_id, "interactive")
            return client.get_order_book()
        except Exception as e:
            logger.error(f"Order book fetch failed for user {user_id}: {e}")
            raise

    def get_positions(self, db: Session, user_id: int) -> Dict:
        """Get positions from IIFL Interactive API"""
        try:
            client = self._get_client(user_id, "interactive")
            return client.get_position_netwise()
        except Exception as e:
            logger.error(f"Positions fetch failed for user {user_id}: {e}")
            raise

    def get_holdings(self, db: Session, user_id: int) -> Dict:
        """Get long-term holdings from IIFL Interactive API"""
        try:
            client = self._get_client(user_id, "interactive")
            return client.get_holding()
        except Exception as e:
            logger.error(f"Holdings fetch failed for user {user_id}: {e}")
            raise

    def get_market_data(self, db: Session, user_id: int, instruments: List[str]) -> Dict:
        """Get market data using IIFL Market Data API"""
        try:
            client = self._get_client(user_id, "market")
            
            # Convert instrument symbols to IIFL format
            instrument_list = []
            for symbol in instruments:
                exchange_segment = self._get_exchange_segment(symbol)
                instrument_id = self._get_instrument_id_by_symbol(symbol)
                instrument_list.append({
                    "exchangeSegment": exchange_segment,
                    "exchangeInstrumentID": instrument_id
                })
            
            # Get quotes using IIFLConnect
            return client.get_quote(
                Instruments=instrument_list,
                xtsMessageCode=1502,  # Full market data
                publishFormat="JSON"
            )
            
        except Exception as e:
            logger.error(f"Market data fetch failed for user {user_id}: {e}")
            raise

    def cancel_order(self, db: Session, user_id: int, order_id: str) -> Dict:
        """Cancel order using IIFL Interactive API"""
        try:
            client = self._get_client(user_id, "interactive")
            return client.cancel_order(
                appOrderID=order_id,
                orderUniqueIdentifier=f"cancel_{order_id}_{int(datetime.now().timestamp())}"
            )
        except Exception as e:
            logger.error(f"Order cancellation failed for user {user_id}: {e}")
            raise

    def modify_order(self, db: Session, user_id: int, order_id: str, modification: TradeRequest) -> Dict:
        """Modify order using IIFL Interactive API"""
        try:
            client = self._get_client(user_id, "interactive")
            
            # Determine modified order type
            if modification.price is not None:
                order_type = "LIMIT"
                limit_price = modification.price
            else:
                order_type = "MARKET"
                limit_price = 0
            
            return client.modify_order(
                appOrderID=int(order_id),
                modifiedProductType="NRML",
                modifiedOrderType=order_type,
                modifiedOrderQuantity=modification.quantity,
                modifiedDisclosedQuantity=0,
                modifiedLimitPrice=limit_price,
                modifiedStopPrice=modification.stop_loss_price or 0,
                modifiedTimeInForce="DAY",
                orderUniqueIdentifier=f"modify_{order_id}_{int(datetime.now().timestamp())}"
            )
        except Exception as e:
            logger.error(f"Order modification failed for user {user_id}: {e}")
            raise

    def get_user_profile(self, db: Session, user_id: int) -> Dict:
        """Get user profile from IIFL"""
        try:
            client = self._get_client(user_id, "interactive")
            return client.get_profile()
        except Exception as e:
            logger.error(f"Profile fetch failed for user {user_id}: {e}")
            raise

    def get_balance(self, db: Session, user_id: int) -> Dict:
        """Get account balance from IIFL"""
        try:
            client = self._get_client(user_id, "interactive")
            return client.get_balance()
        except Exception as e:
            logger.error(f"Balance fetch failed for user {user_id}: {e}")
            raise

    def get_ltp(self, db: Session, user_id: int, instruments: List[Dict]) -> Dict:
        """Get Last Traded Price for instruments using IIFLConnect"""
        try:
            client = self._get_client(user_id, "market")
            
            # Use IIFLConnect's get_quote method
            quote_result = client.get_quote(
                Instruments=instruments,
                xtsMessageCode=1502,  # LTP message code
                publishFormat="JSON"
            )
            
            # Parse LTP data
            ltp_data = {}
            if quote_result.get("type") == "success":
                quotes = quote_result["result"].get("listQuotes", [])
                for quote_str in quotes:
                    import json
                    quote = json.loads(quote_str)
                    instrument_id = quote.get("ExchangeInstrumentID")
                    ltp = float(quote.get("LastTradedPrice", 0))
                    if instrument_id:
                        ltp_data[int(instrument_id)] = ltp
            
            return ltp_data
            
        except Exception as e:
            logger.error(f"LTP fetch failed for user {user_id}: {e}")
            raise

    def logout_user(self, user_id: int, api_type: Literal["market", "interactive", "both"] = "both"):
        """Logout user from IIFL and clear cache"""
        if api_type in ["interactive", "both"]:
            cache_key = f"{user_id}_interactive"
            if cache_key in self._client_cache:
                try:
                    self._client_cache[cache_key].interactive_logout()
                except:
                    pass  # Ignore logout errors
                del self._client_cache[cache_key]
        
        if api_type in ["market", "both"]:
            cache_key = f"{user_id}_market"
            if cache_key in self._client_cache:
                try:
                    self._client_cache[cache_key].marketdata_logout()
                except:
                    pass  # Ignore logout errors
                del self._client_cache[cache_key]

    # Helper methods (same as before)
    def _get_exchange_segment(self, instrument: str) -> str:
        """Get exchange segment for instrument"""
        if instrument in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            return "NSEFO"  # NSE F&O for derivatives
        else:
            return "NSECM"  # NSE Cash Market for equities

    def _get_instrument_id(self, trade_request: TradeRequest) -> int:
        """Get instrument ID - this needs proper implementation"""
        # This is still a placeholder - you need to implement proper instrument lookup
        instrument_map = {
            "NIFTY": 26000,
            "BANKNIFTY": 26009,
            "FINNIFTY": 26037,
            "MIDCPNIFTY": 26014
        }
        return instrument_map.get(trade_request.underlying_instrument, 26000)

    def _get_instrument_id_by_symbol(self, symbol: str) -> int:
        """Get instrument ID by symbol"""
        instrument_map = {
            "NIFTY": 26000,
            "BANKNIFTY": 26009,
            "FINNIFTY": 26037,
            "MIDCPNIFTY": 26014
        }
        return instrument_map.get(symbol, 26000)

    def update_user_credentials(
        self,
        user: User,
        market_api_key: Optional[str] = None,
        market_secret_key: Optional[str] = None,
        market_user_id: Optional[str] = None,
        interactive_api_key: Optional[str] = None,
        interactive_secret_key: Optional[str] = None,
        interactive_user_id: Optional[str] = None
    ) -> User:
        """Update user IIFL credentials and clear cache"""
        from app.core.security import encrypt_data
        
        if market_api_key:
            user.iifl_market_api_key = encrypt_data(market_api_key)
        if market_secret_key:
            user.iifl_market_secret_key = encrypt_data(market_secret_key)
        if market_user_id:
            user.iifl_market_user_id = market_user_id
            
        if interactive_api_key:
            user.iifl_interactive_api_key = encrypt_data(interactive_api_key)
        if interactive_secret_key:
            user.iifl_interactive_secret_key = encrypt_data(interactive_secret_key)
        if interactive_user_id:
            user.iifl_interactive_user_id = interactive_user_id
        
        # Clear cached clients for this user
        self.logout_user(user.id, "both")
        
        return user

    def get_instrument_master(self, db: Session, user_id: int, exchange_segments: List[str] = None) -> Dict:
        """Download instrument master data from IIFL"""
        try:
            client = self._get_client(user_id, "market")
            
            # Default to major exchange segments if none specified
            if not exchange_segments:
                exchange_segments = ["NSECM", "NSEFO"]
            
            return client.get_master(exchangeSegmentList=exchange_segments)
            
        except Exception as e:
            logger.error(f"Instrument master fetch failed for user {user_id}: {e}")
            raise

    def search_instruments(self, db: Session, user_id: int, search_string: str) -> Dict:
        """Search instruments by name/symbol"""
        try:
            client = self._get_client(user_id, "market")
            return client.search_by_scriptname(searchString=search_string)
            
        except Exception as e:
            logger.error(f"Instrument search failed for user {user_id}: {e}")
            raise

def get_iifl_service_fixed(db: Session = Depends(get_db)) -> IIFLServiceFixed:
    """Dependency to get fixed IIFL service instance"""
    return IIFLServiceFixed(db) 