from typing import Dict, List, Optional, Literal, Any
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session
from fastapi import HTTPException, Depends
import traceback
import json

from app.models.user import User
from app.schemas.trading import TradeRequest, MarketDataRequest
from app.core.security import encrypt_data, decrypt_data
from app.services.iifl_connect import IIFLConnect
from app.core.database import get_db

class IIFLService:
    """
    Unified IIFL Service that properly uses IIFLConnect wrapper
    with comprehensive trading, market data, and portfolio functionality
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
            # Validate trade request
            self._validate_trade_request(trade_request)
            
            # Get authenticated IIFLConnect client
            client = self._get_client(user_id, "interactive")
            
            # Get proper instrument details
            instrument_details = self._get_instrument_details(client, trade_request)
            
            # Determine order type and parameters
            order_params = self._prepare_order_parameters(trade_request, instrument_details)
            
            logger.info(f"Placing order for user {user_id} with params: {order_params}")
            
            # Use IIFLConnect's place_order method
            order_result = client.place_order(**order_params)
            
            logger.info(f"Order placed for user {user_id}: {order_result}")
            
            # Validate order result
            if order_result.get("type") != "success":
                error_msg = order_result.get("description", "Unknown error")
                logger.error(f"Order placement failed for user {user_id}: {error_msg}")
                raise HTTPException(
                    status_code=400, 
                    detail=f"IIFL order placement failed: {error_msg}"
                )
            
            return order_result
            
        except HTTPException:
            # Re-raise HTTPExceptions
            raise
        except Exception as e:
            logger.error(f"Order placement failed for user {user_id}: {traceback.format_exc()}")
            # Clear cache on error to force re-authentication next time
            cache_key = f"{user_id}_interactive"
            if cache_key in self._client_cache:
                del self._client_cache[cache_key]
            raise HTTPException(
                status_code=500,
                detail=f"Failed to place order: {str(e)}"
            )
    
    def place_order_with_details(self, db: Session, user_id: int, trade_request: TradeRequest, instrument_details: Dict) -> Dict:
        """Place order using provided instrument details (bypasses hardcoded lookup)"""
        try:
            # Validate trade request
            self._validate_trade_request(trade_request)
            
            # Get authenticated IIFLConnect client
            client = self._get_client(user_id, "interactive")
            
            # Use the provided instrument details instead of looking them up
            logger.info(f"Placing order for user {user_id} with provided instrument details: {instrument_details}")
            
            # Determine order type and parameters
            order_params = self._prepare_order_parameters(trade_request, instrument_details)
            
            logger.info(f"Placing order for user {user_id} with params: {order_params}")
            
            # Use IIFLConnect's place_order method
            order_result = client.place_order(**order_params)
            
            logger.info(f"Order placed for user {user_id}: {order_result}")
            
            # Validate order result
            if order_result.get("type") != "success":
                error_msg = order_result.get("description", "Unknown error")
                logger.error(f"Order placement failed for user {user_id}: {error_msg}")
                raise HTTPException(
                    status_code=400, 
                    detail=f"IIFL order placement failed: {error_msg}"
                )
            
            return order_result
            
        except HTTPException:
            # Re-raise HTTPExceptions
            raise
        except Exception as e:
            logger.error(f"Order placement failed for user {user_id}: {traceback.format_exc()}")
            # Clear cache on error to force re-authentication next time
            cache_key = f"{user_id}_interactive"
            if cache_key in self._client_cache:
                del self._client_cache[cache_key]
            raise HTTPException(
                status_code=500,
                detail=f"Failed to place order: {str(e)}"
            )

    def _validate_trade_request(self, trade_request: TradeRequest):
        """Validate trade request parameters"""
        if not trade_request.underlying_instrument:
            raise HTTPException(status_code=400, detail="Underlying instrument is required")
        
        if trade_request.quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be greater than 0")
        
        # Validate F&O specific parameters
        if self._is_futures_options_instrument(trade_request.underlying_instrument):
            if not trade_request.expiry_date:
                raise HTTPException(status_code=400, detail="Expiry date is required for F&O instruments")
            
            if trade_request.option_type and not trade_request.strike_price:
                raise HTTPException(status_code=400, detail="Strike price is required for options")
            
            if trade_request.strike_price and not trade_request.option_type:
                raise HTTPException(status_code=400, detail="Option type is required when strike price is provided")

    def _is_futures_options_instrument(self, instrument: str) -> bool:
        """Check if instrument is a futures/options instrument"""
        fno_instruments = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
        return instrument.upper() in fno_instruments

    def _get_instrument_details(self, client: IIFLConnect, trade_request: TradeRequest) -> Dict:
        """Get proper instrument details from IIFL using dynamic search"""
        try:
            exchange_segment = self._get_exchange_segment(trade_request.underlying_instrument)
            
            # For F&O instruments, try to get proper instrument ID
            if self._is_futures_options_instrument(trade_request.underlying_instrument):
                instrument_id = self._get_fno_instrument_id(client, trade_request)
            else:
                # For cash market instruments, use dynamic search instead of hardcoded mapping
                instrument_id = self._get_cash_instrument_id_dynamic(client, trade_request.underlying_instrument)
            
            return {
                "exchangeSegment": exchange_segment,
                "exchangeInstrumentID": instrument_id,
                "instrumentType": "F&O" if self._is_futures_options_instrument(trade_request.underlying_instrument) else "CASH"
            }
            
        except Exception as e:
            logger.error(f"Failed to get instrument details: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to get instrument details: {str(e)}"
            )

    def _get_fno_instrument_id(self, client: IIFLConnect, trade_request: TradeRequest) -> int:
        """Get F&O instrument ID using IIFL's instrument search"""
        try:
            # Search for the instrument using IIFL's search API
            search_string = self._build_search_string(trade_request)
            logger.info(f"Searching for F&O instrument: {search_string}")
            
            search_result = client.search_by_scriptname(searchString=search_string)
            
            if search_result.get("type") == "success":
                instruments = search_result.get("result", [])
                
                # Find the matching instrument
                for instrument in instruments:
                    if self._matches_trade_request(instrument, trade_request):
                        instrument_id = instrument.get("ExchangeInstrumentID")
                        if instrument_id:
                            logger.info(f"Found matching instrument ID: {instrument_id}")
                            return int(instrument_id)
            
            # Fallback to basic mapping if search fails
            logger.warning(f"Could not find exact instrument match for {search_string}, using fallback")
            return self._get_fallback_instrument_id(trade_request)
            
        except Exception as e:
            logger.error(f"Failed to get F&O instrument ID: {str(e)}")
            # Use fallback
            return self._get_fallback_instrument_id(trade_request)

    def _build_search_string(self, trade_request: TradeRequest) -> str:
        """Build search string for instrument lookup"""
        base = trade_request.underlying_instrument
        
        if trade_request.option_type:
            # For options: NIFTY 23DEC 19000 CE
            expiry_str = trade_request.expiry_date.strftime("%d%b").upper()
            strike_str = str(int(trade_request.strike_price))
            option_str = trade_request.option_type[:2].upper()  # CE or PE
            return f"{base} {expiry_str} {strike_str} {option_str}"
        else:
            # For futures: NIFTY 23DEC
            expiry_str = trade_request.expiry_date.strftime("%d%b").upper()
            return f"{base} {expiry_str}"

    def _matches_trade_request(self, instrument: Dict, trade_request: TradeRequest) -> bool:
        """Check if instrument matches trade request"""
        try:
            # Basic matching logic - can be enhanced based on actual IIFL response structure
            symbol = instrument.get("Name", "").upper()
            
            # Check if it contains the underlying
            if trade_request.underlying_instrument.upper() not in symbol:
                return False
            
            # For options, check strike and option type
            if trade_request.option_type and trade_request.strike_price:
                strike_str = str(int(trade_request.strike_price))
                option_str = trade_request.option_type[:2].upper()
                
                if strike_str not in symbol or option_str not in symbol:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error matching instrument: {str(e)}")
            return False

    def _get_fallback_instrument_id(self, trade_request: TradeRequest) -> int:
        """Get fallback instrument ID for F&O instruments"""
        # This is a basic fallback - in production, you should have a proper instrument master
        fallback_map = {
            "NIFTY": 26000,
            "BANKNIFTY": 26009,
            "FINNIFTY": 26037,
            "MIDCPNIFTY": 26014,
            "SENSEX": 26065,
            "BANKEX": 26118
        }
        
        instrument_id = fallback_map.get(trade_request.underlying_instrument.upper(), 26000)
        logger.warning(f"Using fallback instrument ID {instrument_id} for {trade_request.underlying_instrument}")
        return instrument_id

    def _get_cash_instrument_id_dynamic(self, client: IIFLConnect, instrument: str) -> int:
        """Get instrument ID for cash market instruments using dynamic search"""
        try:
            logger.info(f"Searching for cash market instrument: {instrument}")
            
            # Search for the instrument using IIFL's search API
            search_result = client.search_by_scriptname(instrument)
            
            if search_result.get("type") == "success":
                instruments = search_result.get("result", [])
                
                # Filter for equity stocks (series EQ) and cash market
                equity_stocks = []
                for inst in instruments:
                    if (inst.get("ExchangeSegment") == 1 and  # NSECM
                        inst.get("Series") == "EQ" and        # Equity series
                        inst.get("Name", "").upper() == instrument.upper()):
                        equity_stocks.append(inst)
                
                if equity_stocks:
                    # Use the first matching equity stock
                    instrument_id = equity_stocks[0].get("ExchangeInstrumentID")
                    if instrument_id:
                        logger.info(f"Found {instrument} with instrument ID: {instrument_id}")
                        return int(instrument_id)
                    else:
                        raise ValueError(f"Instrument ID not found for {instrument}")
                else:
                    # Try to find any instrument with similar name
                    for inst in instruments:
                        if (inst.get("ExchangeSegment") == 1 and  # NSECM
                            inst.get("Name", "").upper() == instrument.upper()):
                            instrument_id = inst.get("ExchangeInstrumentID")
                            if instrument_id:
                                logger.info(f"Found {instrument} (non-EQ) with instrument ID: {instrument_id}")
                                return int(instrument_id)
                    
                    raise ValueError(f"No equity stock found for {instrument}")
            else:
                raise ValueError(f"IIFL search failed for {instrument}: {search_result.get('description', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Failed to get dynamic instrument ID for {instrument}: {str(e)}")
            raise ValueError(f"Failed to find instrument '{instrument}': {str(e)}")

    def _prepare_order_parameters(self, trade_request: TradeRequest, instrument_details: Dict) -> Dict:
        """Prepare order parameters for IIFL API"""
        # Determine order type
        if trade_request.price is not None:
            order_type = "LIMIT"
            limit_price = trade_request.price
        else:
            order_type = "MARKET"
            limit_price = 0
        
        # Handle stop loss
        stop_price = trade_request.stop_loss_price or 0
        if stop_price > 0:
            order_type = "SL-M"  # Stop Loss Market
            limit_price = 0
        
        # Determine product type
        if self._is_futures_options_instrument(trade_request.underlying_instrument):
            product_type = "NRML"  # Normal for F&O
        else:
            product_type = "CNC"  # Cash and Carry for equities
        
        # Convert exchange segment to string format expected by IIFL API
        exchange_segment_map = {
            1: "NSECM",  # NSE Cash Market
            2: "BSECM",  # BSE Cash Market
            3: "NSEFO",  # NSE F&O
            4: "BSEFO"   # BSE F&O
        }
        
        exchange_segment = exchange_segment_map.get(
            instrument_details.get("exchangeSegment"), "NSECM"
        )
        
        return {
            "exchangeSegment": exchange_segment,  # Now a string like "NSECM"
            "exchangeInstrumentID": instrument_details["exchangeInstrumentID"],
            "productType": product_type,
            "orderType": order_type,
            "orderSide": "BUY" if trade_request.order_type == "BUY" else "SELL",
            "timeInForce": "DAY",
            "disclosedQuantity": 0,
            "orderQuantity": trade_request.quantity,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "orderUniqueIdentifier": f"ord_{int(datetime.now().timestamp())}"[-20:],  # Max 20 chars
            "apiOrderSource": "WebAPI"
        }

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
                xtsMessageCode=1512,  # LTP message code
                publishFormat="JSON"
            )
            
            # Parse LTP data
            ltp_data = {}
            if quote_result.get("type") == "success":
                quotes = quote_result["result"].get("listQuotes", [])
                for quote_str in quotes:
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

    def _get_exchange_segment(self, instrument: str) -> str:
        """Get exchange segment for instrument"""
        if instrument in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            return "NSEFO"  # NSE F&O for derivatives
        else:
            return "NSECM"  # NSE Cash Market for equities

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

    def get_market_client(self, user: User) -> IIFLConnect:
        """Get IIFL market data client for a user"""
        if not user.iifl_market_api_key or not user.iifl_market_secret_key:
            raise HTTPException(
                status_code=400,
                detail="IIFL market data credentials not configured"
            )
        return IIFLConnect(user, api_type="market")

    def get_interactive_client(self, user: User) -> IIFLConnect:
        """Get IIFL interactive trading client for a user"""
        if not user.iifl_interactive_api_key or not user.iifl_interactive_secret_key:
            raise HTTPException(
                status_code=400,
                detail="IIFL interactive trading credentials not configured"
            )
        return IIFLConnect(user, api_type="interactive")

    async def validate_credentials(
        self,
        user: User,
        market_api_key: Optional[str] = None,
        market_secret_key: Optional[str] = None,
        interactive_api_key: Optional[str] = None,
        interactive_secret_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate IIFL API credentials"""
        results = {
            "market_valid": False,
            "interactive_valid": False,
            "market_error": None,
            "interactive_error": None
        }

        # Test market data credentials
        if market_api_key and market_secret_key:
            try:
                temp_user = User(
                    iifl_market_api_key=encrypt_data(market_api_key),
                    iifl_market_secret_key=encrypt_data(market_secret_key)
                )
                client = IIFLConnect(temp_user, api_type="market")
                response = client.marketdata_login()
                logger.info(f"Market data login response: {response}")
                
                if response.get("type") == "success" and "result" in response and "token" in response["result"]:
                    results["market_valid"] = True
                else:
                    results["market_error"] = response.get("description", "Unknown error")
            except Exception as e:
                logger.error(f"Market data validation error: {str(e)}")
                results["market_error"] = str(e)

        # Test interactive trading credentials
        if interactive_api_key and interactive_secret_key:
            try:
                temp_user = User(
                    iifl_interactive_api_key=encrypt_data(interactive_api_key),
                    iifl_interactive_secret_key=encrypt_data(interactive_secret_key)
                )
                client = IIFLConnect(temp_user, api_type="interactive")
                response = client.interactive_login()
                logger.info(f"Interactive login response: {response}")
                
                if response.get("type") == "success" and "result" in response and "token" in response["result"]:
                    results["interactive_valid"] = True
                else:
                    results["interactive_error"] = response.get("description", "Unknown error")
            except Exception as e:
                logger.error(f"Interactive validation error: {str(e)}")
                results["interactive_error"] = str(e)

        return results

def get_iifl_service(db: Session = Depends(get_db)) -> IIFLService:
    """Dependency to get IIFL service instance"""
    return IIFLService(db)
