import json
import requests
from typing import Dict, List, Optional, Literal, Any
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session
from fastapi import HTTPException, Depends

from app.models.user import User
from app.schemas.trading import TradeRequest, MarketDataRequest
from app.core.security import decrypt_data, encrypt_data
from app.services.iifl_connect import IIFLConnect
from app.core.database import get_db
from app.core.iifl_session_manager import iifl_session_manager

class IIFLService:
    def __init__(self, db: Session):
        self.db = db
        self.base_url = "https://ttblaze.iifl.com"
        # Fixed: Use correct base URL paths that match iifl_connect.py routes
        self.interactive_api_base_url = f"{self.base_url}/interactive" 
        # Market data specific endpoints use this base
        self.market_data_api_base_url = f"{self.base_url}/apimarketdata"
        
        # Store active sessions to avoid repeated logins
        self._market_sessions = {}
        self._interactive_sessions = {}
    
    def _get_user_credentials(self, db: Session, user_id: int, credential_type: Literal["market", "interactive"]) -> Dict[str, str]:
        """Get decrypted IIFL credentials for user"""
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ValueError("User not found")
        
        if credential_type == "market":
            if not user.iifl_market_api_key:
                raise ValueError("IIFL Market credentials not found for user")
            return {
                "api_key": decrypt_data(user.iifl_market_api_key),
                "secret_key": decrypt_data(user.iifl_market_secret_key),
                "user_id": user.iifl_market_user_id
            }
        else:  # interactive
            if not user.iifl_interactive_api_key:
                raise ValueError("IIFL Interactive credentials not found for user")
            return {
                "api_key": decrypt_data(user.iifl_interactive_api_key),
                "secret_key": decrypt_data(user.iifl_interactive_secret_key),
                "user_id": user.iifl_interactive_user_id
            }
    
    def _create_session(self, credentials: Dict[str, str], session_type: Literal["market", "interactive"]) -> str:
        """Create IIFL session (either Market Data or Interactive) and return session token."""
        login_data = {
            "appKey": credentials["api_key"],
            "secretKey": credentials["secret_key"],
            "source": "WebAPI"
        }
        
        # Fixed: Use correct endpoint paths based on session type
        if session_type == "interactive":
            login_url = f"{self.interactive_api_base_url}/user/session"
        else:  # market
            login_url = f"{self.market_data_api_base_url}/auth/login"
        
        try:
            logger.info(f"Attempting {session_type} login to: {login_url}")
            response = requests.post(
                login_url,
                json=login_data,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get("type") == "success":
                token = result["result"]["token"]
                logger.info(f"IIFL {session_type.capitalize()} session created successfully")
                return token
            else:
                raise Exception(f"{session_type.capitalize()} login failed: {result.get('description', 'Unknown error')}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"IIFL {session_type.capitalize()} session creation failed for URL {login_url}: {e}")
            raise
    
    def _get_market_session(self, db: Session, user_id: int) -> str:
        """Get or create market session for user using session manager"""
        return iifl_session_manager.get_session_token(db, user_id, "market")
    
    def _get_interactive_session(self, db: Session, user_id: int) -> str:
        """Get or create interactive session for user using session manager"""
        return iifl_session_manager.get_session_token(db, user_id, "interactive")
    
    def place_order(self, db: Session, user_id: int, trade_request: TradeRequest) -> Dict:
        """Place order through IIFL Interactive API"""
        try:
            session_token = self._get_interactive_session(db, user_id)
            
            # Determine exchange segment and instrument ID
            exchange_segment = self._get_exchange_segment(trade_request.underlying_instrument)
            
            # IMPORTANT: This _get_instrument_id needs to be accurate for F&O contracts.
            # The previous version was returning SPOT IDs (e.g., 26000 for NIFTY spot)
            # which is incorrect for trading NIFTY F&O.
            # For a real application, you would need to use IIFL's Instrument Master API
            # to fetch the correct exchangeInstrumentID based on underlying, expiry, strike, and option_type.
            # For this example, we'll try to construct a more "options-like" lookup,
            # but it's still a placeholder.
            instrument_id = self._get_instrument_id(trade_request) # Still needs robust implementation
            
            # Determine order type and stop price based on trade_request
            order_type_iifl = "MARKET"
            limit_price = 0
            stop_price = 0
            
            if trade_request.price is not None:
                order_type_iifl = "LIMIT"
                limit_price = trade_request.price
            
            if trade_request.stop_loss_price is not None and trade_request.stop_loss_price > 0:
                # IIFL typically uses different order types for Stop Loss orders
                # For simplicity, if stop loss is provided, we'll assume it's a stop loss market order
                # You might need to confirm the exact orderType for SL-M with IIFL docs
                order_type_iifl = "SL-M" # Assuming "SL-M" for Stop Loss Market
                stop_price = trade_request.stop_loss_price
                limit_price = 0 # For SL-M, limit price is usually 0 or can be a very wide range
                                # if it's an SL-L (Stop Loss Limit) order.
                                # Check IIFL API docs for exact behavior.
                
            order_data = {
                "exchangeSegment": exchange_segment,
                "exchangeInstrumentID": instrument_id,
                "productType": "NRML", # Common for F&O. Could be "MIS" for Intraday. Verify with IIFL.
                "orderType": order_type_iifl,
                "orderSide": trade_request.order_type, # This will be BUY or SELL
                "timeInForce": "DAY", # DAY, GTC, IOC (Immediate or Cancel)
                "disclosedQuantity": 0,
                "orderQuantity": trade_request.quantity,
                "limitPrice": limit_price,
                "stopPrice": stop_price,
                "orderUniqueIdentifier": f"user_{user_id}_{int(datetime.now().timestamp())}"
            }
            
            logger.info(f"Attempting to place order with payload: {order_data}") # Log the payload
            
            headers = {"Authorization": f"Bearer {session_token}"}
            
            response = requests.post(
                f"{self.interactive_api_base_url}/orders", # Use the correct interactive_api_base_url
                json=order_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                logger.warning(f"Session expired for user {user_id}. Attempting to refresh token and retry.")
                # Session expired, refresh and retry
                self._interactive_sessions.pop(user_id, None)
                session_token = self._get_interactive_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                
                # Retry the request
                response = requests.post(
                    f"{self.interactive_api_base_url}/orders", # Use the correct interactive_api_base_url
                    json=order_data,
                    headers=headers,
                    timeout=30
                )
            
            # Check response status again after potential retry
            response.raise_for_status() 
            result = response.json()
            logger.info(f"Order placed for user {user_id}: {result}")
            
            return result
            
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error placing order for user {user_id}: {http_err}. Response: {http_err.response.text}")
            raise
        except Exception as e:
            logger.error(f"Order placement failed for user {user_id}: {e}")
            raise
    
    def get_order_book(self, db: Session, user_id: int) -> Dict:
        """Get order book from IIFL Interactive API"""
        try:
            session_token = self._get_interactive_session(db, user_id)
            headers = {"Authorization": f"Bearer {session_token}"}
            
            response = requests.get(
                f"{self.interactive_api_base_url}/orders", # Use the correct interactive_api_base_url
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                # Session expired, refresh and retry
                self._interactive_sessions.pop(user_id, None)
                session_token = self._get_interactive_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                response = requests.get(
                    f"{self.interactive_api_base_url}/orders", # Use the correct interactive_api_base_url
                    headers=headers,
                    timeout=30
                )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Order book fetch failed for user {user_id}: {e}")
            raise
    
    def get_positions(self, db: Session, user_id: int) -> Dict:
        """Get positions from IIFL Interactive API"""
        try:
            session_token = self._get_interactive_session(db, user_id)
            headers = {"Authorization": f"Bearer {session_token}"}
            
            response = requests.get(
                f"{self.interactive_api_base_url}/portfolio/positions", # Use the correct interactive_api_base_url
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                # Session expired, refresh and retry
                self._interactive_sessions.pop(user_id, None)
                session_token = self._get_interactive_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                response = requests.get(
                    f"{self.interactive_api_base_url}/portfolio/positions", # Use the correct interactive_api_base_url
                    headers=headers,
                    timeout=30
                )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Positions fetch failed for user {user_id}: {e}")
            raise

    def get_balance(self, db: Session, user_id: int) -> Dict:
        """Get account balance from IIFL Interactive API"""
        try:
            # Use IIFLConnect class for proper API handling
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                raise ValueError("User not found")
            
            client = IIFLConnect(user, api_type="interactive")
            
            # Login to get session
            login_response = client.interactive_login()
            if login_response.get("type") != "success":
                raise Exception(f"IIFL login failed: {login_response.get('description', 'Unknown error')}")
            
            # Get balance using the proper IIFLConnect method
            balance_result = client.get_balance()
            
            # Add debugging for balance values
            logger.info(f"Raw balance result: {balance_result}")
            if balance_result.get("type") == "success" and "result" in balance_result:
                balance_list = balance_result["result"].get("BalanceList", [])
                logger.info(f"Balance List: {balance_list}")
                
                for balance_item in balance_list:
                    limit_object = balance_item.get("limitObject", {})
                    rms_sub_limits = limit_object.get("RMSSubLimits", {})
                    margin_available = limit_object.get("marginAvailable", {})
                    
                    logger.info(f"RMS Sub Limits: {rms_sub_limits}")
                    logger.info(f"Margin Available: {margin_available}")
                    
                    # Check specific balance fields
                    cash_available = rms_sub_limits.get("cashAvailable")
                    net_margin_available = rms_sub_limits.get("netMarginAvailable")
                    cash_margin_available = margin_available.get("CashMarginAvailable")
                    
                    logger.info(f"Cash Available: {cash_available} (type: {type(cash_available)})")
                    logger.info(f"Net Margin Available: {net_margin_available} (type: {type(net_margin_available)})")
                    logger.info(f"Cash Margin Available: {cash_margin_available} (type: {type(cash_margin_available)})")
            
            # Logout to clean up session
            try:
                client.interactive_logout()
            except:
                pass  # Ignore logout errors
            
            return balance_result
            
        except Exception as e:
            logger.error(f"Balance fetch failed for user {user_id}: {e}")
            raise
    
    def get_market_data(self, db: Session, user_id: int, instruments: List[str]) -> Dict:
        """Get market data for instruments using Market Data API"""
        try:
            session_token = self._get_market_session(db, user_id)
            
            # Convert instrument symbols to IIFL format
            instrument_list = []
            for symbol in instruments:
                exchange_segment = self._get_exchange_segment(symbol)
                instrument_id = self._get_instrument_id_by_symbol(symbol) # This function will need improvement for F&O
                instrument_list.append({
                    "exchangeSegment": exchange_segment,
                    "exchangeInstrumentID": instrument_id
                })
            
            headers = {"Authorization": f"Bearer {session_token}"}
            
            response = requests.post(
                f"{self.market_data_api_base_url}/instruments/quotes", # Use market_data_api_base_url for market data
                json={"instruments": instrument_list},
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                # Session expired, refresh and retry
                self._market_sessions.pop(user_id, None)
                session_token = self._get_market_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                response = requests.post(
                    f"{self.market_data_api_base_url}/instruments/quotes", # Use market_data_api_base_url for market data
                    json={"instruments": instrument_list},
                    headers=headers,
                    timeout=30
                )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Market data fetch failed for user {user_id}: {e}")
            raise
    
    def get_ltp(self, db: Session, user_id: int, instruments: List[Dict]) -> Dict:
        """Get Last Traded Price for instruments"""
        try:
            session_token = self._get_market_session(db, user_id)
            headers = {"Authorization": f"Bearer {session_token}"}
            
            # Convert instruments list to proper format
            instrument_request = {
                "instruments": instruments,
                "xtsMessageCode":1512,  # LTP message code
                "publishFormat": "JSON"
            }
            
            response = requests.post(
                f"{self.market_data_api_base_url}/instruments/quotes",
                json=instrument_request,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                # Session expired, refresh and retry
                self._market_sessions.pop(user_id, None)
                session_token = self._get_market_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                
                response = requests.post(
                    f"{self.market_data_api_base_url}/instruments/quotes",
                    json=instrument_request,
                    headers=headers,
                    timeout=30
                )
            
            response.raise_for_status()
            result = response.json()
            
            # Parse LTP data
            ltp_data = {}
            if result.get("type") == "success":
                quotes = result["result"].get("listQuotes", [])
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
    
    def cancel_order(self, db: Session, user_id: int, order_id: str) -> Dict:
        """Cancel an order through IIFL Interactive API"""
        try:
            session_token = self._get_interactive_session(db, user_id)
            headers = {"Authorization": f"Bearer {session_token}"}
            
            # Cancel order data
            cancel_data = {
                "appOrderID": order_id,
                "orderUniqueIdentifier": f"cancel_{order_id}_{int(datetime.now().timestamp())}"
            }
            
            response = requests.delete(
                f"{self.interactive_api_base_url}/orders",
                json=cancel_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                # Session expired, refresh and retry
                self._interactive_sessions.pop(user_id, None)
                session_token = self._get_interactive_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                
                response = requests.delete(
                    f"{self.interactive_api_base_url}/orders",
                    json=cancel_data,
                    headers=headers,
                    timeout=30
                )
            
            response.raise_for_status()
            result = response.json()
            logger.info(f"Order cancelled for user {user_id}: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Order cancellation failed for user {user_id}: {e}")
            raise

    def modify_order(self, db: Session, user_id: int, order_id: str, modification: TradeRequest) -> Dict:
        """Modify an order through IIFL Interactive API"""
        try:
            session_token = self._get_interactive_session(db, user_id)
            headers = {"Authorization": f"Bearer {session_token}"}
            
            # Determine order type and prices
            order_type_iifl = "MARKET"
            limit_price = 0
            stop_price = 0
            
            if modification.price is not None:
                order_type_iifl = "LIMIT"
                limit_price = modification.price
            
            if modification.stop_loss_price is not None and modification.stop_loss_price > 0:
                order_type_iifl = "SL-M"
                stop_price = modification.stop_loss_price
                limit_price = 0
            
            # Modify order data
            modify_data = {
                "appOrderID": order_id,
                "modifiedProductType": "NRML",
                "modifiedOrderType": order_type_iifl,
                "modifiedOrderQuantity": modification.quantity,
                "modifiedDisclosedQuantity": 0,
                "modifiedLimitPrice": limit_price,
                "modifiedStopPrice": stop_price,
                "modifiedTimeInForce": "DAY",
                "orderUniqueIdentifier": f"modify_{order_id}_{int(datetime.now().timestamp())}"
            }
            
            response = requests.put(
                f"{self.interactive_api_base_url}/orders",
                json=modify_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 401:
                # Session expired, refresh and retry
                self._interactive_sessions.pop(user_id, None)
                session_token = self._get_interactive_session(db, user_id)
                headers = {"Authorization": f"Bearer {session_token}"}
                
                response = requests.put(
                    f"{self.interactive_api_base_url}/orders",
                    json=modify_data,
                    headers=headers,
                    timeout=30
                )
            
            response.raise_for_status()
            result = response.json()
            logger.info(f"Order modified for user {user_id}: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Order modification failed for user {user_id}: {e}")
            raise
    
    def _get_exchange_segment(self, instrument: str) -> str:
        """Map instrument to exchange segment"""
        # NSECM = NSE Cash, NSEFO = NSE F&O, BSECM = BSE Cash, BSEFO = BSE F&O, MCXFO = MCX F&O
        if instrument in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            return "NSEFO"  # NSE F&O for derivatives
        else:
            return "NSECM"  # NSE Cash Market (default)
    
    def _get_instrument_id(self, trade_request: TradeRequest) -> int:
        """
        Get instrument ID from trade request.
        
        IMPORTANT: This is a placeholder. For actual F&O trading, you need
        to dynamically fetch the correct instrument ID based on:
        - underlying_instrument (e.g., "NIFTY")
        - option_type (e.g., "CALL", "PUT")
        - strike_price
        - expiry_date
        
        IIFL's API will have a way to query instrument master data.
        For now, we'll return hardcoded IDs based on some assumptions for common use cases.
        This will likely still lead to 400 errors for specific F&O orders unless these IDs are valid.
        """
        base_id_map = {
            "NIFTY": 26000,      # Placeholder/Spot ID
            "BANKNIFTY": 26001,  # Placeholder/Spot ID
            "FINNIFTY": 26034,   # Placeholder/Spot ID
            "MIDCPNIFTY": 26121  # Placeholder/Spot ID
        }
        
        underlying = trade_request.underlying_instrument
        option_type = trade_request.option_type
        strike_price = trade_request.strike_price
        expiry_date = trade_request.expiry_date

        if underlying in base_id_map: # Check if it's an index/stock that might have F&O
            if self._get_exchange_segment(underlying) == "NSEFO": # It's an F&O segment based on the check
                # For F&O instruments, you MUST query IIFL's Instrument Master.
                # Hardcoding will almost certainly fail or lead to incorrect trades.
                logger.warning(
                    f"Attempting to get F&O instrument ID for {underlying} {option_type} {strike_price} {expiry_date}. "
                    "This requires an IIFL Instrument Master API lookup. Returning a placeholder ID (will likely fail order placement)."
                )
                # You'd typically call an IIFL API endpoint here to get the real ID.
                # Example (conceptual):
                # instrument_master_url = f"{self.market_data_api_base_url}/instruments/master"
                # params = {
                #     "symbol": underlying,
                #     "segment": "NFO", # or the correct segment for F&O
                #     "optionType": option_type,
                #     "strikePrice": strike_price,
                #     "expiryDate": expiry_date.strftime("%Y%m%d") # Format as required by IIFL
                # }
                # response = requests.get(instrument_master_url, params=params, headers={"Authorization": f"Bearer {session_token}"})
                # if response.status_code == 200:
                #    instrument_data = response.json()
                #    return instrument_data['exchangeInstrumentID'] # or similar field
                
                # For now, return a placeholder that will likely lead to a 400 or other error for F&O orders
                # because it's a spot ID (26000 for NIFTY spot, etc.).
                return base_id_map.get(underlying, 26000) # This is the critical point.
            else: # It's a cash segment instrument
                return base_id_map.get(underlying, 26000) # For cash, the spot ID might be correct.
        
        # Fallback for symbols not in base_id_map or if not F&O specific
        return base_id_map.get(underlying, 26000)

    def _get_instrument_id_by_symbol(self, symbol: str) -> int:
        """Get instrument ID by symbol (used for market data, less strict than trading)"""
        # Based on the session manager code. These are generally spot/cash IDs.
        symbol_map = {
            "NIFTY": 26000,
            "BANKNIFTY": 26001,
            "FINNIFTY": 26034,
            "MIDCPNIFTY": 26121,
            "SENSEX": 26065,
            "BANKEX": 26118
        }
        # For market data, these spot IDs are often fine for indices.
        # For specific F&O quotes, you'd still need to provide the exact F&O instrument ID.
        return symbol_map.get(symbol, 26000)

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
        """Update user's IIFL API credentials"""
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

        self.db.commit()
        self.db.refresh(user)
        return user

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
    return IIFLService(db)