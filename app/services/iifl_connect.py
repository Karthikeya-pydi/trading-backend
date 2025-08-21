"""
    IIFL Connect API wrapper for the trading platform.

    This module provides a wrapper for the IIFL Connect REST APIs, integrated with FastAPI.
    The implementation is based on the official IIFL BLAZE INTERACTIVE API documentation v2.0
    and IIFL BLAZE BINARY MARKETDATA API documentation.

    Key Features:
    - Interactive API: Order placement, modification, cancellation, portfolio management
    - Binary Market Data API: Real-time quotes, instrument search, master data, OHLC
    - WebSocket Support: Real-time streaming for orders, trades, positions
    - Binary Data Streaming: Socket.IO support for real-time market data
    - Authentication: Token-based authentication with automatic session management
    - Error Handling: Comprehensive error handling with detailed logging

    API Documentation Reference:
    - Live Server: https://ttblaze.iifl.com/
    - Sandbox Server: https://developers.symphonyfintech.in/
    - Interactive API Docs: https://ttblaze.iifl.com/doc/interactive/
    - Binary Market Data API Docs: https://ttblaze.iifl.com/apibinarymarketdata/

    :copyright:
    :license: see LICENSE for details.
"""
import configparser
import json
import logging
import requests
from urllib import parse
from typing import Optional, Dict, Any, List
from fastapi import HTTPException
from app.core.config import settings
from app.models.user import User
from app.core.security import decrypt_data

log = logging.getLogger(__name__)

class IIFLCommon:
    """
    Base variables class
    """
    def __init__(self, token=None, userID=None, isInvestorClient=None):
        """Initialize the common variables."""
        self.token = token
        self.userID = userID
        self.isInvestorClient = isInvestorClient

class IIFLConnect(IIFLCommon):
    """
    The IIFL Connect API wrapper class.
    In production, you may initialise a single instance of this class per user.
    """
    # Default root API endpoint
    _default_root_uri = settings.IIFL_ROOT_URI
    _default_login_uri = _default_root_uri + "/user/session"
    _default_timeout = settings.IIFL_TIMEOUT  # In seconds

    # SSL Flag
    _ssl_flag = settings.IIFL_DISABLE_SSL

    # Constants
    # Products
    PRODUCT_MIS = "MIS"
    PRODUCT_NRML = "NRML"
    PRODUCT_CNC = "CNC"
    PRODUCT_CO = "CO"

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_STOPMARKET = "STOPMARKET"
    ORDER_TYPE_STOPLIMIT = "STOPLIMIT"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Time in Force
    TIME_IN_FORCE_DAY = "DAY"
    TIME_IN_FORCE_IOC = "IOC"
    TIME_IN_FORCE_GTC = "GTC"
    TIME_IN_FORCE_GTD = "GTD"
    TIME_IN_FORCE_EOS = "EOS"

    # Order Source
    ORDER_SOURCE_WEBAPI = "WEBAPI"
    ORDER_SOURCE_TWSAPI = "TWSAPI"
    ORDER_SOURCE_MOBILE_ANDROID_API = "MobileAndroidAPI"
    ORDER_SOURCE_MOBILE_WINDOWS_API = "MobileWindowsAPI"
    ORDER_SOURCE_MOBILE_IOS_API = "MobileIOSAPI"

    # Exchange Segments
    EXCHANGE_NSECM = "NSECM"
    EXCHANGE_NSEFO = "NSEFO"
    EXCHANGE_NSECD = "NSECD"
    EXCHANGE_MCXFO = "MCXFO"
    EXCHANGE_BSECM = "BSECM"
    EXCHANGE_BSEFO = "BSEFO"

    # Position Square Off Mode
    SQUARE_OFF_MODE_DAYWISE = "DayWise"
    SQUARE_OFF_MODE_NETWISE = "NetWise"

    # Position Square Off Quantity Type
    SQUARE_OFF_QTY_TYPE_PERCENTAGE = "Percentage"
    SQUARE_OFF_QTY_TYPE_EXACT_QTY = "ExactQty"

    # Day or Net
    DAY_OR_NET_DAY = "DAY"
    DAY_OR_NET_NET = "NET"

    # Binary Market Data API Constants
    # Message Codes
    MESSAGE_CODE_TOUCHLINE = 1501
    MESSAGE_CODE_MARKET_DEPTH = 1502
    MESSAGE_CODE_INDEX_DATA = 1504
    MESSAGE_CODE_CANDLE_DATA = 1505
    MESSAGE_CODE_OPEN_INTEREST = 1510
    MESSAGE_CODE_LTP_DATA = 1512

    # Publish Formats
    PUBLISH_FORMAT_BINARY = "Binary"
    PUBLISH_FORMAT_JSON = "JSON"

    # Broadcast Modes
    BROADCAST_MODE_FULL = "Full"
    BROADCAST_MODE_PARTIAL = "Partial"

    # Compression Values for OHLC
    COMPRESSION_1_SECOND = "1"
    COMPRESSION_1_MINUTE = "60"
    COMPRESSION_2_MINUTE = "120"
    COMPRESSION_3_MINUTE = "180"
    COMPRESSION_5_MINUTE = "300"
    COMPRESSION_10_MINUTE = "600"
    COMPRESSION_15_MINUTE = "900"
    COMPRESSION_30_MINUTE = "1800"
    COMPRESSION_60_MINUTE = "3600"
    COMPRESSION_DAILY = "D"

    # Exchange Segment IDs (as per Binary Market Data API)
    EXCHANGE_SEGMENT_NSECM = 1
    EXCHANGE_SEGMENT_NSEFO = 2
    EXCHANGE_SEGMENT_NSECD = 3
    EXCHANGE_SEGMENT_BSECM = 11
    EXCHANGE_SEGMENT_BSEFO = 12
    EXCHANGE_SEGMENT_BSECD = 13
    EXCHANGE_SEGMENT_MCXFO = 51

    # Instrument Types
    INSTRUMENT_TYPE_FUTURES = 1
    INSTRUMENT_TYPE_OPTIONS = 2
    INSTRUMENT_TYPE_SPREAD = 4
    INSTRUMENT_TYPE_EQUITY = 8
    INSTRUMENT_TYPE_SPOT = 16
    INSTRUMENT_TYPE_PREFERENCE_SHARES = 32
    INSTRUMENT_TYPE_DEBENTURES = 64
    INSTRUMENT_TYPE_WARRANTS = 128
    INSTRUMENT_TYPE_MISCELLANEOUS = 256
    INSTRUMENT_TYPE_MUTUAL_FUND = 512

    # URIs to various calls
    _routes = {
        # Interactive API endpoints
        "interactive.prefix": "interactive",
        "user.login": "/interactive/user/session",
        "user.logout": "/interactive/user/session",
        "user.profile": "/interactive/user/profile",
        "user.balance": "/interactive/user/balance",
        "orders": "/interactive/orders",
        "trades": "/interactive/orders/trades",
        "order.status": "/interactive/orders",
        "order.place": "/interactive/orders",
        "order.modify": "/interactive/orders",
        "order.cancel": "/interactive/orders",
        "order.cancelall": "/interactive/orders/cancelall",
        "order.history": "/interactive/orders",
        "portfolio.positions": "/interactive/portfolio/positions",
        "portfolio.holdings": "/interactive/portfolio/holdings",
        "portfolio.positions.convert": "/interactive/portfolio/positions/convert",
        "portfolio.squareoff": "/interactive/portfolio/positions/squareoff",
        "status.exchange": "/interactive/status/exchange",
        "messages.exchange": "/interactive/messages/exchange",

        # Binary Market Data API endpoints (updated to match documentation)
        "marketdata.prefix": "apibinarymarketdata",
        "market.login": "/apibinarymarketdata/auth/login",
        "market.logout": "/apibinarymarketdata/auth/logout",
        "market.config": "/apibinarymarketdata/config/clientConfig",
        "market.instruments.master": "/apibinarymarketdata/instruments/master",
        "market.instruments.quotes": "/apibinarymarketdata/instruments/quotes",
        "market.instruments.subscription": "/apibinarymarketdata/instruments/subscription",
        "market.instruments.unsubscription": "/apibinarymarketdata/instruments/subscription",
        "market.instruments.ohlc": "/apibinarymarketdata/instruments/ohlc",
        "market.instruments.instrument.series": "/apibinarymarketdata/instruments/instrument/series",
        "market.instruments.instrument.equitysymbol": "/apibinarymarketdata/instruments/instrument/symbol",
        "market.instruments.instrument.expirydate": "/apibinarymarketdata/instruments/instrument/expiryDate",
        "market.instruments.instrument.futuresymbol": "/apibinarymarketdata/instruments/instrument/futureSymbol",
        "market.instruments.instrument.optionsymbol": "/apibinarymarketdata/instruments/instrument/optionSymbol",
        "market.instruments.instrument.strikeprice": "/apibinarymarketdata/instruments/instrument/strikePrice",
        "market.instruments.instrument.optiontype": "/apibinarymarketdata/instruments/instrument/optionType",
        "market.instruments.indexlist": "/apibinarymarketdata/instruments/indexlist",
        "market.search.instrumentsbystring": "/apibinarymarketdata/search/instruments",
        "market.search.instrumentsbyid": "/apibinarymarketdata/search/instrumentsbyid",
    }

    def __init__(self, user: User, api_type: str = "interactive"):
        """
        Initialize IIFL Connect client for a specific user.
        
        Args:
            user: User model instance containing API credentials
            api_type: Either "interactive" or "market" to specify which API to use
        """
        # Initialize base class with None values
        super().__init__(None, None, None)
        
        self.user = user
        self.api_type = api_type
        
        # Get appropriate credentials based on API type
        if api_type == "interactive":
            self.apiKey = decrypt_data(user.iifl_interactive_api_key)
            self.secretKey = decrypt_data(user.iifl_interactive_secret_key)
            self.userID = user.iifl_interactive_user_id
        else:  # market
            self.apiKey = decrypt_data(user.iifl_market_api_key)
            self.secretKey = decrypt_data(user.iifl_market_secret_key)
            self.userID = user.iifl_market_user_id

        self.source = "WEBAPI"
        self.disable_ssl = self._ssl_flag
        self.root = self._default_root_uri
        self.timeout = self._default_timeout
        self.reqsession = requests.Session()
        
        # disable requests SSL warning
        requests.packages.urllib3.disable_warnings()

    def _set_common_variables(self, access_token, userID, isInvestorClient):
        """Set the `access_token` received after a successful authentication."""
        self.token = access_token
        self.userID = userID
        self.isInvestorClient = isInvestorClient

    def interactive_login(self):
        """Login to IIFL Interactive API"""
        try:
            params = {
                "appKey": self.apiKey,
                "secretKey": self.secretKey,
                "source": self.source
            }
            log.info(f"Attempting interactive login with params: {params}")
            response = self._post("user.login", params)
            log.info(f"Interactive login response: {response}")

            if response.get("type") != "success":
                error_msg = response.get("description", "Unknown error")
                log.error(f"Interactive login failed: {error_msg}")
                raise HTTPException(status_code=401, detail=f"IIFL Interactive login failed: {error_msg}")

            if "result" in response and "token" in response["result"]:
                self._set_common_variables(
                    response["result"]["token"],
                    response["result"]["userID"],
                    response["result"]["isInvestorClient"]
                )
                return response
            else:
                error_msg = "Token not found in response"
                log.error(f"Interactive login failed: {error_msg}")
                raise HTTPException(status_code=401, detail=f"IIFL Interactive login failed: {error_msg}")

        except Exception as e:
            log.error(f"Interactive login error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"IIFL Interactive login failed: {str(e)}")

    def marketdata_login(self):
        """Login to IIFL Market Data API"""
        try:
            params = {
                "appKey": self.apiKey,
                "secretKey": self.secretKey,
                "source": self.source
            }
            log.info(f"Attempting market data login with params: {params}")
            response = self._post("market.login", params)
            log.info(f"Market data login response: {response}")

            if response.get("type") != "success":
                error_msg = response.get("description", "Unknown error")
                log.error(f"Market data login failed: {error_msg}")
                raise HTTPException(status_code=401, detail=f"IIFL Market Data login failed: {error_msg}")

            if "result" in response and "token" in response["result"]:
                self._set_common_variables(
                    response["result"]["token"],
                    response["result"]["userID"],
                    False
                )
                return response
            else:
                error_msg = "Token not found in response"
                log.error(f"Market data login failed: {error_msg}")
                raise HTTPException(status_code=401, detail=f"IIFL Market Data login failed: {error_msg}")

        except Exception as e:
            log.error(f"Market data login error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"IIFL Market Data login failed: {str(e)}")

    def get_order_book(self, clientID=None):
        """Request Order book gives states of all the orders placed by an user"""
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get("order.status", params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}
		
    def get_dealer_orderbook(self, clientID=None):
        """Request Order book gives states of all the orders placed by an user"""
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get("order.dealer.status", params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}


    def place_order(self,
                    exchangeSegment,
                    exchangeInstrumentID,
                    productType,
                    orderType,
                    orderSide,
                    timeInForce,
                    disclosedQuantity,
                    orderQuantity,
                    limitPrice,
                    stopPrice,
                    orderUniqueIdentifier,
                    apiOrderSource,
                    clientID=None
                    ):
        """To place an order"""
        try:

            params = {
                "exchangeSegment": exchangeSegment,
                "exchangeInstrumentID": exchangeInstrumentID,
                "productType": productType,
                "orderType": orderType,
                "orderSide": orderSide,
                "timeInForce": timeInForce,
                "disclosedQuantity": disclosedQuantity,
                "orderQuantity": orderQuantity,
                "limitPrice": limitPrice,
                "stopPrice": stopPrice,
                "apiOrderSource":apiOrderSource,
                "orderUniqueIdentifier": orderUniqueIdentifier
            }

            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._post('order.place', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def modify_order(self,
                     appOrderID,
                     modifiedProductType,
                     modifiedOrderType,
                     modifiedOrderQuantity,
                     modifiedDisclosedQuantity,
                     modifiedLimitPrice,
                     modifiedStopPrice,
                     modifiedTimeInForce,
                     orderUniqueIdentifier,
                     clientID=None
                     ):
        """The facility to modify your open orders by allowing you to change limit order to market or vice versa,
        change Price or Quantity of the limit open order, change disclosed quantity or stop-loss of any
        open stop loss order. """
        try:
            appOrderID = int(appOrderID)
            params = {
                'appOrderID': appOrderID,
                'modifiedProductType': modifiedProductType,
                'modifiedOrderType': modifiedOrderType,
                'modifiedOrderQuantity': modifiedOrderQuantity,
                'modifiedDisclosedQuantity': modifiedDisclosedQuantity,
                'modifiedLimitPrice': modifiedLimitPrice,
                'modifiedStopPrice': modifiedStopPrice,
                'modifiedTimeInForce': modifiedTimeInForce,
                'orderUniqueIdentifier': orderUniqueIdentifier
            }

            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._put('order.modify', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}


        
    def place_bracketorder(self,
                    exchangeSegment,
                    exchangeInstrumentID,
                    orderType,
                    orderSide,
                    disclosedQuantity,
                    orderQuantity,
                    limitPrice,
                    squarOff,
                    stopLossPrice,
	                trailingStoploss,
                    isProOrder,
                    apiOrderSource,
                    orderUniqueIdentifier,
                     ):
        """To place a bracketorder"""
        try:

            params = {
                "exchangeSegment": exchangeSegment,
                "exchangeInstrumentID": exchangeInstrumentID,
                "orderType": orderType,
                "orderSide": orderSide,
                "disclosedQuantity": disclosedQuantity,
                "orderQuantity": orderQuantity,
                "limitPrice": limitPrice,
                "squarOff": squarOff,
                "stopLossPrice": stopLossPrice,
                "trailingStoploss": trailingStoploss,
                "isProOrder": isProOrder,
                "apiOrderSource":apiOrderSource,
                "orderUniqueIdentifier": orderUniqueIdentifier
            }
            response = self._post('bracketorder.place', params)
            print(response)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def bracketorder_cancel(self, appOrderID, clientID=None):
        """This API can be called to cancel any open order of the user by providing correct appOrderID matching with
        the chosen open order to cancel. """
        try:
            params = {'boEntryOrderId': int(appOrderID)}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._delete('bracketorder.cancel', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}   

    def modify_bracketorder(self,
                     appOrderID,
                     orderQuantity,
                     limitPrice,
                     stopPrice,
                     clientID=None
                     ):
        try:
            appOrderID = int(appOrderID)
            params = {
                'appOrderID': appOrderID,
                'bracketorder.modify': orderQuantity,
                'limitPrice': limitPrice,
                'stopPrice': stopPrice
            }

            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._put('bracketorder.modify', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}


    def place_cover_order(self, 
                          exchangeSegment, 
                          exchangeInstrumentID, 
                          orderSide,orderType, 
                          orderQuantity, 
                          disclosedQuantity,
                          limitPrice, 
                          stopPrice, 
                          apiOrderSource,
                          orderUniqueIdentifier, 
                          clientID=None):
        """A Cover Order is an advance intraday order that is accompanied by a compulsory Stop Loss Order. This helps
        users to minimize their losses by safeguarding themselves from unexpected market movements. A Cover Order
        offers high leverage and is available in Equity Cash, Equity F&O, Commodity F&O and Currency F&O segments. It
        has 2 orders embedded in itself, they are Limit/Market Order Stop Loss Order """
        try:

            params = {'exchangeSegment': exchangeSegment, 
                      'exchangeInstrumentID': exchangeInstrumentID,
                      'orderSide': orderSide, 
                      "orderType": orderType,
                      'orderQuantity': orderQuantity, 
                      'disclosedQuantity': disclosedQuantity,
                      'limitPrice': limitPrice, 
                      'stopPrice': stopPrice, 
                      'apiOrderSource': apiOrderSource,
                      'orderUniqueIdentifier': orderUniqueIdentifier
                      }
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._post('order.place.cover', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def exit_cover_order(self, appOrderID, clientID=None):
        """Exit Cover API is a functionality to enable user to easily exit an open stoploss order by converting it
        into Exit order. """
        try:

            params = {'appOrderID': appOrderID}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._put('order.exit.cover', json.dumps(params))
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}



    def get_profile(self, clientID=None):
        """Using session token user can access his profile stored with the broker, it's possible to retrieve it any
        point of time with the http: //ip:port/interactive/user/profile API. """
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._get('user.profile', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_balance(self, clientID=None):
        """Get Balance API call grouped under this category information related to limits on equities, derivative,
        upfront margin, available exposure and other RMS related balances available to the user."""
        if self.isInvestorClient:
            try:
                params = {}
                if not self.isInvestorClient:
                    params['clientID'] = clientID
                response = self._get('user.balance', params)
                return response
            except Exception as e:
                return {"type": "error", "description": str(e)}
        else:
            print("Balance : Balance API available for retail API users only, dealers can watch the same on dealer "
                  "terminal")


    def get_trade(self, clientID=None):
        """Trade book returns a list of all trades executed on a particular day , that were placed by the user . The
        trade book will display all filled and partially filled orders. """
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get('trades', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_dealer_tradebook(self, clientID=None):
        """Trade book returns a list of all trades executed on a particular day , that were placed by the user . The
        trade book will display all filled and partially filled orders. """
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get('dealer.trades', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}
		
    def get_holding(self, clientID=None):
        """Holdings API call enable users to check their long term holdings with the broker."""
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._get('portfolio.holdings', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}


    def get_dealerposition_netwise(self, clientID=None):
        """The positions API positions by net. Net is the actual, current net position portfolio."""
        try:
            params = {'dayOrNet': 'NetWise'}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get('portfolio.dealerpositions', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}


           
    def get_dealerposition_daywise(self, clientID=None):
        """The positions API returns positions by day, which is a snapshot of the buying and selling activity for
        that particular day."""
        try:
            params = {'dayOrNet': 'DayWise'}
            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._get('portfolio.dealerpositions', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}
		
    def get_position_daywise(self, clientID=None):
	    
        """The positions API returns positions by day, which is a snapshot of the buying and selling activity for
        that particular day."""
        try:
            params = {'dayOrNet': 'DayWise'}
            if not self.isInvestorClient:
                params['clientID'] = clientID

            response = self._get('portfolio.positions', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_position_netwise(self, clientID=None):
        """The positions API positions by net. Net is the actual, current net position portfolio."""
        try:
            params = {'dayOrNet': 'NetWise'}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get('portfolio.positions', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def convert_position(self, exchangeSegment, exchangeInstrumentID, targetQty, isDayWise, oldProductType,
                         newProductType, clientID=None):
        """Convert position API, enable users to convert their open positions from NRML intra-day to Short term MIS or
        vice versa, provided that there is sufficient margin or funds in the account to effect such conversion """
        try:
            params = {
                'exchangeSegment': exchangeSegment,
                'exchangeInstrumentID': exchangeInstrumentID,
                'targetQty': targetQty,
                'isDayWise': isDayWise,
                'oldProductType': oldProductType,
                'newProductType': newProductType
            }
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._put('portfolio.positions.convert', json.dumps(params))
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def convert_position_enhanced(self, exchangeSegment, exchangeInstrumentID, targetQty, isDayWise, 
                                 oldProductType, newProductType, statisticsLevel="ParentLevel", 
                                 isInterOpPosition=False, clientID=None):
        """Enhanced position conversion with additional parameters as per IIFL API documentation"""
        try:
            params = {
                'exchangeSegment': exchangeSegment,
                'exchangeInstrumentID': exchangeInstrumentID,
                'targetQty': targetQty,
                'isDayWise': isDayWise,
                'oldProductType': oldProductType,
                'newProductType': newProductType,
                'statisticsLevel': statisticsLevel,
                'isInterOpPosition': isInterOpPosition
            }
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._put('portfolio.positions.convert', json.dumps(params))
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def cancel_order(self, appOrderID, orderUniqueIdentifier, clientID=None):
        """This API can be called to cancel any open order of the user by providing correct appOrderID matching with
        the chosen open order to cancel. """
        try:
            params = {'appOrderID': int(appOrderID), 'orderUniqueIdentifier': orderUniqueIdentifier}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._delete('order.cancel', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}
        
    def cancelall_order(self, exchangeSegment, exchangeInstrumentID):
        """This API can be called to cancel all open order of the user by providing exchange segment and exchange instrument ID """
        try:
            params = {"exchangeSegment": exchangeSegment, "exchangeInstrumentID": exchangeInstrumentID}
            if not self.isInvestorClient:
                params['clientID'] = self.userID
            response = self._post('order.cancelall', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def cancel_all_orders_by_segment(self, exchangeSegment):
        """Cancel all open orders for a specific exchange segment"""
        try:
            params = {"exchangeSegment": exchangeSegment, "exchangeInstrumentID": 0}
            if not self.isInvestorClient:
                params['clientID'] = self.userID
            response = self._post('order.cancelall', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}    


    def squareoff_position(self, exchangeSegment, exchangeInstrumentID, productType, squareoffMode,
                           positionSquareOffQuantityType, squareOffQtyValue, blockOrderSending, cancelOrders,
                           clientID=None):
        """User can request square off to close all his positions in Equities, Futures and Option. Users are advised
        to use this request with caution if one has short term holdings. """
        try:

            params = {'exchangeSegment': exchangeSegment, 'exchangeInstrumentID': exchangeInstrumentID,
                      'productType': productType, 'squareoffMode': squareoffMode,
                      'positionSquareOffQuantityType': positionSquareOffQuantityType,
                      'squareOffQtyValue': squareOffQtyValue, 'blockOrderSending': blockOrderSending,
                      'cancelOrders': cancelOrders
                      }
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._put('portfolio.squareoff', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_order_history(self, appOrderID, clientID=None):
        """Order history will provide particular order trail chain. This indicate the particular order & its state
        changes. i.e.Pending New to New, New to PartiallyFilled, PartiallyFilled, PartiallyFilled & PartiallyFilled
        to Filled etc """
        try:
            params = {'appOrderID': appOrderID}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._get('order.history', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def interactive_logout(self, clientID=None):
        """This call invalidates the session token and destroys the API session. After this, the user should go
        through login flow again and extract session token from login response before further activities. """
        try:
            params = {}
            if not self.isInvestorClient:
                params['clientID'] = clientID
            response = self._delete('user.logout', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_exchange_status(self, userID=None):
        """Get exchange status to check if exchanges are available for trading"""
        try:
            params = {}
            if userID:
                params['userID'] = userID
            response = self._get('status.exchange', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_exchange_messages(self, exchangeSegment="NSECM"):
        """Get exchange messages regarding bans, circuit limits, news about listed companies, etc"""
        try:
            params = {'exchangeSegment': exchangeSegment}
            response = self._get('messages.exchange', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

########################################################################################################
# Market data API
########################################################################################################

    def get_config(self):
        try:
            params = {}
            response = self._get('market.config', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_quote(self, Instruments, xtsMessageCode, publishFormat):
        try:

            params = {'instruments': Instruments, 'xtsMessageCode': xtsMessageCode, 'publishFormat': publishFormat}
            response = self._post('market.instruments.quotes', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def send_subscription(self, Instruments, xtsMessageCode):
        try:
            params = {'instruments': Instruments, 'xtsMessageCode': xtsMessageCode}
            response = self._post('market.instruments.subscription', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def send_unsubscription(self, Instruments, xtsMessageCode):
        try:
            params = {'instruments': Instruments, 'xtsMessageCode': xtsMessageCode}
            response = self._put('market.instruments.unsubscription', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_master(self, exchangeSegmentList):
        try:
            params = {"exchangeSegmentList": exchangeSegmentList}
            response = self._post('market.instruments.master', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_ohlc(self, exchangeSegment, exchangeInstrumentID, startTime, endTime, compressionValue):
        try:
            params = {
                'exchangeSegment': exchangeSegment,
                'exchangeInstrumentID': exchangeInstrumentID,
                'startTime': startTime,
                'endTime': endTime,
                'compressionValue': compressionValue}
            response = self._get('market.instruments.ohlc', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_series(self, exchangeSegment):
        try:
            params = {'exchangeSegment': exchangeSegment}
            response = self._get('market.instruments.instrument.series', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_equity_symbol(self, exchangeSegment, series, symbol):
        try:

            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol}
            response = self._get('market.instruments.instrument.equitysymbol', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_expiry_date(self, exchangeSegment, series, symbol):
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol}
            response = self._get('market.instruments.instrument.expirydate', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_future_symbol(self, exchangeSegment, series, symbol, expiryDate):
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol, 'expiryDate': expiryDate}
            response = self._get('market.instruments.instrument.futuresymbol', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_option_symbol(self, exchangeSegment, series, symbol, expiryDate, optionType, strikePrice):
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol, 'expiryDate': expiryDate,
                      'optionType': optionType, 'strikePrice': strikePrice}
            response = self._get('market.instruments.instrument.optionsymbol', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_option_type(self, exchangeSegment, series, symbol, expiryDate):
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol, 'expiryDate': expiryDate}
            response = self._get('market.instruments.instrument.optiontype', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_index_list(self, exchangeSegment):
        try:
            params = {'exchangeSegment': exchangeSegment}
            response = self._get('market.instruments.indexlist', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def search_by_instrumentid(self, Instruments):
        try:
            params = {'source': self.source, 'instruments': Instruments}
            response = self._post('market.search.instrumentsbyid', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}
    
    def search_by_instrument_id(self, instrument_id):
        """Search instruments by a single instrument ID"""
        try:
            # Create instruments list format expected by IIFL
            instruments = [{
                "exchangeSegment": "NSECM",  # Default to NSE Cash Market
                "exchangeInstrumentID": instrument_id
            }]
            
            params = {'source': self.source, 'instruments': instruments}
            response = self._post('market.search.instrumentsbyid', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def search_by_scriptname(self, searchString):
        try:
            params = {'searchString': searchString}
            response = self._get('market.search.instrumentsbystring', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_instrument_series(self, exchangeSegment):
        """Get available series for an exchange segment"""
        try:
            params = {'exchangeSegment': exchangeSegment}
            response = self._get('market.instruments.instrument.series', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_equity_symbol(self, exchangeSegment, series, symbol):
        """Get equity symbol details"""
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol}
            response = self._get('market.instruments.instrument.equitysymbol', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_expiry_date(self, exchangeSegment, series, symbol):
        """Get expiry dates for futures/options"""
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol}
            response = self._get('market.instruments.instrument.expirydate', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_future_symbol(self, exchangeSegment, series, symbol, expiryDate):
        """Get future symbol details"""
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol, 'expiryDate': expiryDate}
            response = self._get('market.instruments.instrument.futuresymbol', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_option_symbol(self, exchangeSegment, series, symbol, expiryDate, optionType, strikePrice):
        """Get option symbol details"""
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol, 'expiryDate': expiryDate,
                      'optionType': optionType, 'strikePrice': strikePrice}
            response = self._get('market.instruments.instrument.optionsymbol', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_option_type(self, exchangeSegment, series, symbol, expiryDate):
        """Get available option types for a symbol"""
        try:
            params = {'exchangeSegment': exchangeSegment, 'series': series, 'symbol': symbol, 'expiryDate': expiryDate}
            response = self._get('market.instruments.instrument.optiontype', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_strike_price(self, exchangeSegment, series, symbol, expiryDate, optionType):
        """Get available strike prices for options"""
        try:
            params = {
                'exchangeSegment': exchangeSegment, 
                'series': series, 
                'symbol': symbol, 
                'expiryDate': expiryDate,
                'optionType': optionType
            }
            response = self._get('market.instruments.instrument.strikeprice', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def get_index_list(self, exchangeSegment):
        """Get list of indices for an exchange segment"""
        try:
            params = {'exchangeSegment': exchangeSegment}
            response = self._get('market.instruments.indexlist', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    def marketdata_logout(self):
        try:
            params = {}
            response = self._delete('market.logout', params)
            return response
        except Exception as e:
            return {"type": "error", "description": str(e)}

    ########################################################################################################
    # Common Methods
    ########################################################################################################

    def _request(self, route, method, parameters=None):
        """Make HTTP request to IIFL API"""
        try:
            url = self.root + self._routes[route]
            headers = {}
            
            # Add authorization header if token exists
            # IIFL expects direct token without "Bearer" prefix
            if self.token:
                headers["Authorization"] = self.token
            
            if method == "GET":
                response = self.reqsession.get(url, params=parameters, headers=headers, verify=not self.disable_ssl, timeout=self.timeout)
            elif method == "POST":
                response = self.reqsession.post(url, json=parameters, headers=headers, verify=not self.disable_ssl, timeout=self.timeout)
            elif method == "PUT":
                response = self.reqsession.put(url, json=parameters, headers=headers, verify=not self.disable_ssl, timeout=self.timeout)
            elif method == "DELETE":
                response = self.reqsession.delete(url, json=parameters, headers=headers, verify=not self.disable_ssl, timeout=self.timeout)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            log.error(f"Request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                log.error(f"Response text: {e.response.text}")
            raise HTTPException(status_code=500, detail=f"IIFL API request failed: {str(e)}")

    def _get(self, route, params=None):
        """Make GET request to IIFL API"""
        return self._request(route, "GET", params)

    def _post(self, route, params=None):
        """Make POST request to IIFL API"""
        return self._request(route, "POST", params)

    def _put(self, route, params=None):
        """Make PUT request to IIFL API"""
        return self._request(route, "PUT", params)

    def _delete(self, route, params=None):
        """Make DELETE request to IIFL API"""
        return self._request(route, "DELETE", params)


class IIFLBinaryMarketDataClient:
    """
    IIFL Binary Market Data WebSocket Client for real-time streaming.
    
    This class handles Socket.IO connections for real-time market data streaming
    as described in the IIFL Binary Market Data API documentation.
    """
    
    def __init__(self, user_id: str, token: str, base_url: str = "https://ttblaze.iifl.com"):
        """
        Initialize the Binary Market Data WebSocket client.
        
        Args:
            user_id: User ID from login response
            token: Authentication token from login response
            base_url: Base URL for the WebSocket connection
        """
        self.user_id = user_id
        self.token = token
        self.base_url = base_url
        self.socketio_path = "/apibinarymarketdata/socketio"
        self.sio = None
        self.connected = False
        
        # Import socketio here to avoid dependency issues
        try:
            import socketio
            self.socketio = socketio
        except ImportError:
            log.error("socketio library not found. Install with: pip install python-socketio")
            raise ImportError("socketio library required for Binary Market Data streaming")
    
    def connect(self, broadcast_mode: str = "Full", publish_format: str = "JSON"):
        """
        Establish Socket.IO connection for real-time streaming.
        
        Args:
            broadcast_mode: "Full" or "Partial" broadcast mode
            publish_format: "JSON" or "Binary" publish format
        """
        try:
            self.sio = self.socketio.Client()
            
            # Set up event handlers
            @self.sio.on("connect")
            def on_connect():
                log.info("Binary Market Data WebSocket connected")
                self.connected = True
            
            @self.sio.on("disconnect")
            def on_disconnect():
                log.info("Binary Market Data WebSocket disconnected")
                self.connected = False
            
            @self.sio.on("joined")
            def on_joined(data):
                log.info(f"Socket joined: {data}")
            
            @self.sio.on("error")
            def on_error(data):
                log.error(f"Socket error: {data}")
            
            # Binary data event handler
            @self.sio.on("xts-binary-packet")
            def on_binary_packet(data):
                log.info(f"Binary data received: {data}")
                # Handle binary data processing here
                self._process_binary_data(data)
            
            # JSON data event handlers
            @self.sio.on("1501-json-full")
            def on_touchline_full(data):
                log.info(f"Touchline data (Full): {data}")
            
            @self.sio.on("1501-json-partial")
            def on_touchline_partial(data):
                log.info(f"Touchline data (Partial): {data}")
            
            @self.sio.on("1502-json-full")
            def on_market_depth_full(data):
                log.info(f"Market Depth data (Full): {data}")
            
            @self.sio.on("1502-json-partial")
            def on_market_depth_partial(data):
                log.info(f"Market Depth data (Partial): {data}")
            
            @self.sio.on("1505-json-full")
            def on_candle_data_full(data):
                log.info(f"Candle Data (Full): {data}")
            
            @self.sio.on("1505-json-partial")
            def on_candle_data_partial(data):
                log.info(f"Candle Data (Partial): {data}")
            
            @self.sio.on("1510-json-full")
            def on_open_interest_full(data):
                log.info(f"Open Interest data (Full): {data}")
            
            @self.sio.on("1510-json-partial")
            def on_open_interest_partial(data):
                log.info(f"Open Interest data (Partial): {data}")
            
            # Build connection string
            connection_string = (
                f"{self.base_url}/?token={self.token}&userID={self.user_id}"
                f"&publishFormat={publish_format}&broadcastMode={broadcast_mode}"
            )
            
            # Connect to the WebSocket
            self.sio.connect(
                self.base_url,
                socketio_path=self.socketio_path,
                query={
                    'token': self.token,
                    'userID': self.user_id,
                    'publishFormat': publish_format,
                    'broadcastMode': broadcast_mode
                }
            )
            
            log.info("Binary Market Data WebSocket connection established")
            
        except Exception as e:
            log.error(f"Failed to connect to Binary Market Data WebSocket: {str(e)}")
            raise
    
    def disconnect(self):
        """Disconnect from the WebSocket"""
        if self.sio and self.connected:
            self.sio.disconnect()
            self.connected = False
            log.info("Binary Market Data WebSocket disconnected")
    
    def _process_binary_data(self, data):
        """
        Process binary data packets as described in the API documentation.
        
        This method handles the binary data deserialization for:
        - Touchline (1501)
        - Market Depth (1502)
        - Open Interest (1510)
        - Candle Data (1505)
        """
        try:
            # Import zlib for decompression
            import zlib
            import struct
            
            # Convert data to bytes if it's a string
            if isinstance(data, str):
                data = data.encode('latin-1')
            
            # Check if data is compressed
            is_gzip_compressed = data[0] if len(data) > 0 else 0
            
            if is_gzip_compressed == 0:
                # Data is compressed, decompress it
                compressed_size = struct.unpack('<H', data[15:17])[0]
                compressed_data = data[17:17+compressed_size]
                decompressed_data = zlib.decompress(compressed_data)
                data = decompressed_data
            
            # Extract message code
            message_code = struct.unpack('<H', data[17:19])[0]
            
            # Process based on message code
            if message_code == 1501:  # Touchline
                self._process_touchline_data(data)
            elif message_code == 1502:  # Market Depth
                self._process_market_depth_data(data)
            elif message_code == 1510:  # Open Interest
                self._process_open_interest_data(data)
            elif message_code == 1505:  # Candle Data
                self._process_candle_data(data)
            else:
                log.warning(f"Unknown message code: {message_code}")
                
        except Exception as e:
            log.error(f"Error processing binary data: {str(e)}")
    
    def _process_touchline_data(self, data):
        """Process Touchline binary data (Message Code 1501)"""
        try:
            # Extract data according to the documentation structure
            # This is a simplified version - implement full parsing as needed
            offset = 19  # Start after common header
            
            # Extract common data
            message_version = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            
            # Continue with other fields as per documentation
            log.info(f"Processed Touchline data - Message Version: {message_version}")
            
        except Exception as e:
            log.error(f"Error processing Touchline data: {str(e)}")
    
    def _process_market_depth_data(self, data):
        """Process Market Depth binary data (Message Code 1502)"""
        try:
            # Implement market depth data processing
            log.info("Processing Market Depth data")
        except Exception as e:
            log.error(f"Error processing Market Depth data: {str(e)}")
    
    def _process_open_interest_data(self, data):
        """Process Open Interest binary data (Message Code 1510)"""
        try:
            # Implement open interest data processing
            log.info("Processing Open Interest data")
        except Exception as e:
            log.error(f"Error processing Open Interest data: {str(e)}")
    
    def _process_candle_data(self, data):
        """Process Candle Data binary data (Message Code 1505)"""
        try:
            # Implement candle data processing
            log.info("Processing Candle Data")
        except Exception as e:
            log.error(f"Error processing Candle Data: {str(e)}")
    
    def is_connected(self):
        """Check if the WebSocket is connected"""
        return self.connected and self.sio is not None
