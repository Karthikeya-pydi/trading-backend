"""
    IIFL Connect API wrapper for the trading platform.

    This module provides a wrapper for the IIFL Connect REST APIs, integrated with FastAPI.

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

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_STOPMARKET = "STOPMARKET"
    ORDER_TYPE_STOPLIMIT = "STOPLIMIT"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Exchange Segments
    EXCHANGE_NSECM = "NSECM"
    EXCHANGE_NSEFO = "NSEFO"
    EXCHANGE_NSECD = "NSECD"
    EXCHANGE_MCXFO = "MCXFO"
    EXCHANGE_BSECM = "BSECM"
    EXCHANGE_BSEFO = "BSEFO"

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
        "portfolio.positions": "/interactive/portfolio/positions",
        "portfolio.holdings": "/interactive/portfolio/holdings",

        # Market API endpoints
        "marketdata.prefix": "apimarketdata",
        "market.login": "/apimarketdata/auth/login",
        "market.logout": "/apimarketdata/auth/logout",
        "market.config": "/apimarketdata/config/clientConfig",
        "market.instruments.master": "/apimarketdata/instruments/master",
        "market.instruments.quotes": "/apimarketdata/instruments/quotes",
        "market.search.instrumentsbystring": "/apimarketdata/search/instrumentsbystring",
        "market.search.instrumentsbyid": "/apimarketdata/search/instrumentsbyid",
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

    def search_by_scriptname(self, searchString):
        try:
            params = {'searchString': searchString}
            response = self._get('market.search.instrumentsbystring', params)
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
