"""
Market Analytics Service

This service provides comprehensive market analytics including:
- Market Cap calculation
- Returns calculation (1D, 1W, 1M, 6M, 1Y)
- CAGR calculation (5Y)
- Gap with Nifty comparison
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import traceback
from loguru import logger
from app.services.iifl_connect import IIFLConnect
from app.models.user import User


class MarketAnalyticsService:
    """Service for calculating market analytics metrics"""
    
    def __init__(self, user: User, db_session=None):
        self.user = user
        self.db_session = db_session
        self.parquet_file_path = "adjusted_eq_data(2025-08-01).parquet"
        self.nifty_symbol = "NIFTY 50"
        
    def calculate_market_cap(self, current_price: float, shares_outstanding: int) -> float:
        """Calculate market capitalization"""
        try:
            market_cap = current_price * shares_outstanding
            return market_cap
        except Exception as e:
            logger.error(f"Error calculating market cap: {e}")
            return None
    
    def calculate_return(self, current_price: float, historical_price: float) -> float:
        """Calculate percentage return"""
        try:
            if historical_price == 0:
                return None
            return ((current_price - historical_price) / historical_price) * 100
        except Exception as e:
            logger.error(f"Error calculating return: {e}")
            return None
    
    def calculate_cagr(self, current_price: float, historical_price: float, years: float) -> float:
        """Calculate Compound Annual Growth Rate"""
        try:
            if historical_price <= 0 or years <= 0:
                return None
            cagr = ((current_price / historical_price) ** (1 / years)) - 1
            return cagr * 100  # Convert to percentage
        except Exception as e:
            logger.error(f"Error calculating CAGR: {e}")
            return None
    
    def get_historical_data_from_parquet(self, symbol: str, days_back: int) -> Optional[float]:
        """Get historical price from parquet file (DISABLED)"""
        # This method is disabled - always returns None
        return None
    
    def get_historical_data_from_iifl(self, symbol: str, days_back: int) -> Optional[float]:
        """Get historical price from IIFL API"""
        try:
            iifl_client = IIFLConnect(self.user, api_type="market")
            
            # Login to IIFL
            login_response = iifl_client.marketdata_login()
            if login_response.get("type") != "success":
                logger.error("Failed to login to IIFL for historical data")
                return None
            
            # Search for the stock
            search_response = iifl_client.search_by_scriptname(symbol)
            if search_response.get("type") != "success" or not search_response.get("result"):
                logger.error(f"No search results found for {symbol}")
                return None
            
            # Log search results for debugging (but not for Nifty to reduce terminal noise)
            if "NIFTY" in symbol.upper():
                pass  # Skip logging for Nifty searches
            
            # Get the first equity stock
            stocks = search_response["result"]
            equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
            
            if equity_stocks:
                stock_info = equity_stocks[0]
            else:
                stock_info = stocks[0]
            
            exchange_segment = stock_info.get("ExchangeSegment", 1)
            exchange_instrument_id = stock_info.get("ExchangeInstrumentID")
            
            if not exchange_instrument_id:
                logger.error(f"No exchange instrument ID found for {symbol}")
                return None
            
            # Calculate date range
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days_back + 10)  # Add buffer
            
            # Get OHLC data
            ohlc_response = iifl_client.get_ohlc(
                exchangeSegment="NSECM" if exchange_segment == 1 else "NSEFO",
                exchangeInstrumentID=exchange_instrument_id,
                startTime=start_time.strftime("%b %d %Y %H%M%S"),
                endTime=end_time.strftime("%b %d %Y %H%M%S"),
                compressionValue=iifl_client.COMPRESSION_DAILY
            )
            
            # Logout
            iifl_client.marketdata_logout()
            
            if ohlc_response.get("type") != "success":
                logger.error(f"OHLC response failed for {symbol}: {ohlc_response}")
                return None
            
            # Parse OHLC data to get historical price
            ohlc_data = ohlc_response.get("result", {})
            logger.info(f"OHLC data for {symbol}: {ohlc_data}")
            
            # Handle different OHLC response formats
            historical_price = None
            
            # Format 1: Check if dataReponse exists (pipe-separated format)
            if isinstance(ohlc_data, dict) and "dataReponse" in ohlc_data:
                data_response = ohlc_data["dataReponse"]
                if isinstance(data_response, str) and data_response.strip():
                    # Parse pipe-separated data: timestamp|open|high|low|close|volume|...
                    data_points = data_response.strip().split(',')
                    if data_points:
                        # Get the data point closest to days_back
                        target_date = end_time - timedelta(days=days_back)
                        closest_data = None
                        min_date_diff = float('inf')
                        
                        for data_point in data_points:
                            if '|' in data_point:
                                parts = data_point.split('|')
                                if len(parts) >= 5:
                                    try:
                                        # Parse timestamp (Unix timestamp)
                                        timestamp = int(parts[0])
                                        data_date = datetime.fromtimestamp(timestamp)
                                        date_diff = abs((data_date - target_date).days)
                                        
                                        if date_diff < min_date_diff:
                                            min_date_diff = date_diff
                                            close_price = float(parts[4])  # Close price is 5th element
                                            closest_data = close_price
                                    except (ValueError, IndexError) as e:
                                        logger.warning(f"Error parsing data point {data_point}: {e}")
                                        continue
                        
                        if closest_data is not None:
                            historical_price = closest_data
                            logger.info(f"Found historical price for {symbol} ({days_back} days ago): {historical_price}")
            
            # Format 2: Check if it's a list of dictionaries
            elif isinstance(ohlc_data, list) and len(ohlc_data) > 0:
                # Get the data point closest to days_back
                target_date = end_time - timedelta(days=days_back)
                closest_data = None
                min_date_diff = float('inf')
                
                for data_point in ohlc_data:
                    if isinstance(data_point, dict) and 'DateTime' in data_point:
                        try:
                            data_date = datetime.strptime(data_point['DateTime'], "%Y-%m-%d %H:%M:%S")
                            date_diff = abs((data_date - target_date).days)
                            
                            if date_diff < min_date_diff:
                                min_date_diff = date_diff
                                closest_data = data_point
                        except Exception as e:
                            logger.warning(f"Error parsing date {data_point.get('DateTime')}: {e}")
                            continue
                
                if closest_data and 'Close' in closest_data:
                    historical_price = float(closest_data['Close'])
                    logger.info(f"Found historical price for {symbol} ({days_back} days ago): {historical_price}")
            
            if historical_price is None:
                logger.warning(f"No historical price found for {symbol} {days_back} days ago")
            
            return historical_price
            
        except Exception as e:
            logger.error(f"Error getting IIFL historical data for {symbol}: {e}")
            return None
    
    def get_nifty_data(self) -> Dict[str, float]:
        """Get Nifty historical data for comparison"""
        try:
            # DISABLED PARQUET, USING ONLY IIFL API
            # Try different Nifty symbols that might work with IIFL API
            nifty_symbols = ["NIFTY50", "NIFTY-50", "NIFTY", "NIFTY50 INDEX"]
            
            nifty_data = {}
            successful_symbol = None
            
            # Try each symbol until one works
            for symbol in nifty_symbols:
                try:
                    nifty_1d = self.get_historical_data_from_iifl(symbol, 1)
                    if nifty_1d is not None:
                        successful_symbol = symbol
                        nifty_data = {
                            "1d": nifty_1d,
                            "1w": self.get_historical_data_from_iifl(symbol, 7),
                            "1m": self.get_historical_data_from_iifl(symbol, 30),
                            "6m": self.get_historical_data_from_iifl(symbol, 180),
                            "1y": self.get_historical_data_from_iifl(symbol, 365),
                            "5y": self.get_historical_data_from_iifl(symbol, 1825)
                        }
                        break
                except Exception as e:
                    continue
            
            if not nifty_data:
                pass  # Silently fail for Nifty data
            
            return nifty_data
            
        except Exception as e:
            logger.error(f"Error getting Nifty data: {e}")
            return {}
    
    def calculate_gap_with_nifty(self, stock_return: float, nifty_return: float) -> float:
        """Calculate gap with Nifty (excess return)"""
        try:
            if stock_return is None or nifty_return is None:
                return None
            return stock_return - nifty_return
        except Exception as e:
            logger.error(f"Error calculating gap with Nifty: {e}")
            return None
    
    def get_stock_analytics(self, symbol: str, current_price: float, shares_outstanding: int = None) -> Dict:
        """Get comprehensive stock analytics"""
        try:
            analytics = {
                "symbol": symbol,
                "current_price": current_price,
                "market_cap": None,
                "returns": {
                    "1d": None,
                    "1w": None,
                    "1m": None,
                    "6m": None,
                    "1y": None
                },
                "cagr": {
                    "5y": None
                },
                "gap_with_nifty": {
                    "1w": None,
                    "1m": None,
                    "6m": None,
                    "1y": None,
                    "5y_cagr": None
                }
            }
            
            # Calculate market cap if shares outstanding provided
            if shares_outstanding:
                analytics["market_cap"] = self.calculate_market_cap(current_price, shares_outstanding)
            
            # Get historical prices - DISABLED PARQUET, USING ONLY IIFL API
            historical_prices = {}
            for period, days in [("1d", 1), ("1w", 7), ("1m", 30), ("6m", 180), ("1y", 365), ("5y", 1825)]:
                # Skip parquet, use only IIFL API
                # price = self.get_historical_data_from_parquet(symbol, days)
                price = self.get_historical_data_from_iifl(symbol, days)
                historical_prices[period] = price
            
            # Calculate returns
            for period in ["1d", "1w", "1m", "6m", "1y"]:
                if historical_prices.get(period) is not None:
                    analytics["returns"][period] = self.calculate_return(current_price, historical_prices[period])
            
            # Calculate 5Y CAGR
            if historical_prices.get("5y") is not None:
                analytics["cagr"]["5y"] = self.calculate_cagr(current_price, historical_prices["5y"], 5)
            
            # Get Nifty data for comparison - DISABLED PARQUET, USING ONLY IIFL API
            nifty_data = self.get_nifty_data()
            
            # Calculate gap with Nifty
            if nifty_data:
                for period in ["1w", "1m", "6m", "1y"]:
                    if (analytics["returns"].get(period) is not None and 
                        nifty_data.get(period) is not None):
                        analytics["gap_with_nifty"][period] = self.calculate_gap_with_nifty(
                            analytics["returns"][period], 
                            self.calculate_return(current_price, nifty_data[period])
                        )
                
                # Calculate 5Y CAGR gap
                if (analytics["cagr"]["5y"] is not None and 
                    nifty_data.get("5y") is not None):
                    nifty_5y_cagr = self.calculate_cagr(current_price, nifty_data["5y"], 5)
                    if nifty_5y_cagr is not None:
                        analytics["gap_with_nifty"]["5y_cagr"] = self.calculate_gap_with_nifty(
                            analytics["cagr"]["5y"], nifty_5y_cagr
                        )
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error calculating stock analytics for {symbol}: {traceback.format_exc()}")
            return {
                "symbol": symbol,
                "error": str(e),
                "current_price": current_price
            }
    
    def get_multiple_stocks_analytics(self, stocks_data: List[Dict]) -> List[Dict]:
        """Get analytics for multiple stocks"""
        try:
            results = []
            for stock_data in stocks_data:
                symbol = stock_data.get("symbol")
                current_price = stock_data.get("current_price")
                shares_outstanding = stock_data.get("shares_outstanding")
                
                if symbol and current_price:
                    analytics = self.get_stock_analytics(symbol, current_price, shares_outstanding)
                    results.append(analytics)
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating multiple stocks analytics: {e}")
            return [] 