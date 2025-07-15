from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Optional
import traceback
from loguru import logger

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.iifl_service_fixed import IIFLServiceFixed
from app.services.iifl_connect import IIFLConnect

router = APIRouter()

@router.get("/")
async def market_data_info():
    """Market Data API Information"""
    return {
        "message": "Market Data API",
        "version": "2.0.0",
        "endpoints": {
            "POST /market-data": "Get real-time market data for instruments",
            "POST /stock-data": "Get full market data for stock by name (POST)",
            "GET /stock/{stock_name}": "Get full market data for stock by name (GET)",
            "POST /ltp": "Get Last Traded Price for instruments", 
            "GET /instruments/search": "Search instruments by name or symbol",
            "GET /instruments/master": "Download instrument master data",
            "WS /ws/market-data": "WebSocket for real-time market data streams"
        },
        "features": {
            "real_time_data": "Live market data from IIFL Binary Market Data API",
            "stock_search": "Search stocks by name, symbol, or ISIN",
            "comprehensive_data": "Touchline, Market Depth, OHLC data",
            "historical_data": "5-day historical OHLC data",
            "authentication": "Bearer JWT token required",
            "rate_limits": "As per IIFL API limits"
        }
    }

@router.post("/market-data")
async def get_market_data(
    request: Dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get real-time market data for instruments
    
    Request body:
    {
        "instruments": [
            {
                "exchangeSegment": "NSECM",
                "exchangeInstrumentID": "1234"
            }
        ]
    }
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    try:
        iifl_service = IIFLServiceFixed(db)
        client = iifl_service._get_client(current_user.id, "market")
        
        instruments = request.get("instruments", [])
        if not instruments:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No instruments provided"
            )
        
        # Validate instrument format
        formatted_instruments = []
        for instrument in instruments:
            if not isinstance(instrument, dict):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Each instrument must be an object with exchangeSegment and exchangeInstrumentID"
                )
            
            if not instrument.get("exchangeInstrumentID"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="exchangeInstrumentID is required for each instrument"
                )
            
            formatted_instruments.append({
                "exchangeSegment": instrument.get("exchangeSegment", "NSECM"),
                "exchangeInstrumentID": str(instrument.get("exchangeInstrumentID"))
            })
        
        # Get quotes from IIFL
        quotes_result = client.get_quote(
            Instruments=formatted_instruments,
            xtsMessageCode=1512,  # Full market data
            publishFormat="JSON"
        )
        
        # Log the raw response for debugging
        logger.info(f"Raw IIFL response: {quotes_result}")
        
        # Normalize the response to handle both listQuotes and quotesList
        if quotes_result.get("type") == "success":
            result = quotes_result.get("result", {})
            
            # Check if data is in quotesList (new format) or listQuotes (old format)
            quotes_data = result.get("quotesList", result.get("listQuotes", []))
            
            # Normalize the response
            normalized_result = {
                "type": "success",
                "code": quotes_result.get("code", "s-quotes-0001"),
                "description": quotes_result.get("description", "Get quotes successfully!"),
                "result": {
                    "mdp": result.get("mdp", 1512),
                    "listQuotes": quotes_data,  # Always use listQuotes for consistency
                    "quotesList": quotes_data   # Keep quotesList for backward compatibility
                }
            }
            
            return normalized_result
        
        return quotes_result
  
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch market data: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch market data"
        )

@router.post("/stock-data")
async def get_stock_data_by_name(
    request: Dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get full market data for a stock by name/symbol
    
    Request body:
    {
        "stock_name": "RELIANCE" or "RELIANCE-EQ" or "RELIANCE"
    }
    
    Returns:
    - Stock information
    - Real-time market data (LTP, Bid/Ask, Volume, etc.)
    - OHLC data
    - Market depth (if available)
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    stock_name = request.get("stock_name", "").strip()
    if not stock_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Stock name is required"
        )
    
    try:
        # Initialize IIFL Connect for market data
        iifl_client = IIFLConnect(current_user, api_type="market")
        
        # Step 1: Login to get token
        login_response = iifl_client.marketdata_login()
        if login_response.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to authenticate with IIFL Market Data API"
            )
        
        # Step 2: Search for the stock
        search_response = iifl_client.search_by_scriptname(stock_name)
        
        if search_response.get("type") != "success" or not search_response.get("result"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock '{stock_name}' not found"
            )
        
        # Filter for equity stocks (prefer NSECM - Cash Market, series EQ)
        stocks = search_response["result"]
        equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
        
        if equity_stocks:
            stock_info = equity_stocks[0]  # Prefer equity stocks
        else:
            stock_info = stocks[0]  # Fallback to first result
        
        exchange_segment = stock_info.get("ExchangeSegment", 1)
        exchange_instrument_id = stock_info.get("ExchangeInstrumentID")
        
        if not exchange_instrument_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Invalid stock data for '{stock_name}'"
            )
        
        # Step 3: Get real-time market data
        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": exchange_instrument_id
        }]
        
        # Get Touchline data (basic market data)
        touchline_response = iifl_client.get_quote(
            Instruments=instruments,
            xtsMessageCode=iifl_client.MESSAGE_CODE_TOUCHLINE,
            publishFormat=iifl_client.PUBLISH_FORMAT_JSON
        )
        
        # Get Market Depth data (order book)
        market_depth_response = iifl_client.get_quote(
            Instruments=instruments,
            xtsMessageCode=iifl_client.MESSAGE_CODE_MARKET_DEPTH,
            publishFormat=iifl_client.PUBLISH_FORMAT_JSON
        )
        
        # Step 4: Get OHLC data (last 5 days)
        from datetime import datetime, timedelta
        end_time = datetime.now()
        start_time = end_time - timedelta(days=5)
        
        ohlc_response = iifl_client.get_ohlc(
            exchangeSegment="NSECM" if exchange_segment == 1 else "NSEFO",
            exchangeInstrumentID=exchange_instrument_id,
            startTime=start_time.strftime("%b %d %Y %H%M%S"),
            endTime=end_time.strftime("%b %d %Y %H%M%S"),
            compressionValue=iifl_client.COMPRESSION_DAILY
        )
        
        # Step 5: Compile comprehensive response
        response_data = {
            "type": "success",
            "stock_info": {
                "name": stock_info.get("DisplayName", stock_info.get("Name")),
                "symbol": stock_info.get("Name"),
                "exchange_segment": stock_info.get("ExchangeSegment"),
                "instrument_id": stock_info.get("ExchangeInstrumentID"),
                "series": stock_info.get("Series"),
                "isin": stock_info.get("ISIN"),
                "lot_size": stock_info.get("LotSize"),
                "tick_size": stock_info.get("TickSize"),
                "price_band_high": stock_info.get("PriceBand", {}).get("High"),
                "price_band_low": stock_info.get("PriceBand", {}).get("Low")
            },
            "market_data": {
                "touchline": touchline_response.get("result", {}),
                "market_depth": market_depth_response.get("result", {})
            },
            "historical_data": {
                "ohlc": ohlc_response.get("result", {})
            },
            "timestamp": datetime.now().isoformat()
        }
        
        # Step 6: Logout
        iifl_client.marketdata_logout()
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch stock data for '{stock_name}': {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch stock data for '{stock_name}'"
        )

@router.get("/stock/{stock_name}")
async def get_stock_data_by_name_get(
    stock_name: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get full market data for a stock by name/symbol (GET endpoint)
    
    Path parameter:
    - stock_name: Stock name or symbol (e.g., "RELIANCE", "TCS", "INFY")
    
    Returns:
    - Stock information
    - Real-time market data (LTP, Bid/Ask, Volume, etc.)
    - OHLC data
    - Market depth (if available)
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    stock_name = stock_name.strip().upper()
    if not stock_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Stock name is required"
        )
    
    try:
        # Initialize IIFL Connect for market data
        iifl_client = IIFLConnect(current_user, api_type="market")
        
        # Step 1: Login to get token
        login_response = iifl_client.marketdata_login()
        if login_response.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to authenticate with IIFL Market Data API"
            )
        
        # Step 2: Search for the stock
        search_response = iifl_client.search_by_scriptname(stock_name)
        
        if search_response.get("type") != "success" or not search_response.get("result"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock '{stock_name}' not found"
            )
        
        # Filter for equity stocks (prefer NSECM - Cash Market, series EQ)
        stocks = search_response["result"]
        equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
        
        if equity_stocks:
            stock_info = equity_stocks[0]  # Prefer equity stocks
        else:
            stock_info = stocks[0]  # Fallback to first result
        
        exchange_segment = stock_info.get("ExchangeSegment", 1)
        exchange_instrument_id = stock_info.get("ExchangeInstrumentID")
        
        if not exchange_instrument_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Invalid stock data for '{stock_name}'"
            )
        
        # Step 3: Get real-time market data
        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": exchange_instrument_id
        }]
        
        # Get Touchline data (basic market data)
        touchline_response = iifl_client.get_quote(
            Instruments=instruments,
            xtsMessageCode=iifl_client.MESSAGE_CODE_TOUCHLINE,
            publishFormat=iifl_client.PUBLISH_FORMAT_JSON
        )
        
        # Get Market Depth data (order book)
        market_depth_response = iifl_client.get_quote(
            Instruments=instruments,
            xtsMessageCode=iifl_client.MESSAGE_CODE_MARKET_DEPTH,
            publishFormat=iifl_client.PUBLISH_FORMAT_JSON
        )
        
        # Step 4: Get OHLC data (last 5 days)
        from datetime import datetime, timedelta
        end_time = datetime.now()
        start_time = end_time - timedelta(days=5)
        
        ohlc_response = iifl_client.get_ohlc(
            exchangeSegment="NSECM" if exchange_segment == 1 else "NSEFO",
            exchangeInstrumentID=exchange_instrument_id,
            startTime=start_time.strftime("%b %d %Y %H%M%S"),
            endTime=end_time.strftime("%b %d %Y %H%M%S"),
            compressionValue=iifl_client.COMPRESSION_DAILY
        )
        
        # Step 5: Compile comprehensive response
        response_data = {
            "type": "success",
            "stock_info": {
                "name": stock_info.get("DisplayName", stock_info.get("Name")),
                "symbol": stock_info.get("Name"),
                "exchange_segment": stock_info.get("ExchangeSegment"),
                "instrument_id": stock_info.get("ExchangeInstrumentID"),
                "series": stock_info.get("Series"),
                "isin": stock_info.get("ISIN"),
                "lot_size": stock_info.get("LotSize"),
                "tick_size": stock_info.get("TickSize"),
                "price_band_high": stock_info.get("PriceBand", {}).get("High"),
                "price_band_low": stock_info.get("PriceBand", {}).get("Low")
            },
            "market_data": {
                "touchline": touchline_response.get("result", {}),
                "market_depth": market_depth_response.get("result", {})
            },
            "historical_data": {
                "ohlc": ohlc_response.get("result", {})
            },
            "timestamp": datetime.now().isoformat()
        }
        
        # Step 6: Logout
        iifl_client.marketdata_logout()
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch stock data for '{stock_name}': {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch stock data for '{stock_name}'"
        )

@router.post("/ltp")
async def get_last_traded_price(
    request: Dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get Last Traded Price (LTP) for instruments
    
    Request body:
    {
        "instruments": [
            {
                "exchangeSegment": "NSECM", 
                "exchangeInstrumentID": "1234"
            }
        ]
    }
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    try:
        instruments = request.get("instruments", [])
        if not instruments:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No instruments provided"
            )
        
        iifl_service = IIFLServiceFixed(db)
        ltp_data = iifl_service.get_ltp(db, current_user.id, instruments)
        return ltp_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch LTP data: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch LTP data"
        )

@router.get("/instruments/search")
async def search_instruments(
    q: str,
    limit: Optional[int] = 20,
    exchange_segment: Optional[str] = "NSECM",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Search for instruments by name, symbol, or ISIN
    
    Query parameters:
    - q: Search query (required, min 1 character)
    - limit: Maximum results to return (default: 20, max: 100)
    - exchange_segment: Exchange segment filter (default: NSECM)
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    if not q or len(q.strip()) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Search query must be at least 1 character"
        )
    
    # Enforce limit bounds
    limit = min(max(1, limit or 20), 100)
    
    try:
        iifl_service = IIFLServiceFixed(db)
        client = iifl_service._get_client(current_user.id, "market")
        
        # Try IIFL native search first
        search_result = client.search_by_scriptname(q)
        
        if (search_result.get("type") == "success" and 
            search_result.get("result") and 
            len(search_result.get("result", [])) > 0):
            # Return IIFL search results
            results = search_result.get("result", [])[:limit]
            return {
                "type": "success",
                "query": q,
                "total_found": len(search_result.get("result", [])),
                "returned": len(results),
                "results": results,
                "source": "iifl_search"
            }
        
        # Fallback to master data search
        master_data = iifl_service.get_instrument_master(db, current_user.id, [exchange_segment])
        
        if master_data.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to access instrument data"
            )
        
        # Parse master data
        raw_result = master_data.get("result", [])
        raw_instruments = []
        
        if isinstance(raw_result, str):
            raw_instruments = raw_result.strip().split('\n')
        elif isinstance(raw_result, list):
            raw_instruments = raw_result
        
        # Search through instruments
        search_query = q.upper()
        matching_instruments = []
        
        for raw_instrument in raw_instruments:
            try:
                if isinstance(raw_instrument, str) and len(raw_instrument.strip()) > 10:
                    if '|' in raw_instrument:
                        parts = raw_instrument.split('|')
                        if len(parts) >= 15:
                            instrument = {
                                "ExchangeSegment": parts[0],
                                "ExchangeInstrumentID": parts[1] if parts[1].isdigit() else None,
                                "InstrumentType": parts[2],
                                "Name": parts[3],
                                "DisplayName": parts[4] if parts[4] else parts[3],
                                "Series": parts[5],
                                "Symbol": parts[6] if parts[6] else parts[3],
                                "ISIN": parts[7],
                                "Description": parts[18] if len(parts) > 18 else parts[4]
                            }
                            
                            # Search in instrument fields
                            searchable_text = " ".join([
                                str(instrument.get("Name", "")),
                                str(instrument.get("DisplayName", "")),
                                str(instrument.get("Symbol", "")),
                                str(instrument.get("ISIN", "")),
                                str(instrument.get("Description", ""))
                            ]).upper()
                            
                            if search_query in searchable_text and instrument.get("ExchangeInstrumentID"):
                                matching_instruments.append(instrument)
                                
                                if len(matching_instruments) >= limit:
                                    break
                                    
            except Exception:
                continue
        
        return {
            "type": "success",
            "query": q,
            "total_found": len(matching_instruments),
            "returned": len(matching_instruments),
            "results": matching_instruments,
            "source": "master_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to search instruments: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search instruments"
        )

@router.get("/instruments/master")
async def get_instrument_master(
    exchange_segments: str = "NSECM,NSEFO,NSECD,NSECO,BSECM,BSEFO,BSECD",
    include_sample: bool = False,
    full_data: bool = True,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Download ALL instrument master data from NSE + BSE exchanges
    
    Returns complete dataset of ~85,000+ instruments by default including:
    - NSE: Cash, F&O, Currency, Commodity derivatives  
    - BSE: Cash, F&O, Currency derivatives
    
    Query parameters (optional):
    - exchange_segments: Filter specific exchanges (default: ALL NSE + BSE)
    - include_sample: Return only sample data (default: false - returns all)
    - full_data: Return complete dataset (default: true)
    
    Available Exchange Segments:
    NSE: NSECM, NSEFO, NSECD, NSECO
    BSE: BSECM, BSEFO, BSECD
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    try:
        iifl_service = IIFLServiceFixed(db)
        segments = [seg.strip() for seg in exchange_segments.split(",")]
        master_data = iifl_service.get_instrument_master(db, current_user.id, segments)
        
        if master_data.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to fetch instrument master data from IIFL"
            )
        
        # Parse the data
        raw_result = master_data.get("result", [])
        raw_instruments = []
        
        if isinstance(raw_result, str):
            raw_instruments = raw_result.strip().split('\n')
        elif isinstance(raw_result, list):
            raw_instruments = raw_result
        
        # Parse instruments
        instruments = []
        parse_errors = 0
        
        for raw_instrument in raw_instruments:
            try:
                if isinstance(raw_instrument, str) and len(raw_instrument.strip()) > 10:
                    if '|' in raw_instrument:
                        parts = raw_instrument.split('|')
                        if len(parts) >= 15:
                            parsed_instrument = {
                                "ExchangeSegment": parts[0],
                                "ExchangeInstrumentID": parts[1] if parts[1].isdigit() else None,
                                "InstrumentType": parts[2],
                                "Name": parts[3],
                                "DisplayName": parts[4] if parts[4] else parts[3],
                                "Series": parts[5],
                                "Symbol": parts[6] if parts[6] else parts[3],
                                "ISIN": parts[7],
                                "PriceBandHigh": parts[8],
                                "PriceBandLow": parts[9],
                                "LotSize": parts[10],
                                "TickSize": parts[11],
                                "Multiplier": parts[12]
                            }
                            
                            if parsed_instrument.get("ExchangeInstrumentID"):
                                instruments.append(parsed_instrument)
                                
            except Exception:
                parse_errors += 1
                continue
        
        response_data = {
            "type": "success",
            "exchange_segments": segments,
            "total_instruments": len(instruments),
            "total_raw_entries": len(raw_instruments),
            "parse_errors": parse_errors,
            "success_rate": f"{(len(instruments)/len(raw_instruments)*100):.2f}%" if raw_instruments else "0%"
        }
        
        # Include instrument data based on request
        if full_data and instruments:
            # Return all instruments
            all_instruments = []
            for instrument in instruments:
                all_instruments.append({
                    "instrument_id": instrument.get("ExchangeInstrumentID"),
                    "name": instrument.get("DisplayName"),
                    "symbol": instrument.get("Symbol"),
                    "exchange": instrument.get("ExchangeSegment"),
                    "series": instrument.get("Series"),
                    "instrument_type": instrument.get("InstrumentType"),
                    "isin": instrument.get("ISIN"),
                    "price_band_high": instrument.get("PriceBandHigh"),
                    "price_band_low": instrument.get("PriceBandLow"),
                    "lot_size": instrument.get("LotSize"),
                    "tick_size": instrument.get("TickSize"),
                    "multiplier": instrument.get("Multiplier")
                })
            response_data["instruments"] = all_instruments
        elif include_sample and instruments:
            # Return sample data (default)
            sample_instruments = []
            for instrument in instruments[:10]:
                sample_instruments.append({
                    "instrument_id": instrument.get("ExchangeInstrumentID"),
                    "name": instrument.get("DisplayName"),
                    "symbol": instrument.get("Symbol"),
                    "exchange": instrument.get("ExchangeSegment"),
                    "series": instrument.get("Series"),
                    "instrument_type": instrument.get("InstrumentType"),
                    "isin": instrument.get("ISIN")
                })
            response_data["sample_instruments"] = sample_instruments
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch instrument master: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch instrument master data"
        )
