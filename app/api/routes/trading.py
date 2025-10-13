from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Optional
from datetime import datetime

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.models.trade import Trade, Position
from app.schemas.trading import (
    TradeRequest, TradeResponse, PositionResponse, 
    StockSearchResponse, BuyStockRequest, BuyStockResponse, StockQuoteResponse,
    EnhancedOrderBookResponse
)
from app.services.iifl_service import IIFLService
from app.services.iifl_connect import IIFLConnect

router = APIRouter()

@router.post("/place-order", response_model=TradeResponse)
async def place_order(
    trade_request: TradeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Place a trading order through IIFL Interactive API.
    
    This endpoint allows you to place various types of orders:
    
    **Cash Market Orders:**
    - Market orders for equities
    - Limit orders with specific prices
    - Stop loss orders
    
    **F&O Orders:**
    - Futures orders (NIFTY, BANKNIFTY, etc.)
    - Options orders (CALL/PUT with strike prices)
    - All order types with proper expiry dates
    
    **Example Requests:**
    
    1. **NIFTY Futures Market Order:**
    ```json
    {
        "underlying_instrument": "NIFTY",
        "order_type": "BUY",
        "quantity": 50,
        "expiry_date": "2024-12-28"
    }
    ```
    
    2. **NIFTY Options Limit Order:**
    ```json
    {
        "underlying_instrument": "NIFTY",
        "option_type": "CALL",
        "strike_price": 19000,
        "order_type": "BUY",
        "quantity": 25,
        "price": 150.50,
        "expiry_date": "2024-12-28"
    }
    ```
    
    3. **Equity Cash Market Order:**
    ```json
    {
        "underlying_instrument": "RELIANCE",
        "order_type": "BUY",
        "quantity": 100,
        "price": 2500.00
    }
    ```
    
    4. **Stop Loss Order:**
    ```json
    {
        "underlying_instrument": "NIFTY",
        "order_type": "SELL",
        "quantity": 50,
        "stop_loss_price": 18500,
        "expiry_date": "2024-12-28"
    }
    ```
    """
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Use the fixed IIFL service
        iifl_service = IIFLService(db)
        order_result = iifl_service.place_order(db, current_user.id, trade_request)
        
        # Extract order ID from successful response
        order_id = order_result.get("result", {}).get("AppOrderID")
        if not order_id:
            # Generate fallback order ID if IIFL doesn't provide one
            import time
            order_id = f"local_{current_user.id}_{int(time.time())}"
        
        # Save trade to database only if order placement succeeded
        trade = Trade(
            user_id=current_user.id,
            order_id=str(order_id),  # Ensure it's a string
            underlying_instrument=trade_request.underlying_instrument,
            option_type=trade_request.option_type,
            strike_price=trade_request.strike_price,
            expiry_date=trade_request.expiry_date,
            order_type=trade_request.order_type,
            quantity=trade_request.quantity,
            price=trade_request.price,
            order_status="NEW",
            stop_loss_price=trade_request.stop_loss_price
        )
        
        db.add(trade)
        db.commit()
        db.refresh(trade)
        
        return TradeResponse(
            id=trade.id,
            order_id=trade.order_id,
            status="success",
            message="Order placed successfully"
        )
        
    except HTTPException:
        # Re-raise HTTPExceptions (like IIFL failures)
        raise
    except Exception as e:
        # Handle other unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to place order: {str(e)}"
        )

@router.post("/place-order-advanced", response_model=TradeResponse)
async def place_order_advanced(
    trade_request: TradeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Advanced order placement with detailed validation and instrument lookup.
    
    This endpoint provides enhanced order placement with:
    - Automatic instrument ID lookup for F&O contracts
    - Detailed validation of order parameters
    - Better error messages and handling
    - Support for all IIFL order types
    
    **Features:**
    - Automatic exchange segment detection
    - F&O instrument search and matching
    - Product type determination (NRML/CNC/MIS)
    - Stop loss order handling
    - Comprehensive error reporting
    """
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured. Please configure your IIFL API credentials first."
        )
    
    try:
        # Use the fixed IIFL service with enhanced features
        iifl_service = IIFLService(db)
        
        # Place order with enhanced validation and instrument lookup
        order_result = iifl_service.place_order(db, current_user.id, trade_request)
        
        # Extract order details
        result_data = order_result.get("result", {})
        order_id = result_data.get("AppOrderID")
        order_status = result_data.get("OrderStatus", "NEW")
        
        if not order_id:
            # Generate fallback order ID
            import time
            order_id = f"local_{current_user.id}_{int(time.time())}"
        
        # Save trade to database
        trade = Trade(
            user_id=current_user.id,
            order_id=str(order_id),
            underlying_instrument=trade_request.underlying_instrument,
            option_type=trade_request.option_type,
            strike_price=trade_request.strike_price,
            expiry_date=trade_request.expiry_date,
            order_type=trade_request.order_type,
            quantity=trade_request.quantity,
            price=trade_request.price,
            order_status=order_status,
            stop_loss_price=trade_request.stop_loss_price
        )
        
        db.add(trade)
        db.commit()
        db.refresh(trade)
        
        return TradeResponse(
            id=trade.id,
            order_id=trade.order_id,
            status="success",
            message=f"Order placed successfully. Order ID: {str(order_id)}"
        )
        
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        # Handle other unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to place order: {str(e)}"
        )

@router.get("/positions", response_model=List[PositionResponse])
async def get_positions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user's current positions from IIFL"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Use the fixed IIFL service
        iifl_service = IIFLService(db)
        positions_result = iifl_service.get_positions(db, current_user.id)
        
        # Process and return positions
        # Handle both dict and string responses gracefully
        positions = []
        
        # Check if positions_result is a dict (expected) or string (error case)
        if isinstance(positions_result, dict):
            if positions_result.get("type") == "success":
                # Access the positionList from the result
                position_list = positions_result.get("result", {})
                if isinstance(position_list, dict):
                    positions_data = position_list.get("positionList", [])
                else:
                    positions_data = position_list if isinstance(position_list, list) else []
                
                for pos_data in positions_data:
                    # Convert IIFL position data to our schema
                    position = PositionResponse(
                        id=0,  # This would be from our database
                        underlying_instrument=pos_data.get("TradingSymbol", "").split()[0] if pos_data.get("TradingSymbol") else "UNKNOWN",
                        quantity=int(pos_data.get("Quantity", 0)),
                        average_price=float(pos_data.get("AveragePrice", 0)),
                        current_price=float(pos_data.get("LTP", 0)),
                        unrealized_pnl=float(pos_data.get("UnrealizedPnL", 0)),
                        stop_loss_active=False
                    )
                    positions.append(position)
            elif positions_result.get("type") == "error":
                # Handle IIFL error responses
                error_msg = positions_result.get("description", "Unknown IIFL error")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"IIFL API error: {error_msg}"
                )
        else:
            # Handle case where positions_result is a string (unexpected)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unexpected response format from IIFL: {str(positions_result)}"
            )
        
        return positions
        
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch positions: {str(e)}"
        )

@router.get("/trades")
async def get_trades(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user's trade history"""
    trades = db.query(Trade).filter(Trade.user_id == current_user.id).order_by(Trade.created_at.desc()).all()
    return trades

@router.get("/order-book", response_model=EnhancedOrderBookResponse)
async def get_order_book(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user's order book from IIFL with enhanced stock information"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Use the fixed IIFL service
        iifl_service = IIFLService(db)
        order_book = iifl_service.get_order_book(db, current_user.id)
        
        # Enhance order book with stock names
        if order_book.get("type") == "success" and order_book.get("result"):
            from app.services.instrument_service import InstrumentMappingService
            
            instrument_service = InstrumentMappingService()
            enhanced_orders = []
            
            for order in order_book["result"]:
                instrument_id = order.get("ExchangeInstrumentID")
                if instrument_id:
                    # Get stock information for this instrument
                    stock_info = await instrument_service.get_stock_info_by_instrument_id(
                        instrument_id, current_user
                    )
                    
                    if stock_info:
                        # Add stock information to the order
                        order["StockSymbol"] = stock_info.get("symbol", f"Unknown_{instrument_id}")
                        order["StockName"] = stock_info.get("name", f"Unknown Instrument {instrument_id}")
                        order["Series"] = stock_info.get("series", "")
                        order["ISIN"] = stock_info.get("isin", "")
                        order["LotSize"] = stock_info.get("lot_size", 1)
                    else:
                        # Fallback if stock info not found
                        order["StockSymbol"] = f"Unknown_{instrument_id}"
                        order["StockName"] = f"Unknown Instrument {instrument_id}"
                        order["Series"] = ""
                        order["ISIN"] = ""
                        order["LotSize"] = 1
                
                enhanced_orders.append(order)
            
            # Update the order book with enhanced orders
            order_book["result"] = enhanced_orders
            order_book["enhanced"] = True
            order_book["message"] = f"Order book retrieved with {len(enhanced_orders)} orders including stock names"
        
        return order_book
        
    except Exception as e:
        logger.error(f"Failed to fetch enhanced order book: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch order book: {str(e)}"
        )

@router.put("/orders/{order_id}/cancel")
async def cancel_order(
    order_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Cancel an existing order"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Get the trade record
        trade = db.query(Trade).filter(
            Trade.order_id == order_id,
            Trade.user_id == current_user.id
        ).first()
        
        if not trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        # Use the fixed IIFL service
        iifl_service = IIFLService(db)
        cancel_result = iifl_service.cancel_order(db, current_user.id, order_id)
        
        # Update trade status
        trade.order_status = "CANCELLED"
        db.commit()
        
        return {"status": "success", "message": "Order cancelled successfully"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel order: {str(e)}"
        )

@router.put("/orders/{order_id}/modify")
async def modify_order(
    order_id: str,
    modification: TradeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Modify an existing order"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Get the trade record
        trade = db.query(Trade).filter(
            Trade.order_id == order_id,
            Trade.user_id == current_user.id
        ).first()
        
        if not trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        # Use the fixed IIFL service
        iifl_service = IIFLService(db)
        modify_result = iifl_service.modify_order(db, current_user.id, order_id, modification)
        
        # Update trade details
        trade.quantity = modification.quantity
        trade.price = modification.price
        trade.stop_loss_price = modification.stop_loss_price
        trade.order_status = "MODIFIED"
        db.commit()
        
        return {"status": "success", "message": "Order modified successfully"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to modify order: {str(e)}"
        )

@router.post("/positions/{position_id}/square-off")
async def square_off_position(
    position_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Square off a position"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Get the position
        position = db.query(Position).filter(
            Position.id == position_id,
            Position.user_id == current_user.id
        ).first()
        
        if not position:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Position not found"
            )
        
        # Create opposite order to square off
        square_off_request = TradeRequest(
            underlying_instrument=position.underlying_instrument,
            option_type=position.option_type,
            strike_price=position.strike_price,
            expiry_date=position.expiry_date,
            order_type="SELL" if position.quantity > 0 else "BUY",
            quantity=abs(position.quantity)
        )
        
        # Use the fixed IIFL service
        iifl_service = IIFLService(db)
        order_result = iifl_service.place_order(db, current_user.id, square_off_request)
        
        return {"status": "success", "message": "Square off order placed successfully"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to square off position: {str(e)}"
        )

@router.get("/search-stocks", response_model=StockSearchResponse)
async def search_stocks_for_trading(
    q: str,
    limit: Optional[int] = 20,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Search for stocks that can be traded
    
    This endpoint searches for stocks by name/symbol and returns:
    - Stock information (name, symbol, ISIN, lot size)
    - Current market price (LTP)
    - Basic market data (bid/ask, volume)
    - Trading parameters (lot size, tick size)
    
    Query parameters:
    - q: Search query (required, min 1 character)
    - limit: Maximum results to return (default: 20, max: 50)
    
    Returns stocks that are available for trading on NSE/BSE
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
    limit = min(max(1, limit or 20), 50)
    
    try:
        # Initialize IIFL Connect for market data
        iifl_client = IIFLConnect(current_user, api_type="market")
        
        # Login to get token
        login_response = iifl_client.marketdata_login()
        if login_response.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to authenticate with IIFL Market Data API"
            )
        
        # Search for stocks
        search_response = iifl_client.search_by_scriptname(q)
        
        if search_response.get("type") != "success" or not search_response.get("result"):
            return StockSearchResponse(
                type="success",
                query=q,
                total_found=0,
                returned=0,
                results=[],
                message=f"No stocks found matching '{q}'"
            )
        
        # Filter for equity stocks (prefer NSECM - Cash Market, series EQ)
        stocks = search_response["result"]
        equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
        
        if not equity_stocks:
            equity_stocks = stocks  # Fallback to all results
        
        # Limit results
        equity_stocks = equity_stocks[:limit]
        
        # Get current market data for each stock
        enhanced_stocks = []
        for stock in equity_stocks:
            try:
                # Get current price and market data
                instruments = [{
                    "exchangeSegment": stock.get("ExchangeSegment", 1),
                    "exchangeInstrumentID": stock.get("ExchangeInstrumentID")
                }]
                
                # Get Touchline data (basic market data)
                touchline_response = iifl_client.get_quote(
                    Instruments=instruments,
                    xtsMessageCode=iifl_client.MESSAGE_CODE_TOUCHLINE,
                    publishFormat=iifl_client.PUBLISH_FORMAT_JSON
                )
                
                # Extract current price
                current_price = None
                try:
                    if touchline_response.get("result", {}).get("listQuotes"):
                        quotes_data = touchline_response["result"]["listQuotes"]
                        if isinstance(quotes_data, list) and len(quotes_data) > 0:
                            first_quote = quotes_data[0]
                            if isinstance(first_quote, dict):
                                current_price = first_quote.get("Touchline", {}).get("LastTradedPrice")
                            elif isinstance(first_quote, str):
                                try:
                                    import json
                                    parsed_quote = json.loads(first_quote)
                                    current_price = parsed_quote.get("LastTradedPrice")
                                except:
                                    pass
                except Exception:
                    pass
                
                # Create enhanced stock data
                enhanced_stock = {
                    "symbol": stock.get("Name"),
                    "name": stock.get("DisplayName", stock.get("Name")),
                    "exchange_segment": stock.get("ExchangeSegment"),
                    "instrument_id": stock.get("ExchangeInstrumentID"),
                    "series": stock.get("Series"),
                    "isin": stock.get("ISIN"),
                    "lot_size": stock.get("LotSize"),
                    "tick_size": stock.get("TickSize"),
                    "current_price": current_price,
                    "market_data": touchline_response.get("result", {})
                }
                
                enhanced_stocks.append(enhanced_stock)
                
            except Exception as e:
                # Skip stocks with errors, continue with others
                continue
        
        # Logout
        iifl_client.marketdata_logout()
        
        return StockSearchResponse(
            type="success",
            query=q,
            total_found=len(search_response["result"]),
            returned=len(enhanced_stocks),
            results=enhanced_stocks,
            message=f"Found {len(enhanced_stocks)} stocks matching '{q}'"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search stocks: {str(e)}"
        )

@router.post("/buy-stock", response_model=BuyStockResponse)
async def buy_stock_simple(
    request: BuyStockRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Simplified stock buying endpoint
    
    This endpoint allows users to buy stocks with minimal parameters:
    - Stock symbol (e.g., "RELIANCE", "TCS")
    - Quantity (number of shares)
    - Optional: Price (for limit orders, omit for market orders)
    
    The system will:
    1. Search for the stock and get its instrument details
    2. Get current market price
    3. Place the buy order through IIFL
    4. Save the trade to database
    """
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured. Please configure your IIFL API credentials first."
        )
    
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured. Please configure your IIFL Market Data credentials first."
        )
    
    try:
        # Step 1: Search for the stock and get instrument details
        iifl_market_client = IIFLConnect(current_user, api_type="market")
        
        # Login to market data API
        market_login = iifl_market_client.marketdata_login()
        if market_login.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to authenticate with IIFL Market Data API"
            )
        
        # Search for the stock
        search_response = iifl_market_client.search_by_scriptname(request.stock_symbol)
        
        if search_response.get("type") != "success" or not search_response.get("result"):
            iifl_market_client.marketdata_logout()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock '{request.stock_symbol}' not found"
            )
        
        # Filter for equity stocks
        stocks = search_response["result"]
        equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
        
        if not equity_stocks:
            iifl_market_client.marketdata_logout()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock '{request.stock_symbol}' is not available for trading"
            )
        
        stock_info = equity_stocks[0]
        exchange_segment = stock_info.get("ExchangeSegment", 1)
        exchange_instrument_id = stock_info.get("ExchangeInstrumentID")
        
        # Get current market price
        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": exchange_instrument_id
        }]
        
        touchline_response = iifl_market_client.get_quote(
            Instruments=instruments,
            xtsMessageCode=iifl_market_client.MESSAGE_CODE_TOUCHLINE,
            publishFormat=iifl_market_client.PUBLISH_FORMAT_JSON
        )
        
        current_price = None
        try:
            if touchline_response.get("result", {}).get("listQuotes"):
                quotes_data = touchline_response["result"]["listQuotes"]
                if isinstance(quotes_data, list) and len(quotes_data) > 0:
                    first_quote = quotes_data[0]
                    if isinstance(first_quote, dict):
                        current_price = first_quote.get("Touchline", {}).get("LastTradedPrice")
                    elif isinstance(first_quote, str):
                        try:
                            import json
                            parsed_quote = json.loads(first_quote)
                            current_price = parsed_quote.get("LastTradedPrice")
                        except:
                            pass
        except Exception:
            pass
        
        # Logout from market data API
        iifl_market_client.marketdata_logout()
        
        # Step 2: Create trade request for IIFL Interactive API
        # NEW APPROACH: We now use the actual instrument details from search results
        # This bypasses the hardcoded fallback that was causing wrong instrument orders
        trade_request = TradeRequest(
            underlying_instrument=request.stock_symbol,
            order_type=request.order_type,
            quantity=request.quantity,
            price=request.price,  # None for market orders
            stop_loss_price=None
        )
        
        # Step 3: Place order through IIFL Interactive API
        # Use the actual instrument details from search instead of hardcoded fallbacks
        iifl_service = IIFLService(db)
        
        # Create a custom trade request with the actual instrument details
        # This bypasses the hardcoded instrument lookup in the IIFL service
        custom_trade_request = TradeRequest(
            underlying_instrument=request.stock_symbol,
            order_type=request.order_type,
            quantity=request.quantity,
            price=request.price,
            stop_loss_price=None
        )
        
        # Store the actual instrument details for use in order placement
        actual_instrument_details = {
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": exchange_instrument_id,
            "instrumentType": "CASH"
        }
        
        # Place the order using the actual instrument details
        order_result = iifl_service.place_order_with_details(db, current_user.id, custom_trade_request, actual_instrument_details)
        
        # Add debug logging
        print(f"DEBUG: Order placed for {request.stock_symbol} with actual instrument ID {exchange_instrument_id}")
        print(f"DEBUG: Order result: {order_result}")
        
        # Extract order details
        result_data = order_result.get("result", {})
        order_id = result_data.get("AppOrderID")
        order_status = result_data.get("OrderStatus", "NEW")
        
        # Log the order_id type for debugging
        print(f"DEBUG: order_id type: {type(order_id)}, value: {order_id}")
        
        if not order_id:
            # Generate fallback order ID
            import time
            order_id = f"local_{current_user.id}_{int(time.time())}"
        
        # Step 4: Save trade to database
        trade = Trade(
            user_id=current_user.id,
            order_id=str(order_id),
            underlying_instrument=request.stock_symbol,
            option_type=None,  # Not applicable for equity stocks
            strike_price=None,  # Not applicable for equity stocks
            expiry_date=None,   # Not applicable for equity stocks
            order_type=request.order_type,
            quantity=request.quantity,
            price=request.price,
            order_status=order_status,
            stop_loss_price=None
        )
        
        db.add(trade)
        db.commit()
        db.refresh(trade)
        
        # Ensure order_id is a string
        order_id_str = str(order_id) if order_id else f"local_{current_user.id}_{int(time.time())}"
        
        # Step 5: Return success response
        return BuyStockResponse(
            status="success",
            message=f"{request.order_type} order placed successfully for {request.quantity} shares of {request.stock_symbol}",
            order_id=order_id_str,  # Use the string version
            trade_id=trade.id,
            stock_info={
                "symbol": request.stock_symbol,
                "name": stock_info.get("DisplayName", request.stock_symbol),
                "instrument_id": exchange_instrument_id,
                "lot_size": stock_info.get("LotSize"),
                "current_price": current_price
            },
            order_details={
                "quantity": request.quantity,
                "price": request.price if request.price else "MARKET",
                "order_type": request.order_type,
                "order_status": order_status
            },
            timestamp=datetime.now().isoformat()
        )
        
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to place {request.order_type} order: {str(e)}"
        )

@router.get("/exchange-status")
async def get_exchange_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Check if exchanges are connected and available for trading"""
    try:
        # Initialize IIFL Connect for interactive API
        iifl_client = IIFLConnect(current_user, api_type="interactive")
        
        # Login to get token
        login_response = iifl_client.interactive_login()
        if login_response.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to authenticate with IIFL Interactive API"
            )
        
        # Check exchange status
        status_response = iifl_client.get_exchange_status()
        
        # Logout
        iifl_client.interactive_logout()
        
        return {
            "status": "success",
            "exchange_status": status_response,
            "message": "Exchange status retrieved successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to get exchange status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get exchange status: {str(e)}"
        )

@router.get("/stock-quote/{stock_symbol}", response_model=StockQuoteResponse)
async def get_stock_quote(
    stock_symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get real-time quote for a specific stock
    
    This endpoint provides:
    - Current market price (LTP)
    - Bid/Ask prices
    - Volume and other market data
    - Stock information (lot size, tick size)
    
    Path parameter:
    - stock_symbol: Stock symbol (e.g., "RELIANCE", "TCS")
    
    Returns real-time market data for the specified stock
    """
    if not current_user.iifl_market_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Market Data credentials not configured"
        )
    
    stock_symbol = stock_symbol.strip().upper()
    if not stock_symbol:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Stock symbol is required"
        )
    
    try:
        # Initialize IIFL Connect for market data
        iifl_client = IIFLConnect(current_user, api_type="market")
        
        # Login to get token
        login_response = iifl_client.marketdata_login()
        if login_response.get("type") != "success":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to authenticate with IIFL Market Data API"
            )
        
        # Search for the stock
        search_response = iifl_client.search_by_scriptname(stock_symbol)
        
        if search_response.get("type") != "success" or not search_response.get("result"):
            iifl_client.marketdata_logout()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock '{stock_symbol}' not found"
            )
        
        # Filter for equity stocks
        stocks = search_response["result"]
        equity_stocks = [s for s in stocks if s.get("ExchangeSegment") == 1 and s.get("Series") == "EQ"]
        
        if not equity_stocks:
            iifl_client.marketdata_logout()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock '{stock_symbol}' is not available for trading"
            )
        
        stock_info = equity_stocks[0]
        exchange_segment = stock_info.get("ExchangeSegment", 1)
        exchange_instrument_id = stock_info.get("ExchangeInstrumentID")
        
        # Get real-time market data
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
        
        # Extract current price and market data
        current_price = None
        market_data = {}
        
        try:
            if touchline_response.get("result", {}).get("listQuotes"):
                quotes_data = touchline_response["result"]["listQuotes"]
                if isinstance(quotes_data, list) and len(quotes_data) > 0:
                    first_quote = quotes_data[0]
                    if isinstance(first_quote, dict):
                        current_price = first_quote.get("Touchline", {}).get("LastTradedPrice")
                        market_data = first_quote.get("Touchline", {})
                    elif isinstance(first_quote, str):
                        try:
                            import json
                            parsed_quote = json.loads(first_quote)
                            current_price = parsed_quote.get("LastTradedPrice")
                            market_data = parsed_quote
                        except:
                            pass
        except Exception:
            pass
        
        # Logout
        iifl_client.marketdata_logout()
        
        # Compile response
        response_data = StockQuoteResponse(
            type="success",
            stock_info={
                "symbol": stock_symbol,
                "name": stock_info.get("DisplayName", stock_symbol),
                "exchange_segment": stock_info.get("ExchangeSegment"),
                "instrument_id": exchange_instrument_id,
                "series": stock_info.get("Series"),
                "isin": stock_info.get("ISIN"),
                "lot_size": stock_info.get("LotSize"),
                "tick_size": stock_info.get("TickSize")
            },
            market_data={
                "current_price": current_price,
                "touchline": market_data,
                "market_depth": market_depth_response.get("result", {})
            },
            timestamp=datetime.now().isoformat()
        )
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stock quote for '{stock_symbol}': {str(e)}"
        )
