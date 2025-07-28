from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.models.trade import Trade, Position
from app.schemas.trading import TradeRequest, TradeResponse, PositionResponse
from app.services.iifl_service_fixed import IIFLServiceFixed

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
        iifl_service = IIFLServiceFixed(db)
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
        iifl_service = IIFLServiceFixed(db)
        
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
            message=f"Order placed successfully. Order ID: {order_id}"
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
        iifl_service = IIFLServiceFixed(db)
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

@router.get("/order-book")
async def get_order_book(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user's order book from IIFL"""
    if not current_user.iifl_interactive_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IIFL Interactive credentials not configured"
        )
    
    try:
        # Use the fixed IIFL service
        iifl_service = IIFLServiceFixed(db)
        order_book = iifl_service.get_order_book(db, current_user.id)
        return order_book
        
    except Exception as e:
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
        iifl_service = IIFLServiceFixed(db)
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
        iifl_service = IIFLServiceFixed(db)
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
        iifl_service = IIFLServiceFixed(db)
        order_result = iifl_service.place_order(db, current_user.id, square_off_request)
        
        return {"status": "success", "message": "Square off order placed successfully"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to square off position: {str(e)}"
        )
