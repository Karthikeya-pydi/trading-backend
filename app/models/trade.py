from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, ForeignKey, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base

class Trade(Base):
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Order details
    order_id = Column(String, unique=True, index=True)
    exchange_order_id = Column(String, nullable=True)
    
    # Instrument details
    underlying_instrument = Column(String, nullable=False)  # NIFTY, BANKNIFTY, etc.
    option_type = Column(String, nullable=True)  # CALL, PUT (null for futures/stocks)
    strike_price = Column(Float, nullable=True)
    expiry_date = Column(DateTime, nullable=True)
    
    # Trade details
    order_type = Column(String, nullable=False)  # BUY, SELL
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=True)
    order_status = Column(String, nullable=False)  # NEW, FILLED, CANCELLED, etc.
    
    # Execution details
    filled_quantity = Column(Integer, default=0)
    average_price = Column(Float, nullable=True)
    
    # Stop loss details
    stop_loss_price = Column(Float, nullable=True)
    stop_loss_order_id = Column(String, nullable=True)
    stop_loss_triggered = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    executed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Additional data (renamed from metadata to avoid SQLAlchemy conflict)
    additional_data = Column(JSON, default={})
    
    # Relationships
    user = relationship("User")

class Position(Base):
    __tablename__ = "positions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Instrument details
    underlying_instrument = Column(String, nullable=False)
    option_type = Column(String, nullable=True)
    strike_price = Column(Float, nullable=True)
    expiry_date = Column(DateTime, nullable=True)
    
    # Position details
    quantity = Column(Integer, nullable=False)  # Positive for long, negative for short
    average_price = Column(Float, nullable=False)
    current_price = Column(Float, nullable=True)
    unrealized_pnl = Column(Float, default=0.0)
    
    # Stop loss
    stop_loss_price = Column(Float, nullable=True)
    stop_loss_active = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    user = relationship("User")
