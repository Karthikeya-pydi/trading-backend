from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, JSON
from sqlalchemy.sql import func
from app.core.database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=True)  # Will be populated from Google profile
    google_id = Column(String, unique=True, index=True, nullable=True)
    profile_picture = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # IIFL API credentials (encrypted) - Market Data
    iifl_market_api_key = Column(Text, nullable=True)
    iifl_market_secret_key = Column(Text, nullable=True)
    iifl_market_user_id = Column(String, nullable=True)
    
    # IIFL API credentials (encrypted) - Interactive (Trading)
    iifl_interactive_api_key = Column(Text, nullable=True)
    iifl_interactive_secret_key = Column(Text, nullable=True)
    iifl_interactive_user_id = Column(String, nullable=True)
    
    # Trading preferences
    trading_preferences = Column(JSON, default=dict)
