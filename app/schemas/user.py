from pydantic import BaseModel, EmailStr
from typing import Optional, Dict, Any
from datetime import datetime

class UserBase(BaseModel):
    email: EmailStr
    name: str

class UserCreate(UserBase):
    google_id: str
    profile_picture: Optional[str] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None
    trading_preferences: Optional[Dict[str, Any]] = None

class IIFLMarketCredentials(BaseModel):
    api_key: str
    secret_key: str
    user_id: str

class IIFLInteractiveCredentials(BaseModel):
    api_key: str
    secret_key: str
    user_id: str

class IIFLCredentials(BaseModel):
    market: IIFLMarketCredentials
    interactive: IIFLInteractiveCredentials

class User(UserBase):
    id: int
    google_id: str
    profile_picture: Optional[str] = None
    is_active: bool
    has_iifl_market_credentials: bool = False
    has_iifl_interactive_credentials: bool = False
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True
