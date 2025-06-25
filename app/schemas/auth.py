from pydantic import BaseModel
from typing import Optional

class Token(BaseModel):
    access_token: str
    refresh_token: str
    user_id: str
    token_type: str = "bearer"

class UserProfile(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    profile_picture: Optional[str] = None
    is_verified: bool
    has_iifl_market_credentials: bool = False
    has_iifl_interactive_credentials: bool = False
