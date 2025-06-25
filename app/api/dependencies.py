from fastapi import Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.models.user import User
from app.core.jwt import AuthJWT, get_auth_jwt

security = HTTPBearer()

async def get_current_user(
    token: str = Depends(security),
    db: Session = Depends(get_db),
    authorize: AuthJWT = Depends(get_auth_jwt)
) -> User:
    """Get current authenticated user"""
    email = authorize.verify_token(token.credentials)
    user = db.query(User).filter(User.email == email).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    
    return user

async def get_current_user_websocket(
    token: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    authorize: AuthJWT = Depends(get_auth_jwt)
) -> Optional[User]:
    """Get current authenticated user for WebSocket connections"""
    if not token:
        return None
        
    try:
        email = authorize.verify_token(token)
        user = db.query(User).filter(User.email == email).first()
        return user if user and user.is_active else None
    except:
        return None
