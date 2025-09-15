from fastapi import Depends, HTTPException, status, Query, Request
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.models.user import User
from app.core.jwt import AuthJWT, get_auth_jwt

security = HTTPBearer()

async def get_current_user(
    request: Request,
    token: str = Depends(security),
    db: Session = Depends(get_db),
    authorize: AuthJWT = Depends(get_auth_jwt)
) -> User:
    """Get current authenticated user with automatic token refresh"""
    # Get refresh token from request headers
    refresh_token = request.headers.get("X-Refresh-Token")
    
    try:
        # Try to verify token with automatic refresh
        email, new_access_token, iifl_refreshed = authorize.verify_token_with_refresh(
            token.credentials, 
            refresh_token,
            db
        )
        
        # If a new access token was generated, add it to response headers
        if new_access_token:
            # Store the new token in request state for the response
            request.state.new_access_token = new_access_token
            
        # If IIFL sessions were refreshed, add that info to response
        if iifl_refreshed:
            request.state.iifl_sessions_refreshed = True
            
    except HTTPException:
        # If automatic refresh fails, fall back to regular verification
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
