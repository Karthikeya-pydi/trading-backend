from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from jose import JWTError, jwt
from fastapi import HTTPException, status
from app.core.config import settings
from app.core.iifl_session_manager import iifl_session_manager

class AuthJWT:
    def __init__(self):
        self.secret_key = settings.jwt_secret_key
        self.algorithm = settings.jwt_algorithm
        self.access_token_expire_minutes = settings.access_token_expire_minutes
    
    def create_access_token(self, subject: str, expires_delta: Optional[timedelta] = None) -> str:
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(minutes=self.access_token_expire_minutes)
        
        to_encode = {"exp": expire, "sub": subject}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def create_refresh_token(self, subject: str) -> str:
        expire = datetime.now(timezone.utc) + timedelta(days=30)  # Refresh token valid for 30 days
        to_encode = {"exp": expire, "sub": subject, "type": "refresh"}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def verify_token(self, token: str) -> str:
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            email: str = payload.get("sub")
            if email is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Could not validate credentials"
                )
            return email
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )
    
    def verify_refresh_token(self, token: str) -> str:
        """
        Verify that the provided token is a refresh token and return the subject (email).
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            if payload.get("type") != "refresh":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid refresh token"
                )
            
            email: str = payload.get("sub")
            if email is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Could not validate credentials"
                )
            return email
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token is invalid or expired"
            )
    
    def verify_token_with_refresh(self, token: str, refresh_token: Optional[str] = None, db: Optional[object] = None) -> Tuple[str, Optional[str], Optional[bool]]:
        """
        Verify access token, and if expired, try to refresh using refresh token.
        Also refreshes IIFL sessions if needed.
        Returns (email, new_access_token, iifl_refreshed) where:
        - new_access_token is None if no refresh needed
        - iifl_refreshed is True if IIFL sessions were refreshed
        """
        try:
            # First try to verify the access token
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            email: str = payload.get("sub")
            if email is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Could not validate credentials"
                )
            return email, None, False  # Token is valid, no refresh needed
        except JWTError:
            # Access token is invalid/expired, try to refresh
            if refresh_token:
                try:
                    # Verify refresh token
                    refresh_payload = jwt.decode(refresh_token, self.secret_key, algorithms=[self.algorithm])
                    if refresh_payload.get("type") != "refresh":
                        raise HTTPException(
                            status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid refresh token type"
                        )
                    
                    email: str = refresh_payload.get("sub")
                    if email is None:
                        raise HTTPException(
                            status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Could not validate refresh token"
                        )
                    
                    # Create new access token
                    new_access_token = self.create_access_token(subject=email)
                    
                    # Refresh IIFL sessions if database is available
                    iifl_refreshed = False
                    if db:
                        try:
                            # Get user from email
                            from app.models.user import User
                            user = db.query(User).filter(User.email == email).first()
                            if user:
                                # Refresh both market and interactive sessions
                                try:
                                    iifl_session_manager.refresh_session(db, user.id, "market")
                                    iifl_refreshed = True
                                except:
                                    pass  # Market session refresh failed, continue
                                
                                try:
                                    iifl_session_manager.refresh_session(db, user.id, "interactive")
                                    iifl_refreshed = True
                                except:
                                    pass  # Interactive session refresh failed, continue
                        except Exception:
                            # IIFL refresh failed, but JWT refresh succeeded
                            pass
                    
                    return email, new_access_token, iifl_refreshed
                except JWTError:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Refresh token is invalid or expired"
                    )
            else:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Access token expired and no refresh token provided"
                )

def get_auth_jwt():
    return AuthJWT()
