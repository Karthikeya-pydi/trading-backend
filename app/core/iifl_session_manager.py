from typing import Dict, Optional, Literal
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from fastapi import HTTPException
import asyncio
import threading
from contextlib import asynccontextmanager

from app.models.user import User
from app.services.iifl_connect import IIFLConnect
from app.core.security import decrypt_data

class IIFLSessionManager:
    """Manages IIFL sessions and handles automatic refresh"""
    
    def __init__(self):
        # Store active sessions with metadata
        self._sessions: Dict[str, Dict] = {}
        self._lock = threading.Lock()
    
    def _get_cache_key(self, user_id: int, api_type: Literal["market", "interactive"]) -> str:
        """Generate cache key for session storage"""
        return f"{user_id}_{api_type}"
    
    def _is_session_valid(self, session_data: Dict) -> bool:
        """Check if session is still valid based on creation time"""
        if not session_data or not session_data.get("token"):
            return False
        
        # IIFL sessions typically expire after 24 hours
        # We'll refresh them every 12 hours to be safe
        created_at = session_data.get("created_at")
        if not created_at:
            return False
        
        # Check if session is older than 12 hours
        return datetime.now() - created_at < timedelta(hours=12)
    
    def _create_iifl_session(self, user: User, api_type: Literal["market", "interactive"]) -> Dict:
        """Create new IIFL session for user"""
        try:
            client = IIFLConnect(user, api_type)
            
            if api_type == "interactive":
                login_response = client.interactive_login()
            else:  # market
                login_response = client.marketdata_login()
            
            if login_response.get("type") != "success":
                raise Exception(f"IIFL {api_type} login failed: {login_response.get('description', 'Unknown error')}")
            
            token = login_response["result"]["token"]
            
            session_data = {
                "token": token,
                "client": client,
                "created_at": datetime.now(),
                "api_type": api_type,
                "user_id": user.id
            }
            
            return session_data
            
        except Exception as e:
            raise HTTPException(
                status_code=401,
                detail=f"Failed to create IIFL {api_type} session: {str(e)}"
            )
    
    def get_session(self, db: Session, user_id: int, api_type: Literal["market", "interactive"]) -> Dict:
        """Get valid IIFL session, creating or refreshing if needed"""
        cache_key = self._get_cache_key(user_id, api_type)
        
        with self._lock:
            # Check if we have a valid cached session
            if cache_key in self._sessions:
                session_data = self._sessions[cache_key]
                if self._is_session_valid(session_data):
                    return session_data
                else:
                    # Remove expired session
                    del self._sessions[cache_key]
            
            # Get user and create new session
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Check if user has credentials for this API type
            if api_type == "interactive":
                if not user.iifl_interactive_api_key:
                    raise HTTPException(
                        status_code=400,
                        detail="IIFL Interactive credentials not configured"
                    )
            else:  # market
                if not user.iifl_market_api_key:
                    raise HTTPException(
                        status_code=400,
                        detail="IIFL Market credentials not configured"
                    )
            
            # Create new session
            session_data = self._create_iifl_session(user, api_type)
            self._sessions[cache_key] = session_data
            
            return session_data
    
    def refresh_session(self, db: Session, user_id: int, api_type: Literal["market", "interactive"]) -> Dict:
        """Force refresh IIFL session"""
        cache_key = self._get_cache_key(user_id, api_type)
        
        with self._lock:
            # Remove existing session
            if cache_key in self._sessions:
                del self._sessions[cache_key]
            
            # Get user and create new session
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Create new session
            session_data = self._create_iifl_session(user, api_type)
            self._sessions[cache_key] = session_data
            
            return session_data
    
    def invalidate_user_sessions(self, user_id: int):
        """Invalidate all sessions for a user (e.g., on logout)"""
        with self._lock:
            keys_to_remove = []
            for key, session_data in self._sessions.items():
                if session_data.get("user_id") == user_id:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._sessions[key]
    
    def get_session_token(self, db: Session, user_id: int, api_type: Literal["market", "interactive"]) -> str:
        """Get IIFL session token, refreshing if needed"""
        session_data = self.get_session(db, user_id, api_type)
        return session_data["token"]
    
    def get_session_client(self, db: Session, user_id: int, api_type: Literal["market", "interactive"]) -> IIFLConnect:
        """Get IIFL session client, refreshing if needed"""
        session_data = self.get_session(db, user_id, api_type)
        return session_data["client"]

# Global session manager instance
iifl_session_manager = IIFLSessionManager()
