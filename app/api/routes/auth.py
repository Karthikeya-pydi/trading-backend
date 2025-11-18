from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import datetime, timezone
import httpx
# from loguru import logger  # ← DISABLED FOR VERCEL

from app.core.database import get_db, SessionLocal
from app.core.config import settings
from app.core.jwt import AuthJWT, get_auth_jwt
from app.core.errors import auth_http_error, AuthErrorCode
from app.models.user import User
from app.schemas.auth import Token, UserProfile
from app.api.dependencies import get_current_user

router = APIRouter()

@router.get("/oauth/google/login")
async def google_login(
    redirect_uri: str = Query(None, description="Frontend callback URL"),
    state: str = Query(None, description="State parameter for frontend callback")
):
    """Redirect to Google OAuth login"""
    if not settings.google_client_id or not settings.google_redirect_uri:
        # Give a more helpful error message
        return {
            "error": "OAuth configuration missing",
            "message": "Google OAuth is not configured. Please set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in your environment variables.",
            "current_config": {
                "google_client_id": settings.google_client_id,
                "google_redirect_uri": settings.google_redirect_uri
            }
        }

    # Use frontend URL from environment variable
    frontend_callback = f"{settings.frontend_url}/auth/callback"
    
    auth_url = (
        f"{settings.google_auth_url}?"
        f"client_id={settings.google_client_id}&"
        f"redirect_uri={settings.google_redirect_uri}&"
        f"response_type=code&"
        f"scope=openid%20profile%20email&"
        f"state={frontend_callback}"  # Pass frontend callback in state
    )
    return RedirectResponse(auth_url)

@router.get("/oauth/google/callback")
async def google_callback(
    db: Session = Depends(get_db),
    code: str = Query(None),
    state: str = Query(None, description="Frontend callback URL"),
    authorize: AuthJWT = Depends(get_auth_jwt),
):
    """Handle Google OAuth callback and redirect back to frontend"""
    
    # Use frontend URL from environment variable
    frontend_callback_url = f"{settings.frontend_url}/auth/callback"
    
    if not code:
        # Redirect with error
        return RedirectResponse(f"{frontend_callback_url}?error=Authorization code not provided")
    
    try:
        async with httpx.AsyncClient() as client:
            # Exchange code for access token
            token_response = await client.post(
                settings.google_token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "redirect_uri": settings.google_redirect_uri,
                },
            )
            
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise Exception("Failed to retrieve access token from Google")

            # Get user info from Google
            userinfo_response = await client.get(
                settings.google_userinfo_url,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            
            user_info = userinfo_response.json()
            email = user_info.get("email")
            name = user_info.get("name")
            google_id = user_info.get("id")
            picture = user_info.get("picture")
            
            if not email:
                raise Exception("Could not retrieve email from Google")

            # Find or create user with retry logic for database connection issues
            try:
                user = db.query(User).filter(User.email == email).first()
                if not user:
                    print(f"User not found, creating a new user: {email}")  # Use print instead of logger
                    user = User(
                        email=email,
                        name=name,
                        google_id=google_id,
                        profile_picture=picture,
                        is_verified=True,
                    )
                    db.add(user)
                else:
                    # Update user info from Google
                    user.name = name or user.name
                    user.google_id = google_id or user.google_id
                    user.profile_picture = picture or user.profile_picture
                    user.is_verified = True
                
                user.last_login_at = datetime.now(timezone.utc)
                db.commit()
                db.refresh(user)
                
            except Exception as db_error:
                print(f"Database error during user lookup/creation: {db_error}")
                db.rollback()
                # Try to reconnect and retry once
                try:
                    with SessionLocal() as retry_db:
                        retry_user = retry_db.query(User).filter(User.email == email).first()
                        if not retry_user:
                            retry_user = User(
                                email=email,
                                name=name,
                                google_id=google_id,
                                profile_picture=picture,
                                is_verified=True,
                            )
                            retry_db.add(retry_user)
                        else:
                            retry_user.name = name or retry_user.name
                            retry_user.google_id = google_id or retry_user.google_id
                            retry_user.profile_picture = picture or retry_user.profile_picture
                            retry_user.is_verified = True
                        
                        retry_user.last_login_at = datetime.now(timezone.utc)
                        retry_db.commit()
                        retry_db.refresh(retry_user)
                        user = retry_user
                except Exception as retry_error:
                    print(f"Database retry failed: {retry_error}")
                    raise Exception(f"Database connection failed: {retry_error}")

            # Create JWT tokens
            jwt_access_token = authorize.create_access_token(subject=email)
            refresh_token = authorize.create_refresh_token(subject=email)
            
            # ✅ CRITICAL: Redirect back to frontend with both tokens
            print(f"OAuth successful, redirecting to: {frontend_callback_url}")
            return RedirectResponse(f"{frontend_callback_url}?access_token={jwt_access_token}&refresh_token={refresh_token}")
            
    except Exception as e:
        print(f"OAuth error: {str(e)}")  # Use print instead of logger
        # Redirect with error
        return RedirectResponse(f"{frontend_callback_url}?error=Authentication failed: {str(e)}")

@router.post("/refresh")
async def refresh_token(
    refresh_token: str,
    db: Session = Depends(get_db),
    authorize: AuthJWT = Depends(get_auth_jwt),
):
    """Refresh access token using refresh token"""
    try:
        # Verify refresh token properly
        email = authorize.verify_refresh_token(refresh_token)
        
        user = db.query(User).filter(User.email == email).first()
        
        if not user:
            raise auth_http_error(AuthErrorCode.USER_NOT_FOUND)
        
        # Create new access token
        new_access_token = authorize.create_access_token(subject=email)
        
        return {
            "access_token": new_access_token,
            "refresh_token": refresh_token,  # Return the same refresh token
            "token_type": "bearer"
        }
        
    except Exception:
        raise auth_http_error(AuthErrorCode.INVALID_TOKEN)


@router.post("/logout")
async def logout(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Logout user and clear IIFL sessions"""
    from app.core.iifl_session_manager import iifl_session_manager
    
    # Clear IIFL sessions for the user
    iifl_session_manager.invalidate_user_sessions(current_user.id)
    
    return {"message": "Successfully logged out and IIFL sessions cleared"}
