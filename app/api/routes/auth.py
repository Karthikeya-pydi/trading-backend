from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import datetime, timezone
import httpx
from loguru import logger

from app.core.database import get_db
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
        raise auth_http_error(AuthErrorCode.OAUTH_CONFIG_MISSING)

    # Use the frontend callback URL if provided, otherwise default
    frontend_callback = state or redirect_uri or "http://localhost:3001/auth/callback"
    
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
    
    # Determine frontend callback URL
    frontend_callback_url = state or "http://localhost:3001/auth/callback"
    
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

            # Find or create user
            user = db.query(User).filter(User.email == email).first()
            if not user:
                logger.info(f"User not found, creating a new user: {email}")
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

            # Create JWT tokens
            jwt_access_token = authorize.create_access_token(subject=email)
            refresh_token = authorize.create_refresh_token(subject=email)
            
            # ✅ CRITICAL: Redirect back to frontend with token
            logger.info(f"OAuth successful, redirecting to: {frontend_callback_url}")
            return RedirectResponse(f"{frontend_callback_url}?token={jwt_access_token}")
            
    except Exception as e:
        logger.error(f"OAuth error: {str(e)}")
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
        email = authorize.verify_token(refresh_token)
        user = db.query(User).filter(User.email == email).first()
        
        if not user:
            raise auth_http_error(AuthErrorCode.USER_NOT_FOUND)
        
        # Create new access token
        new_access_token = authorize.create_access_token(subject=email)
        
        return {
            "access_token": new_access_token,
            "token_type": "bearer"
        }
        
    except Exception:
        raise auth_http_error(AuthErrorCode.INVALID_TOKEN)

@router.get("/me", response_model=UserProfile)
async def get_current_user_profile(
    current_user: User = Depends(get_current_user)
):
    """Get current user profile"""
    return UserProfile(
        id=current_user.id,
        email=current_user.email,
        name=current_user.name,
        profile_picture=current_user.profile_picture,
        is_verified=current_user.is_verified,
        has_iifl_market_credentials=bool(current_user.iifl_market_api_key),
        has_iifl_interactive_credentials=bool(current_user.iifl_interactive_api_key)
    )

@router.post("/logout")
async def logout():
    """Logout user (client should remove tokens)"""
    return {"message": "Successfully logged out"}