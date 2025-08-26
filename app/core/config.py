from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database - defaults to SQLite if not provided
    database_url: str = "sqlite:///./trading_platform.db"
    
    # Redis - defaults to a simple in-memory store if not available
    redis_url: str = "redis://localhost:6379"
    
    # JWT
    jwt_secret_key: str = "your-secret-key-change-this-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # Google OAuth
    google_client_id: str = "your_google_client_id"
    google_client_secret: str = "your_google_client_secret"
    google_redirect_uri: str = "http://localhost:8000/api/auth/oauth/google/callback"
    google_auth_url: str = "https://accounts.google.com/o/oauth2/v2/auth"
    google_token_url: str = "https://oauth2.googleapis.com/token"
    google_userinfo_url: str = "https://www.googleapis.com/oauth2/v1/userinfo"
    
    # App settings
    app_name: str = "Trading Platform"
    debug: bool = True
    
    # XTS API settings
    XTS_ROOT_URI: str = "https://api.xts.com"  # Replace with actual XTS API URL
    XTS_TIMEOUT: int = 7
    XTS_DISABLE_SSL: bool = False
    XTS_API_KEY: Optional[str] = None
    XTS_SECRET_KEY: Optional[str] = None
    XTS_SOURCE: str = "WEBAPI"

    # IIFL API settings
    IIFL_ROOT_URI: str = "https://ttblaze.iifl.com"  # IIFL Interactive API URL
    IIFL_TIMEOUT: int = 7
    IIFL_DISABLE_SSL: bool = False
    IIFL_SOURCE: str = "WEBAPI"

    frontend_url: str = "https://trading-frontend-3enh.vercel.app"
    class Config:
        env_file = ".env"

settings = Settings()
