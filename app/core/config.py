from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    # Database - defaults to SQLite if not provided
    database_url: str = "sqlite:///./trading_platform.db"
    
    # Database connection settings
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_recycle: int = 3600
    database_connect_timeout: int = 10
    database_sslmode: str = "prefer"  # prefer, require, disable
    
    # Redis - defaults to a simple in-memory store if not available
    redis_url: str = "redis://localhost:6379"
    
    # JWT
    jwt_secret_key: str = "your-secret-key-change-this-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 480  # 8 hours - more reasonable for trading platform
    
    # Google OAuth
    google_client_id: str = "your_google_client_id"  # Set this in .env file
    google_client_secret: str = "your_google_client_secret"  # Set this in .env file
    google_redirect_uri: str = "http://127.0.0.1:8000/api/auth/oauth/google/callback"
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

    # backend_url: str = "http://trading-backend-oab.info:8000"
    # frontend_url: str = "https://trading-frontend-3enh.vercel.app"
    backend_url: str = "http://127.0.0.1:8000"  # Backend runs on port 8000
    frontend_url: str = "http://localhost:3000"  # Frontend runs on port 3000
    
    # AWS S3 settings
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "ap-south-1"
    s3_bucket_name: str = "trading-platform-csvs"

    input_aws_access_key_id: Optional[str] = None
    input_aws_secret_access_key: Optional[str] = None
    aws_region: str = "ap-south-1"
    input_s3_bucket_name: str = "trading-platform-csvs"
    
    # Azure OpenAI settings (Azure API Management)
    # Your endpoint format: https://oab-sophius-devtest-01.azure-api.net/karthikeya.chowdary/v1/openai/deployments/{deployment-id}/chat/completions?api-version={api-version}
    # 
    # Required .env variables:
    # AZURE_OPENAI_API_KEY=your_api_key_here
    # AZURE_OPENAI_ENDPOINT=https://oab-sophius-devtest-01.azure-api.net/karthikeya.chowdary/v1
    # AZURE_OPENAI_DEPLOYMENT_NAME=your-deployment-id
    # AZURE_OPENAI_API_VERSION=2024-02-15-preview (or your API version)
    #
    # Optional .env variables:
    # AZURE_OPENAI_MODEL=gpt-4o (default, not used if deployment_name is set)
    # LLM_TEMPERATURE=0.3 (default)
    # LLM_MAX_TOKENS=2000 (default)
    # CHAT_HISTORY_LIMIT=10 (default)
    azure_openai_api_key: Optional[str] = Field(None, alias="AZURE_OPENAI_API_KEY")
    azure_openai_endpoint: Optional[str] = Field(None, alias="AZURE_OPENAI_ENDPOINT")  # Base endpoint without /openai (e.g., https://oab-sophius-devtest-01.azure-api.net/karthikeya.chowdary/v1)
    azure_openai_api_version: str = Field("2024-02-15-preview", alias="AZURE_OPENAI_API_VERSION")  # API version for query param
    azure_openai_model: str = Field("gpt-4o", alias="AZURE_OPENAI_MODEL")  # Model name (not used if deployment_name is set)
    azure_openai_deployment_name: Optional[str] = Field(None, alias="AZURE_OPENAI_DEPLOYMENT_NAME")  # Deployment ID (REQUIRED - this is your deployment-id)
    llm_temperature: float = Field(0.3, alias="LLM_TEMPERATURE")
    llm_max_tokens: int = Field(2000, alias="LLM_MAX_TOKENS")
    chat_history_limit: int = Field(10, alias="CHAT_HISTORY_LIMIT")  # Number of previous messages to include in context
    
    class Config:
        env_file = ".env"
        populate_by_name = True  # Allow both field name and alias to work
        case_sensitive = False  # Case-insensitive for environment variables

settings = Settings()
