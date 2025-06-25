from enum import Enum
from fastapi import HTTPException, status

class AuthErrorCode(Enum):
    OAUTH_CONFIG_MISSING = "oauth_config_missing"
    INVALID_TOKEN = "invalid_token"
    USER_NOT_FOUND = "user_not_found"

def auth_http_error(error_code: AuthErrorCode):
    error_messages = {
        AuthErrorCode.OAUTH_CONFIG_MISSING: "OAuth configuration is missing",
        AuthErrorCode.INVALID_TOKEN: "Invalid or expired token",
        AuthErrorCode.USER_NOT_FOUND: "User not found"
    }
    
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=error_messages.get(error_code, "Authentication error")
    )
