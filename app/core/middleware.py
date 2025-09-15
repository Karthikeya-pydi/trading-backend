from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable
import json

class TokenRefreshMiddleware(BaseHTTPMiddleware):
    """Middleware to automatically add refreshed access tokens to response headers"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Check if a new access token was generated during request processing
        if hasattr(request.state, 'new_access_token'):
            # Add the new access token to response headers
            response.headers["X-New-Access-Token"] = request.state.new_access_token
            response.headers["X-Token-Refreshed"] = "true"
        
        # Check if IIFL sessions were refreshed
        if hasattr(request.state, 'iifl_sessions_refreshed'):
            response.headers["X-IIFL-Sessions-Refreshed"] = "true"
        
        return response
