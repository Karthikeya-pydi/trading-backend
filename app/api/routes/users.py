from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Literal
from pydantic import BaseModel

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.schemas.user import IIFLCredentials, User as UserSchema
from app.core.security import encrypt_data

router = APIRouter()

class SingleIIFLCredentials(BaseModel):
    api_type: Literal["market", "interactive"]
    api_key: str
    secret_key: str
    user_id: str

@router.get("/me", response_model=UserSchema)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user information"""
    user_response = UserSchema.from_orm(current_user)
    user_response.has_iifl_market_credentials = bool(current_user.iifl_market_api_key)
    user_response.has_iifl_interactive_credentials = bool(current_user.iifl_interactive_api_key)
    return user_response

@router.post("/set-iifl-credentials")
async def set_iifl_credentials(
    credentials: SingleIIFLCredentials,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Set IIFL API credentials for a specific API type (market or interactive)"""
    try:
        if credentials.api_type == "market":
            # Encrypt and save Market credentials
            current_user.iifl_market_api_key = encrypt_data(credentials.api_key)
            current_user.iifl_market_secret_key = encrypt_data(credentials.secret_key)
            current_user.iifl_market_user_id = credentials.user_id
            message = "IIFL Market Data credentials saved successfully"
            
        elif credentials.api_type == "interactive":
            # Encrypt and save Interactive credentials
            current_user.iifl_interactive_api_key = encrypt_data(credentials.api_key)
            current_user.iifl_interactive_secret_key = encrypt_data(credentials.secret_key)
            current_user.iifl_interactive_user_id = credentials.user_id
            message = "IIFL Interactive credentials saved successfully"
        
        db.commit()
        
        return {
            "message": message,
            "api_type": credentials.api_type,
            "user_id": credentials.user_id
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save {credentials.api_type} credentials: {str(e)}"
        )

@router.post("/iifl-credentials")
async def save_iifl_credentials(
    credentials: IIFLCredentials,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Save IIFL API credentials for the user"""
    try:
        # Encrypt and save Market credentials
        current_user.iifl_market_api_key = encrypt_data(credentials.market.api_key)
        current_user.iifl_market_secret_key = encrypt_data(credentials.market.secret_key)
        current_user.iifl_market_user_id = credentials.market.user_id
        
        # Encrypt and save Interactive credentials
        current_user.iifl_interactive_api_key = encrypt_data(credentials.interactive.api_key)
        current_user.iifl_interactive_secret_key = encrypt_data(credentials.interactive.secret_key)
        current_user.iifl_interactive_user_id = credentials.interactive.user_id
        
        db.commit()
        
        return {"message": "IIFL credentials saved successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save credentials: {str(e)}"
        )

@router.delete("/iifl-credentials")
async def delete_iifl_credentials(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete IIFL API credentials"""
    # Clear Market credentials
    current_user.iifl_market_api_key = None
    current_user.iifl_market_secret_key = None
    current_user.iifl_market_user_id = None
    
    # Clear Interactive credentials
    current_user.iifl_interactive_api_key = None
    current_user.iifl_interactive_secret_key = None
    current_user.iifl_interactive_user_id = None
    
    db.commit()
    
    return {"message": "IIFL credentials deleted successfully"}
