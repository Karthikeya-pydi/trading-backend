from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from pydantic import BaseModel

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.models.user import User
from app.schemas.user import IIFLMarketCredentials, IIFLInteractiveCredentials
from app.services.iifl_service import IIFLService

class CredentialValidationRequest(BaseModel):
    market_api_key: Optional[str] = None
    market_secret_key: Optional[str] = None
    interactive_api_key: Optional[str] = None
    interactive_secret_key: Optional[str] = None

router = APIRouter()

@router.post("/credentials/market")
async def update_market_credentials(
    credentials: IIFLMarketCredentials,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update user's IIFL market data API credentials"""
    iifl_service = IIFLService(db)
    
    # Validate credentials before saving
    validation = await iifl_service.validate_credentials(
        current_user,
        market_api_key=credentials.api_key,
        market_secret_key=credentials.secret_key
    )
    
    if not validation["market_valid"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid IIFL market data credentials"
        )
    
    # Update user credentials
    user = iifl_service.update_user_credentials(
        current_user,
        market_api_key=credentials.api_key,
        market_secret_key=credentials.secret_key,
        market_user_id=credentials.user_id
    )
    
    return {"message": "IIFL market data credentials updated successfully"}

@router.post("/credentials/interactive")
async def update_interactive_credentials(
    credentials: IIFLInteractiveCredentials,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update user's IIFL interactive trading API credentials"""
    iifl_service = IIFLService(db)
    
    # Validate credentials before saving
    validation = await iifl_service.validate_credentials(
        current_user,
        interactive_api_key=credentials.api_key,
        interactive_secret_key=credentials.secret_key
    )
    
    if not validation["interactive_valid"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid IIFL interactive trading credentials"
        )
    
    # Update user credentials
    user = iifl_service.update_user_credentials(
        current_user,
        interactive_api_key=credentials.api_key,
        interactive_secret_key=credentials.secret_key,
        interactive_user_id=credentials.user_id
    )
    
    return {"message": "IIFL interactive trading credentials updated successfully"}

@router.post("/credentials/validate")
async def validate_credentials(
    request: CredentialValidationRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Validate IIFL API credentials without saving them"""
    iifl_service = IIFLService(db)
    
    validation = await iifl_service.validate_credentials(
        current_user,
        market_api_key=request.market_api_key,
        market_secret_key=request.market_secret_key,
        interactive_api_key=request.interactive_api_key,
        interactive_secret_key=request.interactive_secret_key
    )
    
    return validation 