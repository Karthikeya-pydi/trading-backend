"""
LLM Routes

API endpoints for LLM chat functionality.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Optional
from loguru import logger
import pandas as pd

from app.core.database import get_db
from app.core.config import settings
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.llm.azure_llm_service import AzureLLMService
from app.services.iifl_service import IIFLService, get_iifl_service
from app.services.stock_returns_service import StockReturnsService
from app.services.bhavcopy_service import BhavcopyService
from app.database.chat_repository import ChatRepository
from app.schemas.llm import (
    ChatRequest,
    ChatResponse,
    ChatHistoryResponse,
    UserThreadsResponse,
    ClearConversationResponse
)

router = APIRouter()

# Initialize services
llm_service = AzureLLMService()
returns_service = StockReturnsService()
bhavcopy_service = BhavcopyService()


def get_chat_repository(db: Session = Depends(get_db)) -> ChatRepository:
    """Dependency injection for ChatRepository"""
    return ChatRepository(db)


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    iifl_service: IIFLService = Depends(get_iifl_service),
    chat_repo: ChatRepository = Depends(get_chat_repository)
):
    """
    Main chat endpoint for LLM trader assistant
    
    Args:
        request: Chat request with query and data flags
        current_user: Current authenticated user
        db: Database session
        iifl_service: IIFL service for portfolio data
        chat_repo: Chat repository for history
        
    Returns:
        Chat response with LLM-generated answer
    """
    try:
        # Check if LLM service is available
        if not llm_service.is_available():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LLM service is not available. Please check Azure OpenAI configuration."
            )
        
        # Get or generate thread ID
        thread_id = request.thread_id or chat_repo.generate_thread_id()
        
        # Fetch data based on flags
        portfolio_data = None
        returns_data = None
        bhavcopy_data = None
        
        if request.include_portfolio:
            try:
                if not current_user.iifl_interactive_api_key:
                    logger.warning(f"User {current_user.id} does not have IIFL Interactive credentials")
                    portfolio_data = None
                else:
                    portfolio_data = iifl_service.get_holdings(db, current_user.id)
                    logger.info(f"Fetched portfolio data for user {current_user.id}: {portfolio_data.get('type') if portfolio_data else 'None'}")
                    
                    # Log the structure for debugging
                    if portfolio_data:
                        logger.debug(f"Portfolio data structure: {list(portfolio_data.keys())}")
                        if portfolio_data.get("result"):
                            logger.debug(f"Result type: {type(portfolio_data.get('result'))}")
            except Exception as e:
                logger.error(f"Error fetching portfolio data: {e}", exc_info=True)
                portfolio_data = None
        
        if request.include_returns:
            try:
                # Get returns data for user's holdings
                if portfolio_data and portfolio_data.get("type") == "success":
                    holdings = portfolio_data.get("result", [])
                    symbols = [h.get("TradingSymbol") for h in holdings if h.get("TradingSymbol")]
                    
                    if symbols:
                        # Get returns for each symbol
                        returns_list = []
                        for symbol in symbols[:10]:  # Limit to 10 symbols
                            result = returns_service.get_stock_returns(symbol)
                            if result.get("status") == "success":
                                returns_list.append(result.get("data"))
                        
                        if returns_list:
                            returns_data = {
                                "status": "success",
                                "data": returns_list
                            }
                else:
                    # Get top returns if no portfolio
                    returns_data = returns_service.get_all_returns(limit=20, sort_by="1_Year", sort_order="desc")
                
                logger.info(f"Fetched returns data for user {current_user.id}")
            except Exception as e:
                logger.error(f"Error fetching returns data: {e}")
                returns_data = None
        
        if request.include_bhavcopy:
            try:
                # Get bhavcopy data for user's holdings
                if portfolio_data and portfolio_data.get("type") == "success":
                    holdings = portfolio_data.get("result", [])
                    symbols = [h.get("TradingSymbol") for h in holdings if h.get("TradingSymbol")]
                    
                    if symbols:
                        # Get bhavcopy for each symbol
                        bhavcopy_list = []
                        for symbol in symbols[:10]:  # Limit to 10 symbols
                            result = bhavcopy_service.get_stock_bhavcopy_data(symbol)
                            if result.get("status") == "success":
                                data = result.get("data", [])
                                if isinstance(data, list):
                                    bhavcopy_list.extend(data)
                                else:
                                    bhavcopy_list.append(data)
                        
                        if bhavcopy_list:
                            bhavcopy_data = {
                                "status": "success",
                                "data": bhavcopy_list
                            }
                else:
                    # Get latest bhavcopy if no portfolio
                    # Filter to only equity stocks and limit to top stocks by volume
                    from app.services.s3_service import S3Service
                    s3_service = S3Service()
                    file_info = s3_service.get_latest_bhavcopy_file()
                    if file_info:
                        df = s3_service.get_bhavcopy_data(file_info['s3_key'])
                        if df is not None and not df.empty:
                            # Clean column names (strip whitespace)
                            df.columns = df.columns.str.strip()
                            
                            # Check if required columns exist
                            required_cols = ['SYMBOL', 'CLOSE_PRICE']
                            missing_cols = [col for col in required_cols if col not in df.columns]
                            if missing_cols:
                                logger.error(f"Missing required columns in bhavcopy: {missing_cols}")
                                logger.info(f"Available columns: {list(df.columns)}")
                                bhavcopy_data = None
                            else:
                                # Filter to only equity stocks (series EQ, BE, etc.) if SERIES column exists
                                if 'SERIES' in df.columns:
                                    equity_series = ['EQ', 'BE', 'BZ', 'B1', 'B2']
                                    df = df[df['SERIES'].str.strip().str.upper().isin(equity_series)]
                                
                                # Filter out G-Secs and other non-equity instruments
                                if 'SYMBOL' in df.columns:
                                    df = df[~df['SYMBOL'].str.contains('GS', case=False, na=False)]
                                
                                # Filter out rows with no price data
                                if 'CLOSE_PRICE' in df.columns:
                                    df = df[df['CLOSE_PRICE'].notna() & (df['CLOSE_PRICE'] != '-')]
                                
                                # Convert volume to numeric for sorting
                                try:
                                    if 'TTL_TRD_QNTY' in df.columns:
                                        df['TTL_TRD_QNTY_NUM'] = pd.to_numeric(df['TTL_TRD_QNTY'].replace('-', '0'), errors='coerce')
                                        # Sort by volume descending and get top 30
                                        df = df.sort_values('TTL_TRD_QNTY_NUM', ascending=False).head(30)
                                    else:
                                        # If no volume column, just get first 30
                                        df = df.head(30)
                                except Exception as e:
                                    logger.warning(f"Error sorting by volume: {e}")
                                    df = df.head(30)
                                
                                # Convert to records with proper field names
                                records = []
                                for _, row in df.iterrows():
                                    try:
                                        record = {
                                            "symbol": row['SYMBOL'].strip() if pd.notna(row.get('SYMBOL')) else None,
                                            "series": row['SERIES'].strip() if 'SERIES' in df.columns and pd.notna(row.get('SERIES')) else "EQ",
                                            "date": row['DATE1'].strip() if 'DATE1' in df.columns and pd.notna(row.get('DATE1')) else None,
                                            "prev_close": float(row['PREV_CLOSE']) if 'PREV_CLOSE' in df.columns and pd.notna(row.get('PREV_CLOSE')) and row.get('PREV_CLOSE') != '-' else None,
                                            "open_price": float(row['OPEN_PRICE']) if 'OPEN_PRICE' in df.columns and pd.notna(row.get('OPEN_PRICE')) and row.get('OPEN_PRICE') != '-' else None,
                                            "high_price": float(row['HIGH_PRICE']) if 'HIGH_PRICE' in df.columns and pd.notna(row.get('HIGH_PRICE')) and row.get('HIGH_PRICE') != '-' else None,
                                            "low_price": float(row['LOW_PRICE']) if 'LOW_PRICE' in df.columns and pd.notna(row.get('LOW_PRICE')) and row.get('LOW_PRICE') != '-' else None,
                                            "close_price": float(row['CLOSE_PRICE']) if pd.notna(row.get('CLOSE_PRICE')) and row.get('CLOSE_PRICE') != '-' else None,
                                            "total_traded_qty": int(row['TTL_TRD_QNTY']) if 'TTL_TRD_QNTY' in df.columns and pd.notna(row.get('TTL_TRD_QNTY')) and row.get('TTL_TRD_QNTY') != '-' else None,
                                            "turnover_lacs": float(row['TURNOVER_LACS']) if 'TURNOVER_LACS' in df.columns and pd.notna(row.get('TURNOVER_LACS')) and row.get('TURNOVER_LACS') != '-' else None,
                                        }
                                        records.append(record)
                                    except Exception as e:
                                        logger.warning(f"Error processing row: {e}")
                                        continue
                                
                                if records:
                                    bhavcopy_data = {
                                        "status": "success",
                                        "data": records
                                    }
                                    logger.info(f"Fetched {len(records)} equity stocks from bhavcopy for user {current_user.id}")
                                else:
                                    logger.warning("No equity stocks found in bhavcopy data after processing")
                                    bhavcopy_data = None
                        else:
                            logger.warning("Bhavcopy DataFrame is None or empty")
                            bhavcopy_data = None
                    else:
                        logger.warning("No bhavcopy file found in S3")
                        bhavcopy_data = None
                
                logger.info(f"Fetched bhavcopy data for user {current_user.id}")
            except Exception as e:
                logger.error(f"Error fetching bhavcopy data: {e}")
                bhavcopy_data = None
        
        # Get conversation history
        conversation_history = chat_repo.get_conversation_history(
            current_user.id,
            thread_id,
            limit=settings.chat_history_limit
        )
        formatted_history = chat_repo.format_history_for_llm(conversation_history)
        
        # Get LLM response
        llm_response = await llm_service.get_chat_response(
            user_query=request.query,
            portfolio_data=portfolio_data,
            returns_data=returns_data,
            bhavcopy_data=bhavcopy_data,
            conversation_history=formatted_history,
            system_instructions=request.system_instructions
        )
        
        if llm_response.get("status") != "success":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=llm_response.get("message", "Failed to generate response")
            )
        
        # Save to chat history
        try:
            chat_repo.save_message(
                user_id=current_user.id,
                thread_id=thread_id,
                user_query=request.query,
                assistant_response=llm_response.get("response", ""),
                metadata={
                    "include_portfolio": request.include_portfolio,
                    "include_returns": request.include_returns,
                    "include_bhavcopy": request.include_bhavcopy
                }
            )
        except Exception as e:
            logger.error(f"Error saving chat history: {e}")
            # Don't fail the request if history save fails
        
        return ChatResponse(
            status="success",
            message="Response generated successfully",
            response=llm_response.get("response"),
            thread_id=thread_id,
            metadata=llm_response.get("metadata")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process chat request: {str(e)}"
        )


@router.get("/chat/history/{thread_id}", response_model=ChatHistoryResponse)
async def get_chat_history(
    thread_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    chat_repo: ChatRepository = Depends(get_chat_repository)
):
    """
    Get conversation history for a thread
    
    Args:
        thread_id: Conversation thread ID
        current_user: Current authenticated user
        db: Database session
        chat_repo: Chat repository
        
    Returns:
        Conversation history
    """
    try:
        history = chat_repo.get_conversation_history(current_user.id, thread_id)
        
        return ChatHistoryResponse(
            status="success",
            thread_id=thread_id,
            messages=history,
            total_messages=len(history)
        )
        
    except Exception as e:
        logger.error(f"Error getting chat history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get chat history: {str(e)}"
        )


@router.get("/chat/threads", response_model=UserThreadsResponse)
async def get_user_threads(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    chat_repo: ChatRepository = Depends(get_chat_repository)
):
    """
    Get list of thread IDs for current user
    
    Args:
        current_user: Current authenticated user
        db: Database session
        chat_repo: Chat repository
        
    Returns:
        List of thread IDs
    """
    try:
        threads = chat_repo.get_user_threads(current_user.id)
        
        return UserThreadsResponse(
            status="success",
            threads=threads,
            total_threads=len(threads)
        )
        
    except Exception as e:
        logger.error(f"Error getting user threads: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user threads: {str(e)}"
        )


@router.delete("/chat/history/{thread_id}", response_model=ClearConversationResponse)
async def clear_conversation(
    thread_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    chat_repo: ChatRepository = Depends(get_chat_repository)
):
    """
    Clear conversation history for a thread
    
    Args:
        thread_id: Conversation thread ID
        current_user: Current authenticated user
        db: Database session
        chat_repo: Chat repository
        
    Returns:
        Success response
    """
    try:
        success = chat_repo.clear_conversation(current_user.id, thread_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to clear conversation"
            )
        
        return ClearConversationResponse(
            status="success",
            message="Conversation cleared successfully",
            thread_id=thread_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing conversation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear conversation: {str(e)}"
        )


@router.get("/chat/health")
async def llm_health_check():
    """
    Check LLM service health
    
    Returns:
        Health status
    """
    try:
        is_available = llm_service.is_available()
        
        return {
            "status": "success",
            "llm_available": is_available,
            "message": "LLM service is available" if is_available else "LLM service is not available"
        }
        
    except Exception as e:
        logger.error(f"Error checking LLM health: {e}")
        return {
            "status": "error",
            "llm_available": False,
            "message": f"Error checking LLM health: {str(e)}"
        }

