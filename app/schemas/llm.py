"""
LLM Schemas

Pydantic models for LLM chat API requests and responses.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime


class ChatRequest(BaseModel):
    """Request schema for chat endpoint"""
    query: str = Field(..., description="User's question or query")
    thread_id: Optional[str] = Field(None, description="Conversation thread ID (auto-generated if not provided)")
    include_portfolio: bool = Field(False, description="Include portfolio data in context")
    include_returns: bool = Field(False, description="Include returns data in context")
    include_bhavcopy: bool = Field(False, description="Include bhavcopy data in context")
    system_instructions: Optional[str] = Field(None, description="Custom system instructions")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "What is my portfolio performance?",
                "thread_id": "123e4567-e89b-12d3-a456-426614174000",
                "include_portfolio": True,
                "include_returns": True,
                "include_bhavcopy": False
            }
        }


class ChatMessage(BaseModel):
    """Schema for individual chat message"""
    id: int
    user_query: str
    assistant_response: str
    created_at: datetime
    chat_metadata: Optional[str] = None  # Renamed from metadata to match model
    
    class Config:
        from_attributes = True


class ChatResponse(BaseModel):
    """Response schema for chat endpoint"""
    status: str
    message: str
    response: Optional[str] = None
    thread_id: str
    metadata: Optional[Dict] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Response generated successfully",
                "response": "Your portfolio shows a positive P&L of 5.2%...",
                "thread_id": "123e4567-e89b-12d3-a456-426614174000",
                "metadata": {
                    "model": "gpt-4o",
                    "temperature": 0.3
                }
            }
        }


class ChatHistoryResponse(BaseModel):
    """Response schema for chat history endpoint"""
    status: str
    thread_id: str
    messages: List[ChatMessage]
    total_messages: int
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "thread_id": "123e4567-e89b-12d3-a456-426614174000",
                "messages": [],
                "total_messages": 0
            }
        }


class UserThreadsResponse(BaseModel):
    """Response schema for user threads endpoint"""
    status: str
    threads: List[str]
    total_threads: int
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "threads": ["thread-1", "thread-2"],
                "total_threads": 2
            }
        }


class ClearConversationResponse(BaseModel):
    """Response schema for clear conversation endpoint"""
    status: str
    message: str
    thread_id: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Conversation cleared successfully",
                "thread_id": "123e4567-e89b-12d3-a456-426614174000"
            }
        }

