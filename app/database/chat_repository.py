"""
Chat Repository

Database operations for chat history.
"""

from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime
from loguru import logger
import uuid
import json

from app.models.chat_history import ChatHistory
from app.core.config import settings


class ChatRepository:
    """Repository for chat history operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def save_message(
        self,
        user_id: int,
        thread_id: str,
        user_query: str,
        assistant_response: str,
        metadata: Optional[Dict] = None
    ) -> ChatHistory:
        """
        Save chat message to database
        
        Args:
            user_id: User ID
            thread_id: Conversation thread ID
            user_query: User's query
            assistant_response: Assistant's response
            metadata: Optional metadata dictionary
            
        Returns:
            Created ChatHistory object
        """
        try:
            chat_history = ChatHistory(
                user_id=user_id,
                thread_id=thread_id,
                user_query=user_query,
                assistant_response=assistant_response,
                chat_metadata=json.dumps(metadata) if metadata else None
            )
            
            self.db.add(chat_history)
            self.db.commit()
            self.db.refresh(chat_history)
            
            logger.info(f"Saved chat message for user {user_id}, thread {thread_id}")
            return chat_history
            
        except Exception as e:
            logger.error(f"Error saving chat message: {e}")
            self.db.rollback()
            raise
    
    def get_conversation_history(
        self,
        user_id: int,
        thread_id: str,
        limit: Optional[int] = None
    ) -> List[ChatHistory]:
        """
        Get conversation history for a thread
        
        Args:
            user_id: User ID
            thread_id: Conversation thread ID
            limit: Maximum number of messages to return
            
        Returns:
            List of ChatHistory objects
        """
        try:
            query = self.db.query(ChatHistory).filter(
                ChatHistory.user_id == user_id,
                ChatHistory.thread_id == thread_id
            ).order_by(ChatHistory.created_at.asc())
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
            
        except Exception as e:
            logger.error(f"Error getting conversation history: {e}")
            return []
    
    def get_recent_conversations(
        self,
        user_id: int,
        limit: int = 10
    ) -> List[ChatHistory]:
        """
        Get recent conversations for a user
        
        Args:
            user_id: User ID
            limit: Maximum number of messages to return
            
        Returns:
            List of ChatHistory objects
        """
        try:
            return self.db.query(ChatHistory).filter(
                ChatHistory.user_id == user_id
            ).order_by(desc(ChatHistory.created_at)).limit(limit).all()
            
        except Exception as e:
            logger.error(f"Error getting recent conversations: {e}")
            return []
    
    def get_user_threads(self, user_id: int) -> List[str]:
        """
        Get list of thread IDs for a user
        
        Args:
            user_id: User ID
            
        Returns:
            List of thread IDs
        """
        try:
            threads = self.db.query(ChatHistory.thread_id).filter(
                ChatHistory.user_id == user_id
            ).distinct().all()
            
            return [t[0] for t in threads]
            
        except Exception as e:
            logger.error(f"Error getting user threads: {e}")
            return []
    
    def clear_conversation(
        self,
        user_id: int,
        thread_id: str
    ) -> bool:
        """
        Clear conversation history for a thread
        
        Args:
            user_id: User ID
            thread_id: Conversation thread ID
            
        Returns:
            True if successful
        """
        try:
            deleted = self.db.query(ChatHistory).filter(
                ChatHistory.user_id == user_id,
                ChatHistory.thread_id == thread_id
            ).delete()
            
            self.db.commit()
            logger.info(f"Cleared {deleted} messages for user {user_id}, thread {thread_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing conversation: {e}")
            self.db.rollback()
            return False
    
    def generate_thread_id(self) -> str:
        """
        Generate a new thread ID
        
        Returns:
            New thread ID
        """
        return str(uuid.uuid4())
    
    def format_history_for_llm(
        self,
        history: List[ChatHistory],
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Format chat history for LLM context
        
        Args:
            history: List of ChatHistory objects
            limit: Maximum number of messages to include
            
        Returns:
            List of formatted message dictionaries
        """
        if limit:
            history = history[-limit:]
        
        formatted = []
        for msg in history:
            formatted.append({
                "user_query": msg.user_query,
                "assistant_response": msg.assistant_response,
                "created_at": msg.created_at.isoformat() if msg.created_at else None
            })
        
        return formatted

