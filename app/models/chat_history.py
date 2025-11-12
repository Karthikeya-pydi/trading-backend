from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base

class ChatHistory(Base):
    __tablename__ = "chat_history"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    thread_id = Column(String, nullable=False, index=True)  # Conversation thread ID
    user_query = Column(Text, nullable=False)
    assistant_response = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
    # Optional: Store metadata about the conversation
    # Note: Using 'chat_metadata' instead of 'metadata' because 'metadata' is reserved in SQLAlchemy
    chat_metadata = Column(Text, nullable=True)  # JSON string for additional metadata
    
    # Relationship to User
    user = relationship("User", backref="chat_history")
    
    def __repr__(self):
        return f"<ChatHistory(id={self.id}, user_id={self.user_id}, thread_id={self.thread_id})>"

