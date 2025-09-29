from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, DisconnectionError
import time
from app.core.config import settings

# For SQLite, we need to enable foreign key constraints
if settings.database_url.startswith("sqlite"):
    engine = create_engine(
        settings.database_url, 
        connect_args={"check_same_thread": False},
        echo=settings.debug
    )
else:
    # Enhanced PostgreSQL configuration with SSL and connection pooling
    engine = create_engine(
        settings.database_url, 
        echo=settings.debug,
        poolclass=QueuePool,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,  # Verify connections before use
        pool_recycle=settings.database_pool_recycle,   # Recycle connections every hour
        connect_args={
            "sslmode": settings.database_sslmode,  # Use SSL if available, fallback to non-SSL
            "connect_timeout": settings.database_connect_timeout,
            "application_name": "trading_platform"
        }
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    except (OperationalError, DisconnectionError) as e:
        # Handle SSL connection errors
        print(f"Database connection error: {e}")
        db.rollback()
        db.close()
        # Retry once with a new connection
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    finally:
        db.close()
