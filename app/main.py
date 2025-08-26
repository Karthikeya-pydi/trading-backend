# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import sys # Import sys for printing directly to stderr if needed

from app.core.config import settings
from app.core.database import engine, Base # Ensure Base is imported
from app.api.routes import auth, users, trading, market_data, websocket, iifl, portfolio, returns, stock_screening
# from app.core.logging import setup_logging  # ← DISABLED FOR VERCEL
from app.core.websocket_manager import manager
from app.services.realtime_service import realtime_service

# Setup logging
# setup_logging()  # ← DISABLED FOR VERCEL

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("--- APP STARTUP SEQUENCE INITIATED ---", file=sys.stderr)

    print(f"DEBUG: DATABASE_URL detected: {settings.database_url}", file=sys.stderr)
    print("DEBUG: Attempting to create database tables via Base.metadata.create_all...", file=sys.stderr)

    try:
        Base.metadata.create_all(bind=engine)
        print("DEBUG: Base.metadata.create_all completed successfully.", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to create database tables during startup: {e}", file=sys.stderr)
        # You might want to re-raise the exception or handle it more gracefully
        # raise # Uncomment this line if you want the app to crash on table creation failure

    # Start real-time services
    print("DEBUG: Starting real-time services...", file=sys.stderr)
    await realtime_service.start()
    print("DEBUG: Real-time services started.", file=sys.stderr)

    # Start WebSocket Redis listener
    print("DEBUG: Starting WebSocket Redis listener task...", file=sys.stderr)
    asyncio.create_task(manager.start_redis_listener())
    print("DEBUG: WebSocket Redis listener task initiated.", file=sys.stderr)

    print("--- APP STARTUP SEQUENCE COMPLETED ---", file=sys.stderr)
    yield

    # Shutdown
    print("--- APP SHUTDOWN SEQUENCE INITIATED ---", file=sys.stderr)
    await realtime_service.stop()
    print("--- APP SHUTDOWN SEQUENCE COMPLETED ---", file=sys.stderr)

app = FastAPI(
    title=settings.app_name,
    description="A lightweight trading platform with IIFL integration and real-time features",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])
app.include_router(trading.router, prefix="/api/trading", tags=["Trading"])
app.include_router(market_data.router, prefix="/api/market", tags=["Market Data"])
app.include_router(websocket.router, prefix="/api", tags=["WebSocket"])
app.include_router(iifl.router, prefix="/api/iifl", tags=["IIFL Integration"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["Portfolio"])
app.include_router(returns.router, prefix="/api/returns", tags=["Stock Returns"])
app.include_router(stock_screening.router, prefix="/api/screening", tags=["Stock Screening"])

@app.get("/")
async def root():
    return {"message": "Welcome to the Trading Platform API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "features": ["real-time", "websockets", "notifications"]}