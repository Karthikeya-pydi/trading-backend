# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import sys # Import sys for printing directly to stderr if needed

from app.core.config import settings
from app.core.database import engine, Base # Ensure Base is imported
from app.core.middleware import TokenRefreshMiddleware
from app.api.routes import auth, users, trading, market_data, websocket, iifl, portfolio, returns
from app.services.scheduler_service import scheduler_service
# from app.core.logging import setup_logging  # ← DISABLED FOR VERCEL
# from app.core.websocket_manager import manager  # DISABLED - No Redis
# from app.services.realtime_service import realtime_service  # DISABLED - No Redis

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

    # Start real-time services (DISABLED - No Redis)
    print("DEBUG: Real-time services DISABLED - No Redis available", file=sys.stderr)
    # await realtime_service.start()  # DISABLED
    # print("DEBUG: Real-time services started.", file=sys.stderr)

    # Start WebSocket Redis listener (DISABLED - No Redis)
    print("DEBUG: WebSocket Redis listener DISABLED - No Redis available", file=sys.stderr)
    # asyncio.create_task(manager.start_redis_listener())  # DISABLED
    # print("DEBUG: WebSocket Redis listener task initiated.", file=sys.stderr)

    # Start scheduler service for automated tasks
    print("DEBUG: Starting scheduler service...", file=sys.stderr)
    try:
        await scheduler_service.start()
        print("DEBUG: Scheduler service started successfully.", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to start scheduler service: {e}", file=sys.stderr)

    print("--- APP STARTUP SEQUENCE COMPLETED ---", file=sys.stderr)
    yield

    # Shutdown
    print("--- APP SHUTDOWN SEQUENCE INITIATED ---", file=sys.stderr)
    # await realtime_service.stop()  # DISABLED - No Redis
    
    # Stop scheduler service
    print("DEBUG: Stopping scheduler service...", file=sys.stderr)
    try:
        await scheduler_service.stop()
        print("DEBUG: Scheduler service stopped successfully.", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to stop scheduler service: {e}", file=sys.stderr)
    
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
    allow_origins=[
        "http://127.0.0.1:3000",  
        "http://localhost:3000",  
        "http://127.0.0.1:8000",  
        "http://localhost:8000",   
        "https://trading-frontend-3enh.vercel.app",  # Production frontend
        "https://trading-backend-oab.info"  # Production backend
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Token refresh middleware (should be after CORS)
app.add_middleware(TokenRefreshMiddleware)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])
app.include_router(trading.router, prefix="/api/trading", tags=["Trading"])
app.include_router(market_data.router, prefix="/api/market", tags=["Market Data"])
app.include_router(websocket.router, prefix="/api", tags=["WebSocket"])
app.include_router(iifl.router, prefix="/api/iifl", tags=["IIFL Integration"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["Portfolio"])
app.include_router(returns.router, prefix="/api/returns", tags=["Stock Returns"])

@app.get("/")
async def root():
    return {"message": "Welcome to the Trading Platform API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "features": ["nifty-indices", "market-data", "trading", "portfolio"], "redis": "disabled"}

@app.post("/api/scheduler/trigger-returns")
async def trigger_returns_calculation(target_date: str = None):
    """Manually trigger returns calculation for a specific date"""
    try:
        await scheduler_service.run_returns_calculation_manual(target_date)
        return {"status": "success", "message": f"Returns calculation triggered for {target_date or 'current date'}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get the status of scheduled jobs"""
    try:
        jobs = scheduler_service.get_scheduled_jobs()
        return {
            "status": "success",
            "scheduler_running": scheduler_service.is_running,
            "jobs": jobs
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}