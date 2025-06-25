from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, date

class PortfolioPosition(BaseModel):
    underlying: str
    option_type: Optional[str] = None
    strike_price: Optional[float] = None
    quantity: int
    average_price: float
    current_price: Optional[float] = None
    unrealized_pnl: float
    position_type: str  # "LONG" or "SHORT"

class PortfolioSummary(BaseModel):
    total_positions: int
    long_positions: int
    short_positions: int
    total_investment: float
    current_value: float
    unrealized_pnl: float
    daily_pnl: float
    monthly_pnl: float
    positions: List[PortfolioPosition]

class PnLData(BaseModel):
    total_realized_pnl: float
    total_unrealized_pnl: float
    total_pnl: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_charges: float

class DailyPnL(BaseModel):
    date: date
    daily_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    trades_count: int
    win_rate: float

class RiskMetrics(BaseModel):
    net_exposure: float
    gross_exposure: float
    long_exposure: float
    short_exposure: float
    concentration_risk_percent: float
    positions_at_risk: int
    total_unrealized_loss: float
    portfolio_diversity: int 