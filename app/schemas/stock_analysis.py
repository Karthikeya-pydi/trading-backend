"""
Pydantic schemas for Stock Analysis API responses
"""

from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime


class DescriptiveStats(BaseModel):
    """Descriptive statistics for a stock"""
    n_days: int
    pct_missing: float
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    mean_return: float
    std_return: float
    skew_return: float
    kurtosis_return: float
    min_return: float
    p1_return: float
    p5_return: float
    p95_return: float
    p99_return: float
    max_return: float
    illiquid_flag: bool


class GlobalAnalysis(BaseModel):
    """Global MAD analysis results"""
    global_median: float
    global_mad: float
    global_outlier_count: int


class RollingAnalysis(BaseModel):
    """Rolling window analysis results"""
    window_ready_10: int
    window_ready_40: int
    window_ready_120: int
    mild_anomaly_count: int
    major_anomaly_count: int


class PerStockAnalysis(BaseModel):
    """Per-stock outlier analysis results"""
    per_stock_median: float
    per_stock_mad: float
    robust_outlier_count: int
    very_extreme_count: int


class StockAnalysisSummary(BaseModel):
    """Summary of stock analysis for table display"""
    symbol: str
    data_points: int
    analysis_date: str
    n_days: int
    pct_missing: float
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    mean_return: float
    std_return: float
    min_return: float
    max_return: float
    illiquid_flag: bool
    global_outlier_count: int
    mild_anomaly_count: int
    major_anomaly_count: int
    robust_outlier_count: int
    very_extreme_count: int


class StockAnalysisDetailed(BaseModel):
    """Detailed stock analysis data for table display"""
    symbol: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    log_returns: Optional[float]
    global_outlier_flag: bool
    mild_anomaly_flag: bool
    major_anomaly_flag: bool
    robust_outlier_flag: bool
    very_extreme_flag: bool
    window_ready_10: bool
    window_ready_40: bool
    window_ready_120: bool


class AnalysisSummary(BaseModel):
    """Overall analysis summary"""
    total_stocks: int
    successful_analyses: int
    failed_analyses: int
    analysis_timestamp: str


class StockAnalysisResponse(BaseModel):
    """Complete stock analysis response"""
    summary: AnalysisSummary
    summary_data: List[StockAnalysisSummary]
    detailed_data: List[StockAnalysisDetailed]


class SingleStockAnalysisResponse(BaseModel):
    """Response for single stock analysis"""
    symbol: str
    data_points: int
    analysis_date: str
    descriptive_stats: DescriptiveStats
    global_analysis: GlobalAnalysis
    rolling_analysis: RollingAnalysis
    per_stock_analysis: PerStockAnalysis
    detailed_data: List[StockAnalysisDetailed]
