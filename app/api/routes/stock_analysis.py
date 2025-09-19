"""
Stock Analysis API Routes

Provides endpoints for comprehensive stock data analysis including
descriptive statistics, outlier detection, and anomaly flags.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
import sys
import os
import pandas as pd
import numpy as np

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(project_root)

# Import the stock analysis service from the stock-analysis directory
import importlib.util
stock_analysis_path = os.path.join(project_root, "stock-analysis", "stock_analysis_service.py")
spec = importlib.util.spec_from_file_location("stock_analysis_service", stock_analysis_path)
stock_analysis_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stock_analysis_module)
StockAnalysisService = stock_analysis_module.StockAnalysisService

# Import S3 service for fetching H5 data
from app.services.s3_service import S3Service
from app.services.s3_stock_analysis_service import S3StockAnalysisService
from app.schemas.stock_analysis import (
    StockAnalysisResponse,
    SingleStockAnalysisResponse,
    StockAnalysisSummary,
    StockAnalysisDetailed,
    AnalysisSummary,
    DescriptiveStats,
    GlobalAnalysis,
    RollingAnalysis,
    PerStockAnalysis
)
from app.api.dependencies import get_current_user
from app.models.user import User

router = APIRouter()

# Initialize the S3-enabled stock analysis service
stock_analysis_service = S3StockAnalysisService()


def convert_analysis_to_summary(analysis_results: dict) -> List[StockAnalysisSummary]:
    """Convert analysis results to summary format for table display"""
    summary_data = []
    
    for symbol, analysis in analysis_results['results'].items():
        if 'error' in analysis:
            continue
            
        descriptive_stats = analysis.get('descriptive_stats', {})
        global_analysis = analysis.get('global_analysis', {})
        rolling_analysis = analysis.get('rolling_analysis', {})
        per_stock_analysis = analysis.get('per_stock_analysis', {})
        
        # Count flags
        enhanced_data = analysis.get('enhanced_data')
        global_outlier_count = 0
        mild_anomaly_count = 0
        major_anomaly_count = 0
        robust_outlier_count = 0
        very_extreme_count = 0
        
        if enhanced_data is not None:
            global_outlier_count = enhanced_data.get('global_outlier_flag', pd.Series()).sum()
            mild_anomaly_count = enhanced_data.get('mild_anomaly_flag', pd.Series()).sum()
            major_anomaly_count = enhanced_data.get('major_anomaly_flag', pd.Series()).sum()
            robust_outlier_count = enhanced_data.get('robust_outlier_flag', pd.Series()).sum()
            very_extreme_count = enhanced_data.get('very_extreme_flag', pd.Series()).sum()
        
        summary = StockAnalysisSummary(
            symbol=symbol,
            data_points=analysis.get('data_points', 0),
            analysis_date=analysis.get('analysis_date', ''),
            n_days=descriptive_stats.get('n_days', 0),
            pct_missing=descriptive_stats.get('pct_missing', 0.0),
            start_date=descriptive_stats.get('start_date'),
            end_date=descriptive_stats.get('end_date'),
            mean_return=descriptive_stats.get('mean_return', 0.0),
            std_return=descriptive_stats.get('std_return', 0.0),
            min_return=descriptive_stats.get('min_return', 0.0),
            max_return=descriptive_stats.get('max_return', 0.0),
            illiquid_flag=descriptive_stats.get('illiquid_flag', True),
            global_outlier_count=int(global_outlier_count),
            mild_anomaly_count=int(mild_anomaly_count),
            major_anomaly_count=int(major_anomaly_count),
            robust_outlier_count=int(robust_outlier_count),
            very_extreme_count=int(very_extreme_count)
        )
        summary_data.append(summary)
    
    return summary_data


def convert_analysis_to_detailed(analysis_results: dict) -> List[StockAnalysisDetailed]:
    """Convert analysis results to detailed format for table display"""
    detailed_data = []
    
    for symbol, analysis in analysis_results['results'].items():
        if 'error' in analysis or 'enhanced_data' not in analysis:
            continue
            
        enhanced_data = analysis['enhanced_data']
        
        for _, row in enhanced_data.iterrows():
            detailed = StockAnalysisDetailed(
                symbol=symbol,
                date=row['Date'],
                open=row.get('Open', 0.0),
                high=row.get('High', 0.0),
                low=row.get('Low', 0.0),
                close=row.get('Close', 0.0),
                volume=row.get('Volume', 0.0),
                log_returns=row.get('log_returns'),
                global_outlier_flag=bool(row.get('global_outlier_flag', False)),
                mild_anomaly_flag=bool(row.get('mild_anomaly_flag', False)),
                major_anomaly_flag=bool(row.get('major_anomaly_flag', False)),
                robust_outlier_flag=bool(row.get('robust_outlier_flag', False)),
                very_extreme_flag=bool(row.get('very_extreme_flag', False)),
                window_ready_10=bool(row.get('window_ready_10', False)),
                window_ready_40=bool(row.get('window_ready_40', False)),
                window_ready_120=bool(row.get('window_ready_120', False))
            )
            detailed_data.append(detailed)
    
    return detailed_data


@router.get("/search")
async def search_stock_analysis(
    symbol: str = Query(..., description="Stock symbol to search and analyze"),
    include_h5_status: bool = False,
    force_refresh: bool = False,
    current_user: User = Depends(get_current_user)
):
    """
    Search and analyze a particular stock by symbol.
    
    Args:
        symbol: Stock symbol to search and analyze
        include_h5_status: Include H5 file loading status and performance info
        force_refresh: Force download fresh H5 data even if cache is valid
    
    Returns comprehensive analysis data for the specified stock symbol including
    both summary statistics and detailed daily data with anomaly flags.
    """
    try:
        # Perform analysis for single stock with optional force refresh
        analysis_result = stock_analysis_service.analyze_single_stock(symbol.upper(), force_refresh=force_refresh)
        
        if 'error' in analysis_result:
            raise HTTPException(status_code=404, detail=analysis_result['error'])
        
        # Helper function to handle NaN/inf values
        def safe_float(value, default=0.0):
            if pd.isna(value) or np.isinf(value) or np.isnan(value):
                return default
            return float(value)
        
        # Convert descriptive stats with safe float handling
        desc_stats = analysis_result['descriptive_stats']
        descriptive_stats = DescriptiveStats(
            n_days=int(safe_float(desc_stats.get('n_days', 0))),
            pct_missing=safe_float(desc_stats.get('pct_missing', 0.0)),
            start_date=desc_stats.get('start_date'),
            end_date=desc_stats.get('end_date'),
            mean_return=safe_float(desc_stats.get('mean_return', 0.0)),
            std_return=safe_float(desc_stats.get('std_return', 0.0)),
            skew_return=safe_float(desc_stats.get('skew_return', 0.0)),
            kurtosis_return=safe_float(desc_stats.get('kurtosis_return', 0.0)),
            min_return=safe_float(desc_stats.get('min_return', 0.0)),
            p1_return=safe_float(desc_stats.get('p1_return', 0.0)),
            p5_return=safe_float(desc_stats.get('p5_return', 0.0)),
            p95_return=safe_float(desc_stats.get('p95_return', 0.0)),
            p99_return=safe_float(desc_stats.get('p99_return', 0.0)),
            max_return=safe_float(desc_stats.get('max_return', 0.0)),
            illiquid_flag=bool(desc_stats.get('illiquid_flag', True))
        )
        
        # Convert global analysis with safe float handling
        global_data = analysis_result['global_analysis']
        global_analysis = GlobalAnalysis(
            global_median=safe_float(global_data.get('global_median', 0.0)),
            global_mad=safe_float(global_data.get('global_mad', 0.0)),
            global_outlier_count=int(safe_float(global_data.get('global_outlier_flag', pd.Series()).sum()))
        )
        
        # Convert rolling analysis with safe float handling
        rolling_data = analysis_result['rolling_analysis']
        rolling_analysis = RollingAnalysis(
            window_ready_10=int(safe_float(rolling_data.get('window_ready_10', pd.Series()).sum())),
            window_ready_40=int(safe_float(rolling_data.get('window_ready_40', pd.Series()).sum())),
            window_ready_120=int(safe_float(rolling_data.get('window_ready_120', pd.Series()).sum())),
            mild_anomaly_count=int(safe_float(rolling_data.get('mild_anomaly_flag', pd.Series()).sum())),
            major_anomaly_count=int(safe_float(rolling_data.get('major_anomaly_flag', pd.Series()).sum()))
        )
        
        # Convert per-stock analysis with safe float handling
        per_stock_data = analysis_result['per_stock_analysis']
        per_stock_analysis = PerStockAnalysis(
            per_stock_median=safe_float(per_stock_data.get('per_stock_median', 0.0)),
            per_stock_mad=safe_float(per_stock_data.get('per_stock_mad', 0.0)),
            robust_outlier_count=int(safe_float(per_stock_data.get('robust_outlier_flag', pd.Series()).sum())),
            very_extreme_count=int(safe_float(per_stock_data.get('very_extreme_flag', pd.Series()).sum()))
        )
        
        # Convert detailed data with safe float handling
        enhanced_data = analysis_result.get('enhanced_data')
        detailed_data = []
        if enhanced_data is not None:
            for _, row in enhanced_data.iterrows():
                # Handle log_returns specially - it can be NaN for first day
                log_returns = row.get('log_returns')
                if pd.isna(log_returns) or np.isinf(log_returns) or np.isnan(log_returns):
                    log_returns = None
                else:
                    log_returns = float(log_returns)
                
                detailed = StockAnalysisDetailed(
                    symbol=symbol.upper(),
                    date=row['Date'],
                    open=safe_float(row.get('Open', 0.0)),
                    high=safe_float(row.get('High', 0.0)),
                    low=safe_float(row.get('Low', 0.0)),
                    close=safe_float(row.get('Close', 0.0)),
                    volume=safe_float(row.get('Volume', 0.0)),
                    log_returns=log_returns,
                    global_outlier_flag=bool(row.get('global_outlier_flag', False)),
                    mild_anomaly_flag=bool(row.get('mild_anomaly_flag', False)),
                    major_anomaly_flag=bool(row.get('major_anomaly_flag', False)),
                    robust_outlier_flag=bool(row.get('robust_outlier_flag', False)),
                    very_extreme_flag=bool(row.get('very_extreme_flag', False)),
                    window_ready_10=bool(row.get('window_ready_10', False)),
                    window_ready_40=bool(row.get('window_ready_40', False)),
                    window_ready_120=bool(row.get('window_ready_120', False))
                )
                detailed_data.append(detailed)
        
        response_data = SingleStockAnalysisResponse(
            symbol=symbol.upper(),
            data_points=analysis_result['data_points'],
            analysis_date=analysis_result['analysis_date'],
            descriptive_stats=descriptive_stats,
            global_analysis=global_analysis,
            rolling_analysis=rolling_analysis,
            per_stock_analysis=per_stock_analysis,
            detailed_data=detailed_data
        )
        
        # Add H5 file status if requested
        if include_h5_status:
            try:
                h5_info = stock_analysis_service.get_data_info()
                # Convert to dict and add H5 status
                response_dict = response_data.dict()
                response_dict["h5_file_status"] = h5_info
                return response_dict
            except Exception as e:
                # If H5 status fails, return original response with error
                response_dict = response_data.dict()
                response_dict["h5_file_status"] = {"error": f"Could not get H5 status: {str(e)}"}
                return response_dict
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing stock {symbol}: {str(e)}")


@router.get("/stocks")
async def get_available_stocks(
    include_h5_info: bool = False,
    include_download_url: bool = False,
    force_refresh: bool = False,
    current_user: User = Depends(get_current_user)
):
    """
    Get list of available stock symbols for analysis.
    
    Args:
        include_h5_info: Include H5 file information (size, load time, etc.)
        include_download_url: Include direct S3 download URL for H5 file
        force_refresh: Force download fresh H5 data even if cache is valid
    """
    try:
        # Force refresh data if requested
        if force_refresh:
            stock_analysis_service.clear_data_cache()
        
        stocks = stock_analysis_service.get_unique_stocks()
        
        response = {
            "stocks": stocks, 
            "count": len(stocks)
        }
        
        # Add H5 file information if requested
        if include_h5_info:
            try:
                h5_info = stock_analysis_service.get_data_info()
                response["h5_file_info"] = h5_info
            except Exception as e:
                response["h5_file_info"] = {"error": f"Could not get H5 info: {str(e)}"}
        
        # Add direct download URL if requested
        if include_download_url:
            try:
                from botocore.exceptions import ClientError
                
                download_url = stock_analysis_service.s3_service.s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': 'parquet-eq-data',
                        'Key': 'nse_data/Our_Nseadjprice.h5'
                    },
                    ExpiresIn=3600  # 1 hour
                )
                
                response["h5_download"] = {
                    "download_url": download_url,
                    "filename": "Our_Nseadjprice.h5",
                    "expires_in": 3600,
                    "message": "Use this URL to download the H5 file directly from S3"
                }
            except Exception as e:
                response["h5_download"] = {"error": f"Could not generate download URL: {str(e)}"}
        
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting available stocks: {str(e)}")

@router.post("/clear-cache")
async def clear_data_cache(
    include_h5_info: bool = True,
    current_user: User = Depends(get_current_user)
):
    """
    Clear the cached data to free memory.
    
    Args:
        include_h5_info: Include H5 file information after clearing cache
    """
    try:
        stock_analysis_service.clear_data_cache()
        
        response = {"message": "Data cache cleared successfully"}
        
        # Add H5 file information if requested
        if include_h5_info:
            try:
                h5_info = stock_analysis_service.get_data_info()
                response["h5_file_info"] = h5_info
            except Exception as e:
                response["h5_file_info"] = {"error": f"Could not get H5 info: {str(e)}"}
        
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")
