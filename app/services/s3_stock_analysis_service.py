"""
S3-enabled Stock Analysis Service

This service extends the original stock analysis service to fetch H5 data from S3
instead of using local files, similar to how returnsCalProd works.
"""

import pandas as pd
import numpy as np
import h5py
import tempfile
import os
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Any
from loguru import logger
from pathlib import Path
import warnings
import boto3
import io
warnings.filterwarnings('ignore')

# Import the original stock analysis service
import sys
import importlib.util
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
stock_analysis_path = os.path.join(project_root, "stock-analysis", "stock_analysis_service.py")
spec = importlib.util.spec_from_file_location("stock_analysis_service", stock_analysis_path)
stock_analysis_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stock_analysis_module)

from app.services.s3_service import S3Service


class S3StockAnalysisService:
    """
    S3-enabled Stock Analysis Service that fetches H5 data from S3.
    
    This service extends the original StockAnalysisService to work with S3 data
    instead of local files, following the same pattern as returnsCalProd.
    """
    
    def __init__(self, input_bucket: str = "parquet-eq-data", h5_key: str = "nse_data/Our_Nseadjprice.h5"):
        """
        Initialize the S3 Stock Analysis Service.
        
        Args:
            input_bucket: S3 bucket containing the H5 file
            h5_key: S3 key for the H5 file
        """
        self.input_bucket = input_bucket
        self.h5_key = h5_key
        self.data = None
        self.analysis_results = {}
        self._data_loaded = False  # Add caching flag
        
        # Initialize S3 service
        self.s3_service = S3Service()
        
        # Initialize the original stock analysis service for methods
        self.original_service = stock_analysis_module.StockAnalysisService()
        
    def load_data_from_s3(self) -> pd.DataFrame:
        """
        Load and preprocess the H5 data file from S3.
        
        Returns:
            DataFrame with stock data
        """
        # Return cached data if already loaded
        if self._data_loaded and self.data is not None:
            logger.info("Using cached data from S3")
            return self.data
            
        try:
            logger.info(f"Loading H5 data from S3: s3://{self.input_bucket}/{self.h5_key}")
            
            # Download H5 data from S3
            response = self.s3_service.s3_client.get_object(
                Bucket=self.input_bucket, 
                Key=self.h5_key
            )
            h5_data = response['Body'].read()
            logger.info(f"Downloaded H5 file from S3 ({len(h5_data)} bytes)")
            
            # Convert H5 to DataFrame using the same method as returnsCalProd
            self.data = self._convert_h5_to_dataframe(h5_data)
            
            # Convert Date column to datetime
            self.data['Date'] = pd.to_datetime(self.data['Date'])
            
            # Convert numeric columns
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            
            # Sort by Symbol and Date
            self.data = self.data.sort_values(['Symbol', 'Date']).reset_index(drop=True)
            
            # Mark as loaded
            self._data_loaded = True
            
            logger.info(f"Loaded {len(self.data)} records for {self.data['Symbol'].nunique()} stocks from S3")
            return self.data
            
        except Exception as e:
            logger.error(f"Error loading data from S3: {e}")
            raise
    
    def clear_data_cache(self):
        """Clear the cached data to free memory"""
        self.data = None
        self._data_loaded = False
        logger.info("Cleared data cache")
    
    def _convert_h5_to_dataframe(self, h5_data: bytes) -> pd.DataFrame:
        """
        Convert H5 data to DataFrame using the same method as returnsCalProd.
        
        Args:
            h5_data: Raw H5 data bytes from S3
            
        Returns:
            DataFrame containing the stock data
        """
        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as temp_file:
            temp_file.write(h5_data)
            temp_file_path = temp_file.name
        
        try:
            # Try pandas read first (most reliable)
            try:
                return pd.read_hdf(temp_file_path)
            except:
                pass
            
            # Manual reconstruction for complex HDF5 structures
            with h5py.File(temp_file_path, 'r') as f:
                if 'stage' in f:
                    stage = f['stage']
                    
                    # Get column names from block items
                    columns = []
                    for key in stage.keys():
                        if 'items' in key:
                            items = stage[key][:]
                            if items.dtype.kind == 'S':
                                items = [item.decode('utf-8') for item in items]
                            columns.extend(items)
                    
                    # Get data values
                    data_blocks = []
                    for key in stage.keys():
                        if 'values' in key:
                            values = stage[key][:]
                            if values.ndim == 2:
                                data_blocks.append(values)
                            elif values.ndim == 1:
                                data_blocks.append(values.reshape(-1, 1))
                    
                    if data_blocks:
                        combined_data = np.hstack(data_blocks)
                        return pd.DataFrame(combined_data, columns=columns)
                    else:
                        raise ValueError("No data blocks found")
                else:
                    raise ValueError("No 'stage' group found in HDF5 file")
        
        finally:
            os.unlink(temp_file_path)
    
    def get_unique_stocks(self) -> List[str]:
        """
        Get list of unique stock symbols.
        
        Returns:
            List of unique stock symbols
        """
        if self.data is None:
            self.load_data_from_s3()
        return sorted(self.data['Symbol'].unique().tolist())
    
    def filter_data_for_stock(self, symbol: str, start_date: str = "2003-01-01") -> pd.DataFrame:
        """
        Filter data for a specific stock from 2003 onwards.
        
        Args:
            symbol: Stock symbol to filter
            start_date: Start date for filtering (default: 2003-01-01)
            
        Returns:
            Filtered DataFrame for the specific stock
        """
        if self.data is None:
            self.load_data_from_s3()
        
        # Filter for specific stock and date range
        stock_data = self.data[
            (self.data['Symbol'] == symbol) & 
            (self.data['Date'] >= start_date)
        ].copy()
        
        # Sort by date
        stock_data = stock_data.sort_values('Date').reset_index(drop=True)
        
        return stock_data
    
    def analyze_single_stock(self, symbol: str) -> Dict[str, Any]:
        """
        Perform complete analysis for a single stock using S3 data.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Dictionary containing all analysis results for the stock
        """
        logger.info(f"Analyzing stock from S3: {symbol}")
        
        try:
            # Load data from S3 if not already loaded
            if self.data is None:
                self.load_data_from_s3()
            
            # Use the original service methods but with S3 data
            # Step 1: Data preparation and filtering
            stock_data = self.filter_data_for_stock(symbol)
            
            if len(stock_data) == 0:
                logger.warning(f"No data found for stock: {symbol}")
                return {'error': f'No data found for stock: {symbol}'}
            
            # Calculate log returns
            returns = self.original_service.calculate_log_returns(stock_data['Close'])
            
            # Step 2: Global MAD analysis
            global_analysis = self.original_service.calculate_global_mad_analysis(returns)
            
            # Step 3: Rolling window analysis
            rolling_analysis = self.original_service.calculate_rolling_window_analysis(returns)
            
            # Step 4: Per-stock outlier flags
            per_stock_analysis = self.original_service.calculate_per_stock_outlier_flags(returns)
            
            # Step 5: Descriptive snapshot
            descriptive_stats = self.original_service.generate_descriptive_snapshot(stock_data, returns)
            
            # Combine all results
            analysis_results = {
                'symbol': symbol,
                'data_points': len(stock_data),
                'analysis_date': datetime.now().isoformat(),
                'global_analysis': global_analysis,
                'rolling_analysis': rolling_analysis,
                'per_stock_analysis': per_stock_analysis,
                'descriptive_stats': descriptive_stats
            }
            
            # Add flags to the main DataFrame for easy access
            stock_data['log_returns'] = returns
            stock_data['global_outlier_flag'] = global_analysis['global_outlier_flag']
            stock_data['mild_anomaly_flag'] = rolling_analysis.get('mild_anomaly_flag', False)
            stock_data['major_anomaly_flag'] = rolling_analysis.get('major_anomaly_flag', False)
            stock_data['robust_outlier_flag'] = per_stock_analysis['robust_outlier_flag']
            stock_data['very_extreme_flag'] = per_stock_analysis['very_extreme_flag']
            
            # Add window readiness flags
            for window_size in [10, 40, 120]:
                if f'window_ready_{window_size}' in rolling_analysis:
                    stock_data[f'window_ready_{window_size}'] = rolling_analysis[f'window_ready_{window_size}']
            
            analysis_results['enhanced_data'] = stock_data
            
            logger.info(f"Completed S3 analysis for stock: {symbol}")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error analyzing stock {symbol} from S3: {e}")
            return {'error': f'Error analyzing stock {symbol}: {str(e)}'}
    
    def analyze_all_stocks(self, max_stocks: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform analysis for all stocks in the S3 dataset.
        
        Args:
            max_stocks: Maximum number of stocks to analyze (None for all)
            
        Returns:
            Dictionary containing analysis results for all stocks
        """
        logger.info("Starting comprehensive stock analysis pipeline from S3")
        
        # Load data from S3 if not already loaded
        if self.data is None:
            self.load_data_from_s3()
        
        # Get unique stocks
        unique_stocks = self.get_unique_stocks()
        
        if max_stocks:
            unique_stocks = unique_stocks[:max_stocks]
        
        logger.info(f"Analyzing {len(unique_stocks)} stocks from S3")
        
        # Analyze each stock
        all_results = {}
        successful_analyses = 0
        failed_analyses = 0
        
        for i, symbol in enumerate(unique_stocks):
            logger.info(f"Processing stock {i+1}/{len(unique_stocks)}: {symbol}")
            
            try:
                result = self.analyze_single_stock(symbol)
                all_results[symbol] = result
                
                if 'error' not in result:
                    successful_analyses += 1
                else:
                    failed_analyses += 1
                    
            except Exception as e:
                logger.error(f"Failed to analyze stock {symbol}: {e}")
                all_results[symbol] = {'error': str(e)}
                failed_analyses += 1
        
        # Generate summary
        summary = {
            'total_stocks': len(unique_stocks),
            'successful_analyses': successful_analyses,
            'failed_analyses': failed_analyses,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"S3 Analysis complete. Success: {successful_analyses}, Failed: {failed_analyses}")
        
        return {
            'summary': summary,
            'results': all_results
        }
