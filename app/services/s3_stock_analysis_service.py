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
        self._last_load_time = None  # Track when data was last loaded
        self._load_timeout = 300  # 5 minutes timeout for loading
        
        # Initialize S3 service
        self.s3_service = S3Service()
        
        # Initialize the original stock analysis service for methods
        self.original_service = stock_analysis_module.StockAnalysisService()
        
    def load_data_from_s3(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Load and preprocess the H5 data file from S3 with optimized memory usage.
        
        Args:
            force_refresh: Force download fresh data even if cache is valid
        
        Returns:
            DataFrame with stock data
        """
        # Check if we need to refresh due to file updates
        if self._data_loaded and self.data is not None and not force_refresh:
            if self._last_load_time and (datetime.now() - self._last_load_time).seconds < self._load_timeout:
                # Check if S3 file is newer than our cache
                try:
                    s3_response = self.s3_service.s3_client.head_object(
                        Bucket=self.input_bucket, 
                        Key=self.h5_key
                    )
                    s3_last_modified = s3_response['LastModified']
                    
                    if self._last_load_time.replace(tzinfo=s3_last_modified.tzinfo) >= s3_last_modified:
                        logger.info("Using cached data from S3 (file not updated)")
                        return self.data
                    else:
                        logger.info("S3 file updated, downloading fresh data...")
                        self.clear_data_cache()
                except Exception as e:
                    logger.warning(f"Could not check S3 file timestamp: {e}, using cached data")
                    return self.data
            else:
                logger.info("Cached data expired, reloading...")
                self.clear_data_cache()
            
        try:
            logger.info(f"Loading H5 data from S3: s3://{self.input_bucket}/{self.h5_key}")
            start_time = datetime.now()
            
            # Use streaming download to avoid memory issues
            self.data = self._stream_h5_from_s3()
            
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
            self._last_load_time = datetime.now()
            
            load_time = (self._last_load_time - start_time).total_seconds()
            logger.info(f"Loaded {len(self.data)} records for {self.data['Symbol'].nunique()} stocks from S3 in {load_time:.2f} seconds")
            return self.data
            
        except Exception as e:
            logger.error(f"Error loading data from S3: {e}")
            # Clear cache on error to force reload next time
            self.clear_data_cache()
            raise
    
    def _stream_h5_from_s3(self) -> pd.DataFrame:
        """
        Stream H5 file from S3 with optimized memory usage.
        
        Returns:
            DataFrame with stock data
        """
        try:
            # Get file size first for progress tracking
            response = self.s3_service.s3_client.head_object(
                Bucket=self.input_bucket, 
                Key=self.h5_key
            )
            file_size = response['ContentLength']
            logger.info(f"H5 file size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
            
            # For large files, use streaming approach
            if file_size > 100 * 1024 * 1024:  # > 100MB
                logger.info("Large file detected, using streaming approach")
                return self._stream_large_h5_file()
            else:
                # For smaller files, use direct approach but with memory optimization
                logger.info("Using optimized direct approach")
                return self._load_h5_direct()
                
        except Exception as e:
            logger.error(f"Error in streaming H5 from S3: {e}")
            raise
    
    def _stream_large_h5_file(self) -> pd.DataFrame:
        """
        Stream large H5 file in chunks to avoid memory issues.
        """
        try:
            # Download to temporary file in chunks
            temp_file_path = None
            with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as temp_file:
                temp_file_path = temp_file.name
                
                # Stream download in chunks
                response = self.s3_service.s3_client.get_object(
                    Bucket=self.input_bucket, 
                    Key=self.h5_key
                )
                
                # Download in 10MB chunks
                chunk_size = 10 * 1024 * 1024
                downloaded = 0
                
                for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
                    temp_file.write(chunk)
                    downloaded += len(chunk)
                    
                    # Log progress every 50MB
                    if downloaded % (50 * 1024 * 1024) == 0:
                        logger.info(f"Downloaded {downloaded/1024/1024:.1f} MB")
                
                temp_file.flush()
                logger.info(f"Download completed: {downloaded:,} bytes")
            
            # Convert H5 to DataFrame
            return self._convert_h5_to_dataframe_optimized(temp_file_path)
            
        except Exception as e:
            logger.error(f"Error streaming large H5 file: {e}")
            raise
        finally:
            # Clean up temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except:
                    pass
    
    def _load_h5_direct(self) -> pd.DataFrame:
        """
        Load H5 file directly for smaller files with memory optimization.
        """
        try:
            # Download H5 data from S3
            response = self.s3_service.s3_client.get_object(
                Bucket=self.input_bucket, 
                Key=self.h5_key
            )
            h5_data = response['Body'].read()
            logger.info(f"Downloaded H5 file from S3 ({len(h5_data)} bytes)")
            
            # Convert H5 to DataFrame using optimized method
            return self._convert_h5_to_dataframe_optimized_bytes(h5_data)
            
        except Exception as e:
            logger.error(f"Error loading H5 directly: {e}")
            raise

    def clear_data_cache(self):
        """Clear the cached data to free memory"""
        self.data = None
        self._data_loaded = False
        self._last_load_time = None
        logger.info("Cleared data cache")
    
    def get_data_info(self) -> Dict[str, Any]:
        """
        Get information about the current data state without loading it.
        
        Returns:
            Dictionary with data information
        """
        try:
            # Get file info from S3 without downloading
            response = self.s3_service.s3_client.head_object(
                Bucket=self.input_bucket, 
                Key=self.h5_key
            )
            
            file_size = response['ContentLength']
            last_modified = response['LastModified']
            
            return {
                "file_size_bytes": file_size,
                "file_size_mb": file_size / (1024 * 1024),
                "last_modified": last_modified.isoformat(),
                "is_loaded": self._data_loaded,
                "last_load_time": self._last_load_time.isoformat() if self._last_load_time else None,
                "cache_expired": self._is_cache_expired(),
                "estimated_load_time": self._estimate_load_time(file_size)
            }
        except Exception as e:
            logger.error(f"Error getting data info: {e}")
            return {"error": str(e)}
    
    def _is_cache_expired(self) -> bool:
        """Check if cached data has expired"""
        if not self._last_load_time:
            return True
        return (datetime.now() - self._last_load_time).seconds >= self._load_timeout
    
    def _estimate_load_time(self, file_size: int) -> float:
        """Estimate load time based on file size"""
        # Rough estimation: 10MB per second for download + processing
        estimated_seconds = (file_size / (10 * 1024 * 1024)) + 30  # Add 30s for processing
        return round(estimated_seconds, 1)
    
    def _convert_h5_to_dataframe_optimized(self, file_path: str) -> pd.DataFrame:
        """
        Convert H5 file to DataFrame with optimized memory usage.
        
        Args:
            file_path: Path to the H5 file
            
        Returns:
            DataFrame containing the stock data
        """
        try:
            # Try pandas read first (most reliable and memory efficient)
            try:
                logger.info("Attempting pandas HDF5 read...")
                df = pd.read_hdf(file_path)
                logger.info(f"Successfully loaded H5 with pandas: {len(df)} rows")
                return df
            except Exception as e:
                logger.warning(f"Pandas HDF5 read failed: {e}, trying manual reconstruction...")
            
            # Manual reconstruction for complex HDF5 structures
            with h5py.File(file_path, 'r') as f:
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
                    
                    # Get data values in chunks to avoid memory issues
                    data_blocks = []
                    for key in stage.keys():
                        if 'values' in key:
                            values = stage[key]
                            # Read in chunks if dataset is large
                            if values.size > 1000000:  # > 1M elements
                                logger.info(f"Large dataset detected, reading in chunks: {key}")
                                chunk_size = 100000  # 100K elements per chunk
                                chunks = []
                                for i in range(0, values.size, chunk_size):
                                    end_idx = min(i + chunk_size, values.size)
                                    chunk = values[i:end_idx]
                                    chunks.append(chunk)
                                values = np.concatenate(chunks)
                            else:
                                values = values[:]
                            
                            if values.ndim == 2:
                                data_blocks.append(values)
                            elif values.ndim == 1:
                                data_blocks.append(values.reshape(-1, 1))
                    
                    if data_blocks:
                        logger.info("Combining data blocks...")
                        combined_data = np.hstack(data_blocks)
                        df = pd.DataFrame(combined_data, columns=columns)
                        logger.info(f"Successfully reconstructed H5: {len(df)} rows")
                        return df
                    else:
                        raise ValueError("No data blocks found")
                else:
                    raise ValueError("No 'stage' group found in HDF5 file")
        
        except Exception as e:
            logger.error(f"Error converting H5 to DataFrame: {e}")
            raise
    
    def _convert_h5_to_dataframe_optimized_bytes(self, h5_data: bytes) -> pd.DataFrame:
        """
        Convert H5 data bytes to DataFrame with optimized memory usage.
        
        Args:
            h5_data: Raw H5 data bytes from S3
            
        Returns:
            DataFrame containing the stock data
        """
        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as temp_file:
            temp_file.write(h5_data)
            temp_file_path = temp_file.name
        
        try:
            return self._convert_h5_to_dataframe_optimized(temp_file_path)
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except:
                    pass

    def _convert_h5_to_dataframe(self, h5_data: bytes) -> pd.DataFrame:
        """
        Convert H5 data to DataFrame using the same method as returnsCalProd.
        This is the original method kept for backward compatibility.
        
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
    
    def analyze_single_stock(self, symbol: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Perform complete analysis for a single stock using S3 data.
        
        Args:
            symbol: Stock symbol to analyze
            force_refresh: Force download fresh data even if cache is valid
            
        Returns:
            Dictionary containing all analysis results for the stock
        """
        logger.info(f"Analyzing stock from S3: {symbol}")
        
        try:
            # Load data from S3 if not already loaded or if force refresh requested
            if self.data is None or force_refresh:
                self.load_data_from_s3(force_refresh=force_refresh)
            
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
