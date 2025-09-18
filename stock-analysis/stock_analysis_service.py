"""
Comprehensive Stock Data Analysis Pipeline Service

This service implements a complete per-stock analysis pipeline that processes
historical data from 2003 onwards, creating metrics and flags unique to each
stock's performance and data quality.
"""

import pandas as pd
import numpy as np
import h5py
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Any
from loguru import logger
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


class StockAnalysisService:
    """
    Service for comprehensive stock data analysis pipeline.
    
    This service processes each stock individually, applying the complete
    analysis pipeline from data preparation to final metrics generation.
    """
    
    def __init__(self, h5_file_path: str = "Our_Nseadjprice.h5"):
        """
        Initialize the StockAnalysisService.
        
        Args:
            h5_file_path: Path to the H5 file containing stock data
        """
        self.h5_file_path = h5_file_path
        self.data = None
        self.analysis_results = {}
        
    def load_data(self) -> pd.DataFrame:
        """
        Load and preprocess the H5 data file.
        
        Returns:
            DataFrame with stock data
        """
        try:
            logger.info(f"Loading data from {self.h5_file_path}")
            
            # Use pandas to read the H5 file directly
            # This is more reliable than manually reconstructing from HDF5 components
            self.data = pd.read_hdf(self.h5_file_path, key='stage')
            
            # Convert Date column to datetime
            self.data['Date'] = pd.to_datetime(self.data['Date'])
            
            # Convert numeric columns
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            
            # Sort by Symbol and Date
            self.data = self.data.sort_values(['Symbol', 'Date']).reset_index(drop=True)
            
            logger.info(f"Loaded {len(self.data)} records for {self.data['Symbol'].nunique()} stocks")
            return self.data
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def get_unique_stocks(self) -> List[str]:
        """
        Get list of unique stock symbols.
        
        Returns:
            List of unique stock symbols
        """
        if self.data is None:
            self.load_data()
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
            self.load_data()
        
        # Filter for specific stock and date range
        stock_data = self.data[
            (self.data['Symbol'] == symbol) & 
            (self.data['Date'] >= start_date)
        ].copy()
        
        # Sort by date
        stock_data = stock_data.sort_values('Date').reset_index(drop=True)
        
        return stock_data
    
    def calculate_log_returns(self, prices: pd.Series) -> pd.Series:
        """
        Calculate log returns for a price series.
        
        Args:
            prices: Price series
            
        Returns:
            Log returns series
        """
        return np.log(prices / prices.shift(1))
    
    def calculate_global_mad_analysis(self, returns: pd.Series) -> Dict[str, Any]:
        """
        Perform global MAD analysis and create outlier flags.
        
        Args:
            returns: Log returns series
            
        Returns:
            Dictionary containing global MAD metrics and flags
        """
        # Remove NaN values
        clean_returns = returns.dropna()
        
        if len(clean_returns) == 0:
            return {
                'global_median': np.nan,
                'global_mad': np.nan,
                'global_outlier_flag': pd.Series([False] * len(returns), index=returns.index)
            }
        
        # Calculate global median and MAD
        global_median = np.median(clean_returns)
        global_mad = np.median(np.abs(clean_returns - global_median))
        
        # Calculate robust z-scores
        robust_z_scores = 0.6745 * (returns - global_median) / global_mad
        
        # Create global outlier flag (|z| > 6)
        global_outlier_flag = np.abs(robust_z_scores) > 6
        
        return {
            'global_median': global_median,
            'global_mad': global_mad,
            'global_outlier_flag': global_outlier_flag
        }
    
    def calculate_rolling_window_analysis(self, returns: pd.Series, 
                                        window_sizes: List[int] = [10, 40, 120]) -> Dict[str, Any]:
        """
        Perform rolling window analysis for regime detection.
        
        Args:
            returns: Log returns series
            window_sizes: List of window sizes to analyze
            
        Returns:
            Dictionary containing rolling window metrics and flags
        """
        results = {}
        
        for window_size in window_sizes:
            # Calculate window readiness (80% of window size)
            window_nobs = returns.rolling(window=window_size, min_periods=1).count()
            window_ready = window_nobs >= 0.8 * window_size
            results[f'window_ready_{window_size}'] = window_ready
            results[f'window_nobs_{window_size}'] = window_nobs
        
        # Special analysis for 40-day window
        if 40 in window_sizes:
            # Calculate rolling median and MAD for 40-day window
            rolling_median = returns.rolling(window=40, min_periods=1).median()
            rolling_mad = returns.rolling(window=40, min_periods=1).apply(
                lambda x: np.median(np.abs(x - np.median(x))) if len(x.dropna()) > 0 else np.nan
            )
            
            # Calculate rolling robust z-scores
            rolling_robust_z = 0.6745 * (returns - rolling_median) / rolling_mad
            
            # Create anomaly flags
            mild_anomaly_flag = np.abs(rolling_robust_z) > 3
            major_anomaly_flag = np.abs(rolling_robust_z) > 6
            
            results.update({
                'rolling_median_40': rolling_median,
                'rolling_mad_40': rolling_mad,
                'rolling_robust_z_40': rolling_robust_z,
                'mild_anomaly_flag': mild_anomaly_flag,
                'major_anomaly_flag': major_anomaly_flag
            })
        
        return results
    
    def calculate_per_stock_outlier_flags(self, returns: pd.Series) -> Dict[str, Any]:
        """
        Calculate per-stock outlier flags using stock-specific MAD.
        
        Args:
            returns: Log returns series
            
        Returns:
            Dictionary containing per-stock outlier flags
        """
        # Remove NaN values
        clean_returns = returns.dropna()
        
        if len(clean_returns) == 0:
            return {
                'per_stock_median': np.nan,
                'per_stock_mad': np.nan,
                'robust_outlier_flag': pd.Series([False] * len(returns), index=returns.index),
                'very_extreme_flag': pd.Series([False] * len(returns), index=returns.index)
            }
        
        # Calculate per-stock median and MAD
        per_stock_median = np.median(clean_returns)
        per_stock_mad = np.median(np.abs(clean_returns - per_stock_median))
        
        # Calculate robust z-scores
        robust_z_scores = 0.6745 * (returns - per_stock_median) / per_stock_mad
        
        # Create outlier flags
        robust_outlier_flag = np.abs(robust_z_scores) > 6
        very_extreme_flag = np.abs(robust_z_scores) > 10
        
        return {
            'per_stock_median': per_stock_median,
            'per_stock_mad': per_stock_mad,
            'robust_outlier_flag': robust_outlier_flag,
            'very_extreme_flag': very_extreme_flag
        }
    
    def generate_descriptive_snapshot(self, stock_data: pd.DataFrame, 
                                    returns: pd.Series) -> Dict[str, Any]:
        """
        Generate descriptive snapshot and illiquid flag for a stock.
        
        Args:
            stock_data: Stock data DataFrame
            returns: Log returns series
            
        Returns:
            Dictionary containing descriptive statistics and flags
        """
        # Calculate basic statistics
        clean_returns = returns.dropna()
        
        if len(clean_returns) == 0:
            return {
                'n_days': 0,
                'pct_missing': 100.0,
                'start_date': None,
                'end_date': None,
                'illiquid_flag': True
            }
        
        # Calculate missing percentage
        total_possible_days = len(returns)
        missing_days = total_possible_days - len(clean_returns)
        pct_missing = (missing_days / total_possible_days) * 100
        
        # Calculate descriptive statistics
        stats = {
            'n_days': len(clean_returns),
            'pct_missing': pct_missing,
            'start_date': stock_data['Date'].min(),
            'end_date': stock_data['Date'].max(),
            'mean_return': np.mean(clean_returns),
            'std_return': np.std(clean_returns),
            'skew_return': clean_returns.skew(),
            'kurtosis_return': clean_returns.kurtosis(),
            'min_return': np.min(clean_returns),
            'p1_return': np.percentile(clean_returns, 1),
            'p5_return': np.percentile(clean_returns, 5),
            'p95_return': np.percentile(clean_returns, 95),
            'p99_return': np.percentile(clean_returns, 99),
            'max_return': np.max(clean_returns),
            'illiquid_flag': pct_missing > 30.0
        }
        
        return stats
    
    def analyze_single_stock(self, symbol: str) -> Dict[str, Any]:
        """
        Perform complete analysis for a single stock.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Dictionary containing all analysis results for the stock
        """
        logger.info(f"Analyzing stock: {symbol}")
        
        try:
            # Step 1: Data preparation and filtering
            stock_data = self.filter_data_for_stock(symbol)
            
            if len(stock_data) == 0:
                logger.warning(f"No data found for stock: {symbol}")
                return {'error': f'No data found for stock: {symbol}'}
            
            # Calculate log returns
            returns = self.calculate_log_returns(stock_data['Close'])
            
            # Step 2: Global MAD analysis
            global_analysis = self.calculate_global_mad_analysis(returns)
            
            # Step 3: Rolling window analysis
            rolling_analysis = self.calculate_rolling_window_analysis(returns)
            
            # Step 4: Per-stock outlier flags
            per_stock_analysis = self.calculate_per_stock_outlier_flags(returns)
            
            # Step 5: Descriptive snapshot
            descriptive_stats = self.generate_descriptive_snapshot(stock_data, returns)
            
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
            
            logger.info(f"Completed analysis for stock: {symbol}")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error analyzing stock {symbol}: {e}")
            return {'error': f'Error analyzing stock {symbol}: {str(e)}'}
    
    def analyze_all_stocks(self, max_stocks: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform analysis for all stocks in the dataset.
        
        Args:
            max_stocks: Maximum number of stocks to analyze (None for all)
            
        Returns:
            Dictionary containing analysis results for all stocks
        """
        logger.info("Starting comprehensive stock analysis pipeline")
        
        # Load data if not already loaded
        if self.data is None:
            self.load_data()
        
        # Get unique stocks
        unique_stocks = self.get_unique_stocks()
        
        if max_stocks:
            unique_stocks = unique_stocks[:max_stocks]
        
        logger.info(f"Analyzing {len(unique_stocks)} stocks")
        
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
        
        logger.info(f"Analysis complete. Success: {successful_analyses}, Failed: {failed_analyses}")
        
        return {
            'summary': summary,
            'results': all_results
        }
    
    def export_results_to_csv(self, results: Dict[str, Any], 
                            output_path: str = "stock_analysis_results.csv") -> str:
        """
        Export analysis results to CSV format.
        
        Args:
            results: Analysis results dictionary
            output_path: Output file path
            
        Returns:
            Path to the exported file
        """
        try:
            # Create a list to store all stock data
            all_stock_data = []
            
            for symbol, analysis in results['results'].items():
                if 'enhanced_data' in analysis:
                    stock_df = analysis['enhanced_data'].copy()
                    stock_df['analysis_symbol'] = symbol
                    all_stock_data.append(stock_df)
            
            if all_stock_data:
                # Combine all stock data
                combined_df = pd.concat(all_stock_data, ignore_index=True)
                
                # Export to CSV
                combined_df.to_csv(output_path, index=False)
                logger.info(f"Results exported to {output_path}")
                return output_path
            else:
                logger.warning("No data to export")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting results: {e}")
            raise
