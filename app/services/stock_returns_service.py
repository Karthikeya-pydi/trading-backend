import pandas as pd
import numpy as np
import boto3
import io
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger
import os

class StockReturnsService:
    """
    Service to handle stock returns data operations from S3
    """
    
    def __init__(self):
        self.s3_bucket = "trading-platform-csvs"
        self.s3_prefix = "adjusted-eq-data"
        self.data = None
        self.s3_client = None
        self._init_s3_client()
        self._load_returns_data_from_s3()
    
    def _init_s3_client(self):
        """Initialize S3 client"""
        try:
            self.s3_client = boto3.client('s3')
            logger.info("S3 client initialized")
        except Exception as e:
            logger.error(f"Error initializing S3 client: {e}")
            raise
    
    def _get_latest_s3_file(self) -> str:
        """Get the latest file from S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=self.s3_prefix
            )
            
            if 'Contents' not in response:
                raise ValueError("No files found in S3 bucket")
            
            # Get all CSV files and sort by last modified date
            csv_files = [
                obj for obj in response['Contents'] 
                if obj['Key'].endswith('.csv')
            ]
            
            if not csv_files:
                raise ValueError("No CSV files found in S3 bucket")
            
            # Sort by last modified date (newest first)
            latest_file = max(csv_files, key=lambda x: x['LastModified'])
            
            logger.info(f"Latest file found: {latest_file['Key']}")
            return latest_file['Key']
            
        except Exception as e:
            logger.error(f"Error getting latest S3 file: {e}")
            raise
    
    def _load_returns_data_from_s3(self):
        """Load stock returns data from latest S3 file"""
        try:
            # Get the latest file from S3
            latest_file_key = self._get_latest_s3_file()
            
            # Download the file content
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=latest_file_key
            )
            
            # Read CSV data from S3
            csv_data = response['Body'].read().decode('utf-8')
            self.data = pd.read_csv(io.StringIO(csv_data))
            
            # Convert date columns
            self.data['Latest_Date'] = pd.to_datetime(self.data['Latest_Date'])
            
            logger.info(f"Loaded stock returns data from S3: {latest_file_key}")
            logger.info(f"Data contains {len(self.data)} symbols")
            
        except Exception as e:
            logger.error(f"Error loading stock returns data from S3: {e}")
            raise
    
    def refresh_data_from_s3(self):
        """Refresh data from S3 to get latest data"""
        try:
            self._load_returns_data_from_s3()
            logger.info("Data refreshed from S3 successfully")
        except Exception as e:
            logger.error(f"Error refreshing data from S3: {e}")
            raise
    
    def get_latest_file_info(self) -> Dict:
        """Get information about the latest file being used"""
        try:
            latest_file_key = self._get_latest_s3_file()
            response = self.s3_client.head_object(
                Bucket=self.s3_bucket,
                Key=latest_file_key
            )
            return {
                "source": "S3",
                "file_key": latest_file_key,
                "last_modified": response['LastModified'],
                "size_bytes": response['ContentLength'],
                "bucket": self.s3_bucket
            }
        except Exception as e:
            return {"source": "Error", "error": str(e)}
    
    def get_stock_returns(self, symbol: str) -> Dict:
        """
        Get returns data for a specific stock symbol
        
        Args:
            symbol: Stock symbol to search for
        
        Returns:
            Dictionary containing stock returns data or error message
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
                }
            
            # Search for symbol (case-insensitive)
            symbol_data = self.data[
                self.data['Symbol'].str.upper() == symbol.strip().upper()
            ]
            
            if symbol_data.empty:
                return {
                    "status": "error",
                    "message": f"Symbol '{symbol}' not found in returns data"
                }
            
            # Get the first match
            stock_info = symbol_data.iloc[0].to_dict()
            
            # Format the response
            return {
                "status": "success",
                "symbol": stock_info['Symbol'],
                "data": {
                    "fincode": stock_info.get('Fincode', ''),
                    "isin": stock_info.get('ISIN', ''),
                    "latest_date": stock_info['Latest_Date'].strftime('%Y-%m-%d'),
                    "latest_close": float(stock_info['Latest_Close']),
                    "latest_volume": int(stock_info.get('Latest_Volume', 0)),
                    "turnover": float(stock_info.get('Turnover', 0)),
                    "returns": {
                        "1_week": float(stock_info.get('1_Week', 0)),
                        "1_month": float(stock_info.get('1_Month', 0)),
                        "3_months": float(stock_info.get('3_Months', 0)),
                        "6_months": float(stock_info.get('6_Months', 0)),
                        "9_months": float(stock_info.get('9_Months', 0)),
                        "1_year": float(stock_info.get('1_Year', 0)),
                        "3_years": float(stock_info.get('3_Years', 0)),
                        "5_years": float(stock_info.get('5_Years', 0))
                    },
                    "scores": {
                        "raw_score": float(stock_info.get('Raw_Score', 0)),
                        "normalized_score": float(stock_info.get('Normalized_Score', 0))
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting stock returns for {symbol}: {e}")
            return {
                "status": "error",
                "message": f"Error retrieving data for {symbol}: {str(e)}"
            }
    
    def get_top_performers(self, limit: int = 10, period: str = "1_Year") -> Dict:
        """
        Get top performing stocks by a specific return period
        
        Args:
            limit: Number of top performers to return
            period: Return period to sort by (e.g., '1_Year', '6_Months')
        
        Returns:
            Dictionary containing top performers data
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
                }
            
            if period not in self.data.columns:
                return {
                    "status": "error",
                    "message": f"Invalid period '{period}'. Available periods: {list(self.data.columns)}"
                }
            
            # Filter out NaN values and sort by period
            valid_data = self.data.dropna(subset=[period])
            top_performers = valid_data.nlargest(limit, period)
            
            performers = []
            for _, row in top_performers.iterrows():
                performers.append({
                    "symbol": row['Symbol'],
                    "return_percent": float(row[period]),
                    "latest_close": float(row['Latest_Close']),
                    "raw_score": float(row.get('Raw_Score', 0)),
                    "normalized_score": float(row.get('Normalized_Score', 0))
                })
            
            return {
                "status": "success",
                "period": period,
                "count": len(performers),
                "performers": performers
            }
            
        except Exception as e:
            logger.error(f"Error getting top performers: {e}")
            return {
                "status": "error",
                "message": f"Error retrieving top performers: {str(e)}"
            }
    
    def get_stock_scores_summary(self) -> Dict:
        """
        Get summary statistics of stock scores
        
        Returns:
            Dictionary containing score statistics
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
                }
            
            if 'Raw_Score' not in self.data.columns:
                return {
                    "status": "error",
                    "message": "Score data not available"
                }
            
            # Get valid scores
            valid_scores = self.data['Raw_Score'].dropna()
            valid_norm_scores = self.data['Normalized_Score'].dropna()
            
            if len(valid_scores) == 0:
                return {
                    "status": "error",
                    "message": "No valid score data found"
                }
            
            return {
                "status": "success",
                "summary": {
                    "total_stocks": len(self.data),
                    "stocks_with_scores": len(valid_scores),
                    "raw_score_stats": {
                        "mean": float(valid_scores.mean()),
                        "median": float(valid_scores.median()),
                        "min": float(valid_scores.min()),
                        "max": float(valid_scores.max()),
                        "std": float(valid_scores.std())
                    },
                    "normalized_score_stats": {
                        "mean": float(valid_norm_scores.mean()),
                        "median": float(valid_norm_scores.median()),
                        "min": float(valid_norm_scores.min()),
                        "max": float(valid_norm_scores.max()),
                        "std": float(valid_norm_scores.std())
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting score summary: {e}")
            return {
                "status": "error",
                "message": f"Error retrieving score summary: {str(e)}"
            }
    
    def search_stocks(self, query: str, limit: int = 10) -> Dict:
        """
        Search for stocks by symbol (partial match)
        
        Args:
            query: Search query (partial symbol match)
            limit: Maximum number of results to return
        
        Returns:
            Dictionary containing search results
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
                }
            
            # Search for partial matches (case-insensitive)
            query_upper = query.strip().upper()
            matches = self.data[
                self.data['Symbol'].str.upper().str.contains(query_upper, na=False)
            ].head(limit)
            
            if matches.empty:
                return {
                    "status": "success",
                    "query": query,
                    "count": 0,
                    "results": []
                }
            
            results = []
            for _, row in matches.iterrows():
                results.append({
                    "symbol": row['Symbol'],
                    "fincode": row.get('Fincode', ''),
                    "latest_close": float(row['Latest_Close']),
                    "1_year_return": float(row.get('1_Year', 0)),
                    "raw_score": float(row.get('Raw_Score', 0)),
                    "normalized_score": float(row.get('Normalized_Score', 0))
                })
            
            return {
                "status": "success",
                "query": query,
                "count": len(results),
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Error searching stocks: {e}")
            return {
                "status": "error",
                "message": f"Error searching stocks: {str(e)}"
            }
    
    def get_data_summary(self) -> Dict:
        """
        Get overall summary of the loaded data
        
        Returns:
            Dictionary containing data summary
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "No data loaded"
                }
            
            # Get file info
            file_info = self.get_latest_file_info()
            
            return {
                "status": "success",
                "summary": {
                    "total_symbols": len(self.data),
                    "data_date_range": {
                        "earliest": self.data['Latest_Date'].min().strftime('%Y-%m-%d'),
                        "latest": self.data['Latest_Date'].max().strftime('%Y-%m-%d')
                    },
                    "file_info": file_info,
                    "available_columns": list(self.data.columns),
                    "has_scores": 'Raw_Score' in self.data.columns
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {
                "status": "error",
                "message": f"Error retrieving data summary: {str(e)}"
            }