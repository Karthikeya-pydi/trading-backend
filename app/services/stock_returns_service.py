import pandas as pd
from typing import Dict, Optional
from datetime import datetime
from loguru import logger
from .s3_service import S3Service

class StockReturnsService:
    """
    Service to handle stock returns data operations from S3
    """
    
    def __init__(self):
        self.s3_service = S3Service()
        self.data = None
        self.current_file_info = None
    
    def _format_stock_record(self, row: pd.Series) -> dict:
        """Helper function to format stock data from pandas row"""
        return {
            "symbol": row['Symbol'],
            "fincode": str(row['Fincode']),
            "isin": row['ISIN'],
            "latest_date": row['Latest_Date'],
            "latest_close": float(row['Latest_Close']),
            "latest_volume": int(row['Latest_Volume']),
            "turnover": float(row['Turnover']) if pd.notna(row['Turnover']) else None,
            "returns_1_week": float(row['1_Week']) if pd.notna(row['1_Week']) else None,
            "returns_1_month": float(row['1_Month']) if pd.notna(row['1_Month']) else None,
            "returns_3_months": float(row['3_Months']) if pd.notna(row['3_Months']) else None,
            "returns_6_months": float(row['6_Months']) if pd.notna(row['6_Months']) else None,
            "returns_9_months": float(row['9_Months']) if pd.notna(row['9_Months']) else None,
            "returns_1_year": float(row['1_Year']) if pd.notna(row['1_Year']) else None,
            "returns_3_years": float(row['3_Years']) if pd.notna(row['3_Years']) else None,
            "returns_5_years": float(row['5_Years']) if pd.notna(row['5_Years']) else None,
            "raw_score": float(row['Raw_Score']) if pd.notna(row['Raw_Score']) else None,
            
            # Historical Raw Scores
            "raw_score_1_week_ago": float(row['1_Week_Raw_Score']) if pd.notna(row.get('1_Week_Raw_Score')) else None,
            "raw_score_1_month_ago": float(row['1_Month_Raw_Score']) if pd.notna(row.get('1_Month_Raw_Score')) else None,
            "raw_score_3_months_ago": float(row['3_Months_Raw_Score']) if pd.notna(row.get('3_Months_Raw_Score')) else None,
            "raw_score_6_months_ago": float(row['6_Months_Raw_Score']) if pd.notna(row.get('6_Months_Raw_Score')) else None,
            "raw_score_9_months_ago": float(row['9_Months_Raw_Score']) if pd.notna(row.get('9_Months_Raw_Score')) else None,
            "raw_score_1_year_ago": float(row['1_Year_Raw_Score']) if pd.notna(row.get('1_Year_Raw_Score')) else None,
            
            # Percentage Changes in Scores
            "score_change_1_week": float(row['%change_1week']) if pd.notna(row.get('%change_1week')) else None,
            "score_change_1_month": float(row['%change_1month']) if pd.notna(row.get('%change_1month')) else None,
            "score_change_3_months": float(row['%change_3months']) if pd.notna(row.get('%change_3months')) else None,
            "score_change_6_months": float(row['%change_6months']) if pd.notna(row.get('%change_6months')) else None,
            "score_change_9_months": float(row['%change_9months']) if pd.notna(row.get('%change_9months')) else None,
            "score_change_1_year": float(row['%change_1year']) if pd.notna(row.get('%change_1year')) else None,
            
            # Sign Pattern Comparisons
            "sign_pattern_1_week": str(row['symbol_1week']) if pd.notna(row.get('symbol_1week')) else None,
            "sign_pattern_1_month": str(row['symbol_1month']) if pd.notna(row.get('symbol_1month')) else None,
            "sign_pattern_3_months": str(row['symbol_3months']) if pd.notna(row.get('symbol_3months')) else None,
            "sign_pattern_6_months": str(row['symbol_6months']) if pd.notna(row.get('symbol_6months')) else None,
            "sign_pattern_9_months": str(row['symbol_9months']) if pd.notna(row.get('symbol_9months')) else None,
            "sign_pattern_1_year": str(row['symbol_1year']) if pd.notna(row.get('symbol_1year')) else None
        }
    
    def _load_returns_data(self):
        """Load stock returns data from S3"""
        try:
            # Get latest file from S3
            file_info = self.s3_service.get_latest_adjusted_eq_file()
            if not file_info:
                logger.warning("No adjusted-eq-data files found in S3")
                return
            
            # Load data from S3
            self.data = self.s3_service.get_adjusted_eq_data(file_info['s3_key'])
            if self.data is not None:
                # Convert date columns
                self.data['Latest_Date'] = pd.to_datetime(self.data['Latest_Date'])
                self.current_file_info = file_info
                logger.info(f"Loaded stock returns data for {len(self.data)} symbols from S3")
            else:
                logger.error("Failed to load stock returns data from S3")
        except Exception as e:
            logger.error(f"Error loading stock returns data from S3: {e}")
    
    def get_stock_returns(self, symbol: str) -> Dict:
        """
        Get returns data for a specific stock symbol
        
        Args:
            symbol: Stock symbol to search for
        
        Returns:
            Dictionary containing stock returns data or error message
        """
        try:
            # Load data from S3 if not already loaded
            if self.data is None:
                self._load_returns_data()
            
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not available from S3"
                }
            
            # Search for symbol (case-insensitive)
            symbol_data = self.data[
                self.data['Symbol'].str.upper() == symbol.strip().upper()
            ]
            
            if symbol_data.empty:
                return {
                    "status": "error",
                    "message": f"No returns data found for symbol: {symbol}",
                    "symbol": symbol
                }
            
            # Get the first match
            row = symbol_data.iloc[0]
            
            # Format the data using helper function
            formatted_data = self._format_stock_record(row)
            
            return {
                "status": "success",
                "symbol": symbol,
                "data": formatted_data,
                "source_file": self.current_file_info['filename'] if self.current_file_info else "unknown",
                "source": "S3",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching stock returns for {symbol}: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch stock returns: {str(e)}",
                "symbol": symbol
            }
    
    def get_all_returns(self, limit: Optional[int] = None, 
                       sort_by: str = '1_Year', 
                       sort_order: str = 'desc') -> Dict:
        """
        Get all stock returns data with optional filtering and sorting
        
        Args:
            limit: Maximum number of records to return
            sort_by: Column to sort by (default: '1_Year')
            sort_order: Sort order ('asc' or 'desc')
        
        Returns:
            Dictionary containing all stock returns data
        """
        try:
            # Load data from S3 if not already loaded
            if self.data is None:
                self._load_returns_data()
            
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not available from S3"
                }
            
            # Create a copy for processing
            processed_data = self.data.copy()
            
            # Sort the data
            if sort_by in processed_data.columns:
                processed_data = processed_data.sort_values(
                    by=sort_by, 
                    ascending=(sort_order == 'asc'),
                    na_position='last'
                )
            
            # Apply limit if specified
            if limit:
                processed_data = processed_data.head(limit)
            
            # Convert to list of dictionaries using helper function
            records = []
            for _, row in processed_data.iterrows():
                record = self._format_stock_record(row)
                # Convert date to ISO format for list responses
                if record['latest_date']:
                    record['latest_date'] = record['latest_date'].isoformat()
                records.append(record)
            
            return {
                "status": "success",
                "data": records,
                "total_count": len(records),
                "source_file": self.current_file_info['filename'] if self.current_file_info else "unknown",
                "source": "S3",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching all stock returns: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch stock returns: {str(e)}"
            }
    
    def get_available_files(self) -> Dict:
        """
        Get list of available adjusted-eq-data files from S3
        
        Returns:
            Dictionary containing available files
        """
        try:
            return self.s3_service.get_adjusted_eq_summary()
        except Exception as e:
            logger.error(f"Error fetching available files: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch available files: {str(e)}"
            }