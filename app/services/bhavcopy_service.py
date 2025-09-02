import csv
import os
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger
from pathlib import Path
from .s3_service import S3Service

class BhavcopyService:
    """
    Service to handle BSE bhavcopy data operations from S3
    """
    
    def __init__(self):
        self.s3_service = S3Service()
        self.bhavcopy_files = []
        self._load_bhavcopy_files()
    
    def _load_bhavcopy_files(self):
        """Load available bhavcopy files from S3"""
        try:
            # Get summary from S3 to populate file list
            summary = self.s3_service.get_bhavcopy_summary()
            if summary.get('status') == 'success':
                self.bhavcopy_files = summary.get('files', [])
                logger.info(f"Loaded {len(self.bhavcopy_files)} bhavcopy files from S3")
            else:
                logger.error(f"Error loading bhavcopy files from S3: {summary.get('message')}")
        except Exception as e:
            logger.error(f"Error loading bhavcopy files: {e}")
    
    def get_latest_bhavcopy_file(self) -> Optional[Dict]:
        """Get the most recent bhavcopy file from S3"""
        try:
            return self.s3_service.get_latest_bhavcopy_file()
        except Exception as e:
            logger.error(f"Error getting latest bhavcopy file: {e}")
            return None
    
    def get_stock_bhavcopy_data(self, symbol: str, date: Optional[str] = None) -> Dict:
        """
        Get bhavcopy data for a specific stock symbol from S3
        
        Args:
            symbol: Stock symbol to search for
            date: Optional date filter (format: DD-MMM-YYYY)
        
        Returns:
            Dictionary containing stock data or error message
        """
        try:
            # Use latest file if no specific date provided
            file_info = self.get_latest_bhavcopy_file()
            if not file_info:
                return {
                    "status": "error",
                    "message": "No bhavcopy files found in S3"
                }
            
            # Get data from S3
            df = self.s3_service.get_bhavcopy_data(file_info['s3_key'])
            if df is None:
                return {
                    "status": "error",
                    "message": "Failed to load bhavcopy data from S3"
                }
            
            # Filter by symbol (case-insensitive)
            symbol_mask = df['SYMBOL'].str.strip().str.upper() == symbol.strip().upper()
            filtered_df = df[symbol_mask]
            
            # Apply date filter if provided
            if date:
                date_mask = df['DATE1'].str.strip() == date.strip()
                filtered_df = filtered_df[date_mask]
            
            if filtered_df.empty:
                return {
                    "status": "error",
                    "message": f"No data found for symbol: {symbol}",
                    "symbol": symbol
                }
            
            # Convert to list of dictionaries
            stock_data = []
            for _, row in filtered_df.iterrows():
                formatted_row = {
                    "symbol": row['SYMBOL'].strip(),
                    "series": row['SERIES'].strip(),
                    "date": row['DATE1'].strip(),
                    "prev_close": float(row['PREV_CLOSE']) if row['PREV_CLOSE'] != '-' else None,
                    "open_price": float(row['OPEN_PRICE']) if row['OPEN_PRICE'] != '-' else None,
                    "high_price": float(row['HIGH_PRICE']) if row['HIGH_PRICE'] != '-' else None,
                    "low_price": float(row['LOW_PRICE']) if row['LOW_PRICE'] != '-' else None,
                    "last_price": float(row['LAST_PRICE']) if row['LAST_PRICE'] != '-' else None,
                    "close_price": float(row['CLOSE_PRICE']) if row['CLOSE_PRICE'] != '-' else None,
                    "avg_price": float(row['AVG_PRICE']) if row['AVG_PRICE'] != '-' else None,
                    "total_traded_qty": int(row['TTL_TRD_QNTY']) if row['TTL_TRD_QNTY'] != '-' else None,
                    "turnover_lacs": float(row['TURNOVER_LACS']) if row['TURNOVER_LACS'] != '-' else None,
                    "no_of_trades": int(row['NO_OF_TRADES']) if row['NO_OF_TRADES'] != '-' else None,
                    "delivery_qty": int(row['DELIV_QTY']) if row['DELIV_QTY'] != '-' else None,
                    "delivery_percentage": float(row['DELIV_PER']) if row['DELIV_PER'] != '-' else None
                }
                stock_data.append(formatted_row)
            
            return {
                "status": "success",
                "symbol": symbol,
                "data": stock_data,
                "count": len(stock_data),
                "source_file": file_info['filename'],
                "source": "S3",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching bhavcopy data for {symbol}: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch bhavcopy data: {str(e)}",
                "symbol": symbol
            }
    
    def get_available_symbols(self, limit: int = 100) -> Dict:
        """
        Get list of available symbols from bhavcopy data in S3
        
        Args:
            limit: Maximum number of symbols to return
        
        Returns:
            Dictionary containing available symbols
        """
        try:
            file_info = self.get_latest_bhavcopy_file()
            if not file_info:
                return {
                    "status": "error",
                    "message": "No bhavcopy files found in S3"
                }
            
            # Get data from S3
            df = self.s3_service.get_bhavcopy_data(file_info['s3_key'])
            if df is None:
                return {
                    "status": "error",
                    "message": "Failed to load bhavcopy data from S3"
                }
            
            # Get unique symbols
            symbols = df['SYMBOL'].str.strip().unique()
            symbols = [s for s in symbols if s and s != '-']
            symbols = sorted(symbols)[:limit]
            
            return {
                "status": "success",
                "symbols": symbols,
                "count": len(symbols),
                "source_file": file_info['filename'],
                "source": "S3",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching available symbols: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch available symbols: {str(e)}"
            }
    
    def get_bhavcopy_summary(self) -> Dict:
        """
        Get summary of available bhavcopy data from S3
        
        Returns:
            Dictionary containing bhavcopy summary
        """
        try:
            return self.s3_service.get_bhavcopy_summary()
        except Exception as e:
            logger.error(f"Error fetching bhavcopy summary: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch bhavcopy summary: {str(e)}"
            }
