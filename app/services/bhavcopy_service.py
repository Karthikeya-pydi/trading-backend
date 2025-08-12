import csv
import os
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger
from pathlib import Path

class BhavcopyService:
    """
    Service to handle BSE bhavcopy data operations
    """
    
    def __init__(self):
        self.uploads_dir = Path("uploads")
        self.bhavcopy_files = []
        self._load_bhavcopy_files()
    
    def _load_bhavcopy_files(self):
        """Load available bhavcopy files from uploads directory"""
        try:
            if self.uploads_dir.exists():
                for file_path in self.uploads_dir.glob("sec_bhavdata_full_*.csv"):
                    self.bhavcopy_files.append(file_path)
                logger.info(f"Loaded {len(self.bhavcopy_files)} bhavcopy files")
        except Exception as e:
            logger.error(f"Error loading bhavcopy files: {e}")
    
    def get_latest_bhavcopy_file(self) -> Optional[Path]:
        """Get the most recent bhavcopy file"""
        if not self.bhavcopy_files:
            return None
        
        # Sort by modification time and return the latest
        latest_file = max(self.bhavcopy_files, key=lambda x: x.stat().st_mtime)
        return latest_file
    
    def get_stock_bhavcopy_data(self, symbol: str, date: Optional[str] = None) -> Dict:
        """
        Get bhavcopy data for a specific stock symbol
        
        Args:
            symbol: Stock symbol to search for
            date: Optional date filter (format: DD-MMM-YYYY)
        
        Returns:
            Dictionary containing stock data or error message
        """
        try:
            # Use latest file if no specific date provided
            file_path = self.get_latest_bhavcopy_file()
            if not file_path:
                return {
                    "status": "error",
                    "message": "No bhavcopy files found"
                }
            
            stock_data = []
            
            with open(file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    # Check if symbol matches (case-insensitive)
                    if row['SYMBOL'].strip().upper() == symbol.strip().upper():
                        # Apply date filter if provided
                        if date and row['DATE1'].strip() != date.strip():
                            continue
                        
                        # Format the data
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
            
            if not stock_data:
                return {
                    "status": "error",
                    "message": f"No data found for symbol: {symbol}",
                    "symbol": symbol
                }
            
            return {
                "status": "success",
                "symbol": symbol,
                "data": stock_data,
                "count": len(stock_data),
                "source_file": file_path.name,
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
        Get list of available symbols from bhavcopy data
        
        Args:
            limit: Maximum number of symbols to return
        
        Returns:
            Dictionary containing available symbols
        """
        try:
            file_path = self.get_latest_bhavcopy_file()
            if not file_path:
                return {
                    "status": "error",
                    "message": "No bhavcopy files found"
                }
            
            symbols = set()
            
            with open(file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    symbol = row['SYMBOL'].strip()
                    if symbol and symbol != '-':
                        symbols.add(symbol)
                    
                    if len(symbols) >= limit:
                        break
            
            return {
                "status": "success",
                "symbols": sorted(list(symbols)),
                "count": len(symbols),
                "source_file": file_path.name,
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
        Get summary of available bhavcopy data
        
        Returns:
            Dictionary containing bhavcopy summary
        """
        try:
            if not self.bhavcopy_files:
                return {
                    "status": "error",
                    "message": "No bhavcopy files found"
                }
            
            summary = []
            for file_path in self.bhavcopy_files:
                file_stats = file_path.stat()
                summary.append({
                    "filename": file_path.name,
                    "size_mb": round(file_stats.st_size / (1024 * 1024), 2),
                    "modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                    "path": str(file_path)
                })
            
            return {
                "status": "success",
                "files": summary,
                "total_files": len(summary),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching bhavcopy summary: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch bhavcopy summary: {str(e)}"
            }
