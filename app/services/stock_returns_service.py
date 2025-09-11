import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path

class StockReturnsService:
    """
    Service to handle stock returns data operations
    """
    
    def __init__(self):
        self.returns_file = Path("stock_returns_2025-09-10.csv")
        self.data = None
        self._load_returns_data()
    
    def _load_returns_data(self):
        """Load stock returns data from CSV file"""
        try:
            if self.returns_file.exists():
                self.data = pd.read_csv(self.returns_file)
                # Convert date columns
                self.data['Latest_Date'] = pd.to_datetime(self.data['Latest_Date'])
                logger.info(f"Loaded stock returns data for {len(self.data)} symbols")
            else:
                logger.warning("Stock returns file not found. Run returnsCalculation.py first.")
        except Exception as e:
            logger.error(f"Error loading stock returns data: {e}")
    
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
                    "message": f"No returns data found for symbol: {symbol}",
                    "symbol": symbol
                }
            
            # Get the first match
            row = symbol_data.iloc[0]
            
            # Format the data
            formatted_data = {
                "symbol": row['Symbol'],
                "fincode": str(row['Fincode']),
                "isin": row['ISIN'],
                "latest_date": row['Latest_Date'],
                "latest_close": float(row['Latest_Close']),
                "latest_volume": int(row['Latest_Volume']),
                "returns_1_week": float(row['1_Week']) if pd.notna(row['1_Week']) else None,
                "returns_1_month": float(row['1_Month']) if pd.notna(row['1_Month']) else None,
                "returns_3_months": float(row['3_Months']) if pd.notna(row['3_Months']) else None,
                "returns_6_months": float(row['6_Months']) if pd.notna(row['6_Months']) else None,
                "returns_1_year": float(row['1_Year']) if pd.notna(row['1_Year']) else None,
                "returns_3_years": float(row['3_Years']) if pd.notna(row['3_Years']) else None,
                "returns_5_years": float(row['5_Years']) if pd.notna(row['5_Years']) else None
            }
            
            return {
                "status": "success",
                "symbol": symbol,
                "data": formatted_data,
                "source_file": self.returns_file.name,
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
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
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
            
            # Convert to list of dictionaries
            records = []
            for _, row in processed_data.iterrows():
                record = {
                    "symbol": row['Symbol'],
                    "fincode": str(row['Fincode']),
                    "isin": row['ISIN'],
                    "latest_date": row['Latest_Date'].isoformat() if pd.notna(row['Latest_Date']) else None,
                    "latest_close": float(row['Latest_Close']) if pd.notna(row['Latest_Close']) else None,
                    "latest_volume": int(row['Latest_Volume']) if pd.notna(row['Latest_Volume']) else None,
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
                    "normalized_score": float(row['Normalized_Score']) if pd.notna(row['Normalized_Score']) else None
                }
                records.append(record)
            
            return {
                "status": "success",
                "data": records,
                "total_count": len(records),
                "source_file": self.returns_file.name,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching all stock returns: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch stock returns: {str(e)}"
            }
    
    def get_returns_summary(self) -> Dict:
        """
        Get summary statistics of stock returns data
        
        Returns:
            Dictionary containing returns summary statistics
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
                }
            
            # Calculate summary statistics for each return period
            return_columns = ['1_Week', '1_Month', '3_Months', '6_Months', '1_Year', '3_Years', '5_Years']
            summary = {}
            
            for col in return_columns:
                if col in self.data.columns:
                    valid_returns = self.data[col].dropna()
                    if len(valid_returns) > 0:
                        summary[col] = {
                            "mean": round(float(valid_returns.mean()), 2),
                            "median": round(float(valid_returns.median()), 2),
                            "min": round(float(valid_returns.min()), 2),
                            "max": round(float(valid_returns.max()), 2),
                            "std": round(float(valid_returns.std()), 2),
                            "count": len(valid_returns)
                        }
            
            # Get top and bottom performers for 1 year
            if '1_Year' in self.data.columns:
                top_performers = self.data.nlargest(5, '1_Year')[['Symbol', '1_Year', 'Latest_Close']]
                bottom_performers = self.data.nsmallest(5, '1_Year')[['Symbol', '1_Year', 'Latest_Close']]
                
                summary['top_performers_1y'] = [
                    {"symbol": row['Symbol'], "return": float(row['1_Year']), "price": float(row['Latest_Close'])}
                    for _, row in top_performers.iterrows()
                ]
                
                summary['bottom_performers_1y'] = [
                    {"symbol": row['Symbol'], "return": float(row['1_Year']), "price": float(row['Latest_Close'])}
                    for _, row in bottom_performers.iterrows()
                ]
            
            return {
                "status": "success",
                "summary": summary,
                "total_symbols": len(self.data),
                "source_file": self.returns_file.name,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching returns summary: {e}")
            return {
                "status": "error",
                "message": f"Failed to fetch returns summary: {str(e)}"
            }
    
    def search_symbols(self, query: str, limit: int = 20) -> Dict:
        """
        Search for symbols by partial match
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
        
        Returns:
            Dictionary containing matching symbols
        """
        try:
            if self.data is None:
                return {
                    "status": "error",
                    "message": "Stock returns data not loaded"
                }
            
            # Search for symbols containing the query (case-insensitive)
            matching_symbols = self.data[
                self.data['Symbol'].str.contains(query, case=False, na=False)
            ]
            
            # Limit results
            if limit:
                matching_symbols = matching_symbols.head(limit)
            
            # Format results
            symbols = []
            for _, row in matching_symbols.iterrows():
                symbols.append({
                    "symbol": row['Symbol'],
                    "fincode": str(row['Fincode']),
                    "latest_close": float(row['Latest_Close']) if pd.notna(row['Latest_Close']) else None,
                    "returns_1_year": float(row['1_Year']) if pd.notna(row['1_Year']) else None
                })
            
            return {
                "status": "success",
                "query": query,
                "symbols": symbols,
                "count": len(symbols),
                "source_file": self.returns_file.name,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error searching symbols: {e}")
            return {
                "status": "error",
                "message": f"Failed to search symbols: {str(e)}"
            }
    
    def refresh_data(self) -> Dict:
        """
        Refresh the stock returns data by reloading from file
        
        Returns:
            Dictionary containing refresh status
        """
        try:
            self._load_returns_data()
            
            if self.data is not None:
                return {
                    "status": "success",
                    "message": f"Data refreshed successfully. Loaded {len(self.data)} symbols",
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "status": "error",
                    "message": "Failed to refresh data"
                }
                
        except Exception as e:
            logger.error(f"Error refreshing data: {e}")
            return {
                "status": "error",
                "message": f"Failed to refresh data: {str(e)}"
            }
