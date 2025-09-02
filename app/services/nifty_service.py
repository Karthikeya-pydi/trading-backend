import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from loguru import logger
from .s3_service import S3Service


class NiftyService:
    """
    Service to handle Nifty indices data operations from S3
    """
    
    def __init__(self):
        self.s3_service = S3Service()
    
    def get_available_indices(self) -> List[Dict[str, Any]]:
        """
        Get list of available nifty indices from S3
        
        Returns:
            List of dictionaries containing index metadata
        """
        try:
            return self.s3_service.get_available_nifty_indices()
        except Exception as e:
            logger.error(f"Error getting available nifty indices: {e}")
            return []
    
    def get_index_data(self, index_name: str) -> Dict[str, Any]:
        """
        Get data for a specific nifty index from S3
        
        Args:
            index_name: Name of the nifty index
            
        Returns:
            Dictionary containing index data or error message
        """
        try:
            # Get file info for the specific index
            file_info = self.s3_service.get_latest_nifty_file(index_name)
            if not file_info:
                return {
                    "status": "error",
                    "message": f"Index '{index_name}' not found in S3"
                }
            
            # Get data from S3
            df = self.s3_service.get_nifty_data(file_info['s3_key'])
            if df is None:
                return {
                    "status": "error",
                    "message": "Failed to load nifty index data from S3"
                }
            
            records = df.to_dict('records')
            columns = list(df.columns) if not df.empty else []
            
            return {
                "status": "success",
                "index_name": index_name,
                "filename": file_info['filename'],
                "s3_key": file_info['s3_key'],
                "total_constituents": len(records),
                "data_size_bytes": file_info['size'],
                "source": "S3",
                "columns": columns,
                "data": records,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting nifty index data for {index_name}: {e}")
            return {
                "status": "error",
                "message": f"Failed to get nifty index data: {str(e)}",
                "index_name": index_name
            }
    
    def get_index_constituents(self, index_name: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Get constituent stocks for a specific nifty index from S3
        
        Args:
            index_name: Name of the nifty index
            limit: Optional limit on number of constituents to return
            
        Returns:
            Dictionary containing constituent data or error message
        """
        try:
            result = self.get_index_data(index_name)
            if result.get("status") != "success":
                return result
            
            data = result.get("data", [])
            if limit and limit > 0:
                data = data[:limit]
            
            return {
                "status": "success",
                "index_name": index_name,
                "constituents": data,
                "count": len(data),
                "source": "S3",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting nifty index constituents for {index_name}: {e}")
            return {
                "status": "error",
                "message": f"Failed to get nifty index constituents: {str(e)}",
                "index_name": index_name
            }
    
    def search_index_by_name(self, search_term: str) -> List[Dict[str, Any]]:
        """
        Search for nifty indices by name
        
        Args:
            search_term: Search term to match against index names
            
        Returns:
            List of matching indices
        """
        try:
            all_indices = self.get_available_indices()
            search_term_lower = search_term.lower()
            
            matching_indices = [
                idx for idx in all_indices
                if search_term_lower in idx['index_name'].lower()
            ]
            
            return matching_indices
            
        except Exception as e:
            logger.error(f"Error searching nifty indices: {e}")
            return []
    
    def get_index_summary(self) -> Dict[str, Any]:
        """
        Get summary of all available nifty indices
        
        Returns:
            Dictionary containing summary information
        """
        try:
            indices = self.get_available_indices()
            
            return {
                "status": "success",
                "total_indices": len(indices),
                "indices": indices,
                "source": "S3",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting nifty index summary: {e}")
            return {
                "status": "error",
                "message": f"Failed to get nifty index summary: {str(e)}"
            }
