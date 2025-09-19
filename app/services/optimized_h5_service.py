"""
Optimized H5 Service for Production
Handles large H5 files efficiently with streaming and chunking
"""

import boto3
import pandas as pd
import h5py
import tempfile
import os
import io
from typing import Dict, List, Optional, Any, Generator
from datetime import datetime
from loguru import logger
from botocore.exceptions import ClientError
import numpy as np
from app.core.config import settings


class OptimizedH5Service:
    """
    Optimized H5 service that handles large files efficiently
    """
    
    def __init__(self, bucket_name: str = "parquet-eq-data", h5_key: str = "nse_data/Our_Nseadjprice.h5"):
        self.bucket_name = bucket_name
        self.h5_key = h5_key
        self.s3_client = boto3.client('s3', region_name=settings.aws_region)
        self._cache = {}
        
    def get_file_info(self) -> Dict[str, Any]:
        """Get H5 file information without downloading"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=self.h5_key)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'etag': response['ETag'],
                'content_type': response.get('ContentType', 'application/octet-stream')
            }
        except ClientError as e:
            logger.error(f"Error getting file info: {e}")
            return {}
    
    def stream_h5_data(self, chunk_size: int = 1024 * 1024) -> Generator[bytes, None, None]:
        """Stream H5 file in chunks to avoid memory issues"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.h5_key
            )
            
            # Stream data in chunks
            for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
                yield chunk
                
        except ClientError as e:
            logger.error(f"Error streaming H5 data: {e}")
            raise
    
    def convert_h5_to_parquet_streaming(self, output_path: str = None) -> str:
        """
        Convert H5 to Parquet using streaming to avoid memory issues
        Parquet is more efficient for large datasets
        """
        try:
            if not output_path:
                output_path = tempfile.mktemp(suffix='.parquet')
            
            logger.info(f"Converting H5 to Parquet: {self.h5_key}")
            
            # Stream H5 data to temporary file
            temp_h5_path = tempfile.mktemp(suffix='.h5')
            
            with open(temp_h5_path, 'wb') as temp_file:
                for chunk in self.stream_h5_data():
                    temp_file.write(chunk)
            
            # Convert to DataFrame efficiently
            df = self._convert_h5_to_dataframe_optimized(temp_h5_path)
            
            # Save as Parquet (much more efficient)
            df.to_parquet(output_path, compression='snappy', index=False)
            
            # Clean up temp file
            os.unlink(temp_h5_path)
            
            logger.info(f"Successfully converted to Parquet: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting H5 to Parquet: {e}")
            raise
    
    def _convert_h5_to_dataframe_optimized(self, h5_path: str) -> pd.DataFrame:
        """Optimized H5 to DataFrame conversion"""
        try:
            # Try pandas read first (most reliable)
            try:
                return pd.read_hdf(h5_path)
            except Exception as e:
                logger.warning(f"Pandas read failed: {e}, trying manual conversion")
            
            # Manual reconstruction for complex HDF5 structures
            with h5py.File(h5_path, 'r') as f:
                if 'stage' in f:
                    stage = f['stage']
                    
                    # Get column names efficiently
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
                            # Process in chunks if large
                            if values.size > 1000000:  # 1M elements
                                chunk_size = 100000
                                for i in range(0, values.size, chunk_size):
                                    chunk = values[i:i+chunk_size]
                                    if chunk.ndim == 2:
                                        data_blocks.append(chunk)
                                    elif chunk.ndim == 1:
                                        data_blocks.append(chunk.reshape(-1, 1))
                            else:
                                values = values[:]
                                if values.ndim == 2:
                                    data_blocks.append(values)
                                elif values.ndim == 1:
                                    data_blocks.append(values.reshape(-1, 1))
                    
                    if data_blocks:
                        combined_data = np.vstack(data_blocks)
                        return pd.DataFrame(combined_data, columns=columns)
                    else:
                        raise ValueError("No data blocks found")
                else:
                    raise ValueError("No 'stage' group found in HDF5 file")
        
        except Exception as e:
            logger.error(f"Error converting H5 to DataFrame: {e}")
            raise
    
    def get_pre_signed_download_url(self, expires_in: int = 3600) -> str:
        """Generate pre-signed URL for direct S3 download"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': self.h5_key
                },
                ExpiresIn=expires_in
            )
            return url
        except ClientError as e:
            logger.error(f"Error generating pre-signed URL: {e}")
            raise
    
    def download_h5_as_csv_streaming(self) -> Generator[str, None, None]:
        """
        Stream H5 data as CSV to avoid memory issues
        Yields CSV chunks for streaming response
        """
        try:
            # Convert to Parquet first (more efficient)
            parquet_path = self.convert_h5_to_parquet_streaming()
            
            # Read Parquet in chunks and convert to CSV
            df = pd.read_parquet(parquet_path)
            
            # Yield CSV header
            yield df.columns.to_csv(index=False)
            
            # Yield data in chunks
            chunk_size = 10000  # 10K rows per chunk
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                yield chunk.to_csv(index=False, header=False)
            
            # Clean up
            os.unlink(parquet_path)
            
        except Exception as e:
            logger.error(f"Error streaming H5 as CSV: {e}")
            raise
    
    def get_sample_data(self, n_rows: int = 1000) -> pd.DataFrame:
        """Get a sample of the H5 data for preview"""
        try:
            # Stream first chunk only
            temp_h5_path = tempfile.mktemp(suffix='.h5')
            
            with open(temp_h5_path, 'wb') as temp_file:
                chunk_count = 0
                for chunk in self.stream_h5_data(chunk_size=10*1024*1024):  # 10MB chunks
                    temp_file.write(chunk)
                    chunk_count += 1
                    if chunk_count >= 3:  # Only first 30MB for sample
                        break
            
            # Convert sample
            df = self._convert_h5_to_dataframe_optimized(temp_h5_path)
            
            # Return sample
            sample_df = df.head(n_rows)
            
            # Clean up
            os.unlink(temp_h5_path)
            
            return sample_df
            
        except Exception as e:
            logger.error(f"Error getting sample data: {e}")
            raise
