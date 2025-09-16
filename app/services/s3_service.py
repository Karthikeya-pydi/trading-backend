import boto3
import pandas as pd
import io
from typing import Dict, List, Optional, Any
from datetime import datetime
from loguru import logger
from botocore.exceptions import ClientError, NoCredentialsError
import os
from app.core.config import settings


class S3Service:
    """
    Service to handle S3 operations for fetching nifty_indices and bhavcopies
    """
    
    def __init__(self):
        self.bucket_name = settings.s3_bucket_name
        self.region = settings.aws_region
        self.nifty_folder = "nifty_indices"
        self.bhavcopy_folder = "bhavcopies"
        self.adjusted_eq_folder = "adjusted-eq-data"
        
        # Initialize S3 client
        try:
            # Use credentials from settings if available, otherwise use environment variables
            if settings.aws_access_key_id and settings.aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=settings.aws_access_key_id,
                    aws_secret_access_key=settings.aws_secret_access_key,
                    region_name=self.region
                )
                logger.info("S3 client initialized successfully with provided credentials")
            else:
                # Use environment variables or IAM roles
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.region
                )
                logger.info("S3 client initialized successfully with environment/IAM credentials")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please check your environment variables or settings.")
            raise
        except Exception as e:
            logger.error(f"Error initializing S3 client: {e}")
            raise
    
    def _list_s3_objects(self, folder: str, file_extension: str = ".csv") -> List[Dict[str, Any]]:
        """
        List all objects in a specific S3 folder
        
        Args:
            folder: S3 folder path
            file_extension: File extension to filter by
            
        Returns:
            List of object metadata dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f"{folder}/"
            )
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith(file_extension):
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'filename': obj['Key'].split('/')[-1]
                        })
            
            # Sort by last modified date (newest first)
            objects.sort(key=lambda x: x['last_modified'], reverse=True)
            return objects
            
        except ClientError as e:
            logger.error(f"Error listing S3 objects in {folder}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing S3 objects: {e}")
            return []
    
    def _get_s3_object_content(self, key: str) -> Optional[str]:
        """
        Get content of an S3 object as string
        
        Args:
            key: S3 object key
            
        Returns:
            Object content as string or None if error
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return response['Body'].read().decode('utf-8')
            
        except ClientError as e:
            logger.error(f"Error getting S3 object {key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting S3 object: {e}")
            return None
    
    def get_latest_bhavcopy_file(self) -> Optional[Dict[str, Any]]:
        """
        Get the most recent bhavcopy file from S3
        
        Returns:
            Dictionary containing file metadata or None if no files found
        """
        try:
            objects = self._list_s3_objects(self.bhavcopy_folder)
            if objects:
                obj = objects[0]  # Already sorted by date, newest first
                # Return with consistent field names
                return {
                    'key': obj['key'],
                    's3_key': obj['key'],  # Add s3_key for compatibility
                    'size': obj['size'],
                    'last_modified': obj['last_modified'],
                    'filename': obj['filename']
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest bhavcopy file: {e}")
            return None
    
    def get_latest_nifty_file(self, index_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent nifty file for a specific index from S3
        
        Args:
            index_name: Name of the nifty index
            
        Returns:
            Dictionary containing file metadata or None if no files found
        """
        try:
            objects = self._list_s3_objects(self.nifty_folder)
            
            # Create safe filename for comparison
            safe_filename = index_name.replace(' ', '_').replace('&', 'and').replace('/', '_')
            target_filename = f"{safe_filename}.csv"
            
            for obj in objects:
                if obj['filename'] == target_filename:
                    # Return with consistent field names
                    return {
                        'key': obj['key'],
                        's3_key': obj['key'],  # Add s3_key for compatibility
                        'size': obj['size'],
                        'last_modified': obj['last_modified'],
                        'filename': obj['filename']
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest nifty file for {index_name}: {e}")
            return None
    
    def get_bhavcopy_data(self, file_key: str) -> Optional[pd.DataFrame]:
        """
        Get bhavcopy data from S3 as DataFrame
        
        Args:
            file_key: S3 object key for the bhavcopy file
            
        Returns:
            DataFrame containing bhavcopy data or None if error
        """
        try:
            content = self._get_s3_object_content(file_key)
            if content is None:
                return None
            
            # Read CSV from string content
            df = pd.read_csv(io.StringIO(content))
            logger.info(f"Successfully loaded bhavcopy data from S3: {file_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading bhavcopy data from S3: {e}")
            return None
    
    def get_nifty_data(self, file_key: str) -> Optional[pd.DataFrame]:
        """
        Get nifty index data from S3 as DataFrame
        
        Args:
            file_key: S3 object key for the nifty file
            
        Returns:
            DataFrame containing nifty data or None if error
        """
        try:
            content = self._get_s3_object_content(file_key)
            if content is None:
                return None
            
            # Read CSV from string content
            df = pd.read_csv(io.StringIO(content))
            logger.info(f"Successfully loaded nifty data from S3: {file_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading nifty data from S3: {e}")
            return None
    
    def get_available_nifty_indices(self) -> List[Dict[str, Any]]:
        """
        Get list of available nifty indices from S3
        
        Returns:
            List of dictionaries containing index metadata
        """
        try:
            objects = self._list_s3_objects(self.nifty_folder)
            indices = []
            
            for obj in objects:
                # Extract index name from filename
                filename = obj['filename']
                if filename.endswith('.csv'):
                    index_name = filename[:-4].replace('_', ' ')
                    
                    indices.append({
                        'index_name': index_name,
                        'filename': filename,
                        's3_key': obj['key'],
                        'size_bytes': obj['size'],
                        'last_modified': obj['last_modified'].isoformat(),
                        'source': 'S3'
                    })
            
            # Sort by index name
            indices.sort(key=lambda x: x['index_name'])
            return indices
            
        except Exception as e:
            logger.error(f"Error getting available nifty indices: {e}")
            return []
    
    def get_latest_adjusted_eq_file(self) -> Optional[Dict[str, Any]]:
        """
        Get the most recent adjusted-eq-data file from S3
        
        Returns:
            Dictionary containing file metadata or None if no files found
        """
        try:
            objects = self._list_s3_objects(self.adjusted_eq_folder)
            if objects:
                obj = objects[0]  # Already sorted by date, newest first
                return {
                    'key': obj['key'],
                    's3_key': obj['key'],
                    'size': obj['size'],
                    'last_modified': obj['last_modified'],
                    'filename': obj['filename']
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest adjusted-eq-data file: {e}")
            return None
    
    def get_adjusted_eq_data(self, file_key: str) -> Optional[pd.DataFrame]:
        """
        Get adjusted-eq-data from S3 as DataFrame
        
        Args:
            file_key: S3 object key for the adjusted-eq-data file
            
        Returns:
            DataFrame containing adjusted-eq-data or None if error
        """
        try:
            content = self._get_s3_object_content(file_key)
            if content is None:
                return None
            
            # Read CSV from string content
            df = pd.read_csv(io.StringIO(content))
            logger.info(f"Successfully loaded adjusted-eq-data from S3: {file_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading adjusted-eq-data from S3: {e}")
            return None

    def get_bhavcopy_summary(self) -> Dict[str, Any]:
        """
        Get summary of available bhavcopy files from S3
        
        Returns:
            Dictionary containing bhavcopy summary
        """
        try:
            objects = self._list_s3_objects(self.bhavcopy_folder)
            
            summary = []
            for obj in objects:
                summary.append({
                    'filename': obj['filename'],
                    's3_key': obj['key'],
                    'size_mb': round(obj['size'] / (1024 * 1024), 2),
                    'last_modified': obj['last_modified'].isoformat(),
                    'source': 'S3'
                })
            
            return {
                'status': 'success',
                'files': summary,
                'total_files': len(summary),
                'source': 'S3',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting bhavcopy summary: {e}")
            return {
                'status': 'error',
                'message': f'Failed to fetch bhavcopy summary: {str(e)}'
            }
    
    def get_adjusted_eq_summary(self) -> Dict[str, Any]:
        """
        Get summary of available adjusted-eq-data files from S3
        
        Returns:
            Dictionary containing adjusted-eq-data summary
        """
        try:
            objects = self._list_s3_objects(self.adjusted_eq_folder)
            
            summary = []
            for obj in objects:
                summary.append({
                    'filename': obj['filename'],
                    's3_key': obj['key'],
                    'size_mb': round(obj['size'] / (1024 * 1024), 2),
                    'last_modified': obj['last_modified'].isoformat(),
                    'source': 'S3'
                })
            
            return {
                'status': 'success',
                'files': summary,
                'total_files': len(summary),
                'source': 'S3',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting adjusted-eq-data summary: {e}")
            return {
                'status': 'error',
                'message': f'Failed to fetch adjusted-eq-data summary: {str(e)}'
            }
    
    def test_s3_connection(self) -> Dict[str, Any]:
        """
        Test S3 connection and bucket access
        
        Returns:
            Dictionary containing test results
        """
        try:
            # Try to list objects in the bucket
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
            
            return {
                'status': 'success',
                'message': 'S3 connection successful',
                'bucket_name': self.bucket_name,
                'region': self.region,
                'timestamp': datetime.now().isoformat()
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return {
                    'status': 'error',
                    'message': f'Bucket {self.bucket_name} does not exist',
                    'error_code': error_code
                }
            elif error_code == 'AccessDenied':
                return {
                    'status': 'error',
                    'message': f'Access denied to bucket {self.bucket_name}',
                    'error_code': error_code
                }
            else:
                return {
                    'status': 'error',
                    'message': f'S3 error: {str(e)}',
                    'error_code': error_code
                }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Unexpected error testing S3 connection: {str(e)}'
            }
