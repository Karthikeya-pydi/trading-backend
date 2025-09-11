import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
import boto3
from botocore.exceptions import ClientError

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from returnsCalsProd import ProductionReturnsCalculator

class SchedulerService:
    """
    Service for managing scheduled tasks in the trading platform.
    Handles automatic execution of returns calculations and other periodic tasks.
    """
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
        # Configuration for production returns calculation
        self.input_bucket = "parquet-eq-data"
        self.output_bucket = "trading-platform-csvs"
        self.h5_folder = "nse_data/"  # Folder to search for latest H5 file
        self.output_prefix = "adjusted-eq-data"
    
    def get_latest_h5_file(self, input_credentials: dict) -> str:
        """
        Find the latest H5 file in the nse_data/ folder
        
        Args:
            input_credentials (dict): AWS credentials for S3 access
            
        Returns:
            str: S3 key of the latest H5 file
            
        Raises:
            Exception: If no H5 files found or S3 access fails
        """
        try:
            # Create S3 client with input credentials
            s3_client = boto3.client(
                's3',
                aws_access_key_id=input_credentials['access_key'],
                aws_secret_access_key=input_credentials['secret_key']
            )
            
            # List objects in the nse_data/ folder
            response = s3_client.list_objects_v2(
                Bucket=self.input_bucket,
                Prefix=self.h5_folder
            )
            
            if 'Contents' not in response:
                raise Exception(f"No files found in s3://{self.input_bucket}/{self.h5_folder}")
            
            # Filter for H5 files and sort by last modified date
            h5_files = [
                obj for obj in response['Contents'] 
                if obj['Key'].endswith('.h5')
            ]
            
            if not h5_files:
                raise Exception(f"No H5 files found in s3://{self.input_bucket}/{self.h5_folder}")
            
            # Sort by last modified date (newest first)
            latest_file = max(h5_files, key=lambda x: x['LastModified'])
            latest_key = latest_file['Key']
            
            logger.info(f"Found latest H5 file: {latest_key}")
            logger.info(f"Last modified: {latest_file['LastModified']}")
            
            return latest_key
            
        except ClientError as e:
            logger.error(f"AWS S3 error while finding latest H5 file: {e}")
            raise Exception(f"Failed to access S3 bucket: {e}")
        except Exception as e:
            logger.error(f"Error finding latest H5 file: {e}")
            raise
        
    async def start(self):
        """Start the scheduler service"""
        if self.is_running:
            logger.warning("Scheduler service is already running")
            return
            
        try:
            # Schedule returns calculation to run every day at 9:30 AM
            self.scheduler.add_job(
                func=self.run_returns_calculation,
                trigger=CronTrigger(hour=9, minute=30),  # 9:30 AM daily
                id='returns_calculation',
                name='Daily Returns Calculation',
                replace_existing=True,
                max_instances=1  # Prevent overlapping executions
            )
            
            # Start the scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info("Scheduler service started successfully")
            logger.info("Returns calculation scheduled for 9:30 AM daily")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler service: {e}")
            raise
    
    async def stop(self):
        """Stop the scheduler service"""
        if not self.is_running:
            return
            
        try:
            self.scheduler.shutdown(wait=True)
            self.is_running = False
            logger.info("Scheduler service stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler service: {e}")
    
    async def run_returns_calculation(self):
        """
        Execute the production returns calculation for the current date.
        This method runs automatically every morning at 9:30 AM.
        """
        try:
            logger.info("Starting scheduled production returns calculation...")
            
            # Get current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # AWS Credentials (from environment variables)
            input_credentials = {
                'access_key': os.getenv('INPUT_AWS_ACCESS_KEY_ID'),
                'secret_key': os.getenv('INPUT_AWS_SECRET_ACCESS_KEY')
            }
            
            output_credentials = {
                'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
                'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY')
            }
            
            # Find the latest H5 file in nse_data/ folder
            logger.info("Searching for latest H5 file in nse_data/ folder...")
            latest_h5_key = self.get_latest_h5_file(input_credentials)
            
            # Create production calculator instance with latest H5 file
            calculator = ProductionReturnsCalculator(
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                h5_key=latest_h5_key,  # Use the latest H5 file
                output_prefix=self.output_prefix,
                input_credentials=input_credentials,
                output_credentials=output_credentials
            )
            
            # Run complete production flow
            result_s3_key = calculator.run_complete_flow(include_scoring=True)
            
            logger.info(f"Production returns calculation completed successfully!")
            logger.info(f"Used H5 file: s3://{self.input_bucket}/{latest_h5_key}")
            logger.info(f"Results uploaded to: s3://{self.output_bucket}/{result_s3_key}")
            
        except Exception as e:
            logger.error(f"Error during scheduled returns calculation: {e}")
            # Don't re-raise the exception to prevent scheduler from stopping
    
    async def run_returns_calculation_manual(self, target_date: str = None):
        """
        Manually trigger production returns calculation for a specific date.
        
        Args:
            target_date (str): Date in YYYY-MM-DD format. If None, uses current date.
        """
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Manually triggering production returns calculation for {target_date}")
        
        try:
            # AWS Credentials (from environment variables)
            input_credentials = {
                'access_key': os.getenv('INPUT_AWS_ACCESS_KEY_ID'),
                'secret_key': os.getenv('INPUT_AWS_SECRET_ACCESS_KEY')
            }
            
            output_credentials = {
                'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
                'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY')
            }
            
            # Find the latest H5 file in nse_data/ folder
            logger.info("Searching for latest H5 file in nse_data/ folder...")
            latest_h5_key = self.get_latest_h5_file(input_credentials)
            
            # Create production calculator instance with latest H5 file
            calculator = ProductionReturnsCalculator(
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                h5_key=latest_h5_key,  # Use the latest H5 file
                output_prefix=self.output_prefix,
                input_credentials=input_credentials,
                output_credentials=output_credentials
            )
            
            # Run complete production flow
            result_s3_key = calculator.run_complete_flow(target_date=target_date, include_scoring=True)
            
            logger.info(f"Manual production returns calculation completed successfully!")
            logger.info(f"Used H5 file: s3://{self.input_bucket}/{latest_h5_key}")
            logger.info(f"Results uploaded to: s3://{self.output_bucket}/{result_s3_key}")
            
        except Exception as e:
            logger.error(f"Error during manual returns calculation: {e}")
            raise
    
    def get_scheduled_jobs(self):
        """Get information about scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        return jobs
    
    def is_job_running(self, job_id: str) -> bool:
        """Check if a specific job is currently running"""
        job = self.scheduler.get_job(job_id)
        return job is not None and job.next_run_time is not None
    
    async def test_latest_h5_detection(self):
        """
        Test method to verify latest H5 file detection works
        Useful for debugging and testing S3 connectivity
        """
        try:
            input_credentials = {
                'access_key': os.getenv('INPUT_AWS_ACCESS_KEY_ID'),
                'secret_key': os.getenv('INPUT_AWS_SECRET_ACCESS_KEY')
            }
            
            if not input_credentials['access_key'] or not input_credentials['secret_key']:
                logger.error("INPUT_AWS_ACCESS_KEY_ID or INPUT_AWS_SECRET_ACCESS_KEY not set")
                return False
            
            latest_h5_key = self.get_latest_h5_file(input_credentials)
            logger.info(f"✅ Latest H5 file detection test successful: {latest_h5_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Latest H5 file detection test failed: {e}")
            return False

# Global scheduler service instance
scheduler_service = SchedulerService()
