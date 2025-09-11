import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

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
        self.h5_key = "nse_data/Our_Nseadjprice.h5"
        self.output_prefix = "adjusted-eq-data"
        
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
            
            # Create production calculator instance
            calculator = ProductionReturnsCalculator(
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                h5_key=self.h5_key,
                output_prefix=self.output_prefix,
                input_credentials=input_credentials,
                output_credentials=output_credentials
            )
            
            # Run complete production flow
            result_s3_key = calculator.run_complete_flow(include_scoring=True)
            
            logger.info(f"Production returns calculation completed successfully!")
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
            
            # Create production calculator instance
            calculator = ProductionReturnsCalculator(
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                h5_key=self.h5_key,
                output_prefix=self.output_prefix,
                input_credentials=input_credentials,
                output_credentials=output_credentials
            )
            
            # Run complete production flow
            result_s3_key = calculator.run_complete_flow(target_date=target_date, include_scoring=True)
            
            logger.info(f"Manual production returns calculation completed successfully!")
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

# Global scheduler service instance
scheduler_service = SchedulerService()
