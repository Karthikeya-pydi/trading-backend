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

from returnsCalculation import ReturnsCalculator
from app.services.s3_service import S3Service

class SchedulerService:
    """
    Service for managing scheduled tasks in the trading platform.
    Handles automatic execution of returns calculations and other periodic tasks.
    """
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
        self.s3_service = S3Service()
        
    async def start(self):
        """Start the scheduler service"""
        if self.is_running:
            logger.warning("Scheduler service is already running")
            return
            
        try:
            # Schedule returns calculation to run every day at 6:00 PM (18:00)
            self.scheduler.add_job(
                func=self.run_returns_calculation,
                trigger=CronTrigger(hour=18, minute=0),  # 6:00 PM daily
                id='returns_calculation',
                name='Daily Returns Calculation',
                replace_existing=True,
                max_instances=1  # Prevent overlapping executions
            )
            
            # Start the scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info("Scheduler service started successfully")
            logger.info("Returns calculation scheduled for 6:00 PM daily")
            
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
        Execute the returns calculation for the current date.
        This method runs automatically every evening at 6:00 PM.
        """
        try:
            logger.info("Starting scheduled returns calculation...")
            
            # Get current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # Get the latest adjusted-eq-data file from S3
            logger.info("Fetching latest adjusted-eq-data file from S3...")
            s3_file_info = self.s3_service.get_latest_adjusted_eq_file()
            
            if not s3_file_info:
                logger.error("No adjusted-eq-data files found in S3")
                return
            
            logger.info(f"Found latest file in S3: {s3_file_info['filename']}")
            logger.info(f"File last modified: {s3_file_info['last_modified']}")
            
            # Download the file content from S3
            logger.info("Downloading file content from S3...")
            df = self.s3_service.get_adjusted_eq_data(s3_file_info['s3_key'])
            
            if df is None:
                logger.error("Failed to download file content from S3")
                return
            
            # Create calculator instance and set data directly
            calculator = ReturnsCalculator("")  # Empty path since we'll set data directly
            calculator.data = df  # Set the DataFrame directly
            
            # Run the analysis with scoring
            output_file = f"stock_returns_{current_date}.csv"
            calculator.run_analysis_with_scoring(output_file)
            
            logger.info(f"Returns calculation completed successfully. Output saved to: {output_file}")
            
        except Exception as e:
            logger.error(f"Error during scheduled returns calculation: {e}")
            # Don't re-raise the exception to prevent scheduler from stopping
    
    async def run_returns_calculation_manual(self, target_date: str = None):
        """
        Manually trigger returns calculation for a specific date.
        
        Args:
            target_date (str): Date in YYYY-MM-DD format. If None, uses current date.
        """
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Manually triggering returns calculation for {target_date}")
        
        try:
            # Get the latest adjusted-eq-data file from S3
            logger.info("Fetching latest adjusted-eq-data file from S3...")
            s3_file_info = self.s3_service.get_latest_adjusted_eq_file()
            
            if not s3_file_info:
                logger.error("No adjusted-eq-data files found in S3")
                return
            
            logger.info(f"Found latest file in S3: {s3_file_info['filename']}")
            logger.info(f"File last modified: {s3_file_info['last_modified']}")
            
            # Download the file content from S3
            logger.info("Downloading file content from S3...")
            df = self.s3_service.get_adjusted_eq_data(s3_file_info['s3_key'])
            
            if df is None:
                logger.error("Failed to download file content from S3")
                return
            
            # Create calculator instance and set data directly
            calculator = ReturnsCalculator("")  # Empty path since we'll set data directly
            calculator.data = df  # Set the DataFrame directly
            
            # Run the analysis with scoring
            output_file = f"stock_returns_{target_date}.csv"
            calculator.run_analysis_with_scoring(output_file)
            
            logger.info(f"Manual returns calculation completed successfully. Output saved to: {output_file}")
            
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
