"""
Scheduler for running returnsCalsProd.py at 9:35 PM daily
Can be used with cron on Linux or Task Scheduler on Windows
"""

import subprocess
import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_returns_calculation():
    """Run the returns calculation script"""
    try:
        # Get the directory of this script
        script_dir = Path(__file__).parent
        script_path = script_dir / "returnsCalsProd.py"
        
        # Change to the script directory
        os.chdir(script_dir)
        
        logger.info("="*60)
        logger.info(f"Starting returns calculation at {datetime.now()}")
        logger.info(f"Script path: {script_path}")
        logger.info("="*60)
        
        # Run the returns calculation script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        logger.info("STDOUT:")
        logger.info(result.stdout)
        
        if result.stderr:
            logger.error("STDERR:")
            logger.error(result.stderr)
        
        if result.returncode == 0:
            logger.info("="*60)
            logger.info(f"Returns calculation completed successfully at {datetime.now()}")
            logger.info("="*60)
        else:
            logger.error("="*60)
            logger.error(f"Returns calculation failed with exit code {result.returncode}")
            logger.error("="*60)
            
        return result.returncode
        
    except subprocess.TimeoutExpired:
        logger.error("Script execution timed out after 1 hour")
        return -1
    except Exception as e:
        logger.error(f"Error running returns calculation: {str(e)}")
        return -1

if __name__ == "__main__":
    logger.info("Scheduler started")
    exit_code = run_returns_calculation()
    sys.exit(exit_code)
