"""
ETL Scheduler for automated incremental loads
Monitors source directory and triggers ETL on new data
"""

import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from etl_pipeline import PowerSectorETL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLScheduler:
    """Scheduler for automated ETL execution"""
    
    def __init__(
        self,
        interval_minutes: int = 60,
        source_dir: str = "power_sector_data",
        target_dir: str = "power_sector_data_silver_layer"
    ):
        self.interval = timedelta(minutes=interval_minutes)
        self.etl = PowerSectorETL(source_dir, target_dir)
        self.last_run = None
    
    def should_run(self) -> bool:
        """Check if ETL should run"""
        if self.last_run is None:
            return True
        return datetime.now() - self.last_run >= self.interval
    
    def run_scheduled_etl(self):
        """Execute scheduled ETL"""
        try:
            logger.info("Running scheduled incremental ETL...")
            self.etl.run_incremental_load()
            self.last_run = datetime.now()
            logger.info(f"Next run scheduled at: {self.last_run + self.interval}")
        except Exception as e:
            logger.error(f"ETL execution failed: {e}")
    
    def start(self):
        """Start the scheduler"""
        logger.info(f"ETL Scheduler started (interval: {self.interval})")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                if self.should_run():
                    self.run_scheduled_etl()
                
                # Sleep for 1 minute and check again
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Scheduler')
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Interval in minutes between ETL runs (default: 60)'
    )
    
    args = parser.parse_args()
    
    scheduler = ETLScheduler(interval_minutes=args.interval)
    scheduler.start()


if __name__ == "__main__":
    main()
