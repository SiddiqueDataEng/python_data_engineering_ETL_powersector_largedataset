"""
ETL Pipeline - Small Batch Mode for Extremely Memory-Constrained Systems
Process only a few subdivisions at a time
"""

from etl_pipeline import PowerSectorETL
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    etl = PowerSectorETL()
    
    # Get all subdivisions
    source_dir = Path(etl.source_dir)
    subdivisions = []
    
    for division_dir in source_dir.iterdir():
        if not division_dir.is_dir():
            continue
        for subdiv_dir in division_dir.iterdir():
            if not subdiv_dir.is_dir():
                continue
            subdivisions.append((division_dir.name, subdiv_dir.name))
    
    logger.info(f"Found {len(subdivisions)} subdivisions")
    logger.info("Processing in small batches of 5 subdivisions...")
    
    # Process in batches of 5
    batch_size = 5
    for batch_num, i in enumerate(range(0, len(subdivisions), batch_size), 1):
        batch = subdivisions[i:i+batch_size]
        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH {batch_num}: Processing {len(batch)} subdivisions")
        logger.info(f"{'='*60}")
        
        for div, subdiv in batch:
            logger.info(f"  - {div}/{subdiv}")
        
        # Process this batch
        # You would need to modify ETL to process specific subdivisions
        # For now, just showing the concept
        
        input(f"\nPress Enter to process batch {batch_num} (or Ctrl+C to stop)...")

if __name__ == "__main__":
    main()
