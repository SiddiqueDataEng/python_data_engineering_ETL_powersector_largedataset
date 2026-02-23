"""
Power Sector Data ETL Pipeline
Processes bills (TSV) and readings (CSV) from hierarchical directory structure
into consolidated Parquet files with CDC support for incremental loading.
"""

import os
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging

import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PowerSectorETL:
    """ETL Pipeline for Power Sector Data with CDC support"""
    
    def __init__(
        self,
        source_dir: str = r"\\counter2\E\iesco\power_sector_data",
        target_dir: str = "power_sector_data_silver_layer",
        state_file: str = "etl_state.json"
    ):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.state_file = Path(target_dir) / state_file
        
        # Create target directory
        self.target_dir.mkdir(parents=True, exist_ok=True)
        
        # Load processing state for CDC
        self.state = self._load_state()
        
    def _load_state(self) -> Dict:
        """Load ETL state for CDC tracking"""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        return {
            'bills': {},
            'readings': {},
            'last_full_load': None,
            'last_incremental_load': None
        }
    
    def _save_state(self):
        """Save ETL state"""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, indent=2, fp=f)
        logger.info(f"State saved to {self.state_file}")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """Generate hash for file to detect changes"""
        stat = file_path.stat()
        # Use size and modification time for quick comparison
        return hashlib.md5(
            f"{stat.st_size}_{stat.st_mtime}".encode()
        ).hexdigest()
    
    def _discover_files(
        self,
        data_type: str,
        incremental: bool = False
    ) -> List[Tuple[Path, str, str]]:
        """
        Discover data files in directory structure
        Returns: List of (file_path, division, subdivision)
        """
        pattern = "bills.tsv" if data_type == "bills" else "readings.csv"
        discovered_files = []
        
        # Walk through division/subdivision structure
        for division_dir in self.source_dir.iterdir():
            if not division_dir.is_dir() or division_dir.name.startswith('.'):
                continue
                
            division = division_dir.name
            
            for subdivision_dir in division_dir.iterdir():
                if not subdivision_dir.is_dir():
                    continue
                    
                subdivision = subdivision_dir.name
                data_dir = subdivision_dir / data_type
                
                if not data_dir.exists():
                    continue
                
                file_path = data_dir / pattern
                if not file_path.exists():
                    continue
                
                # CDC: Check if file needs processing
                file_key = str(file_path.relative_to(self.source_dir))
                file_hash = self._get_file_hash(file_path)
                
                if incremental:
                    # Skip if already processed and unchanged
                    if file_key in self.state[data_type]:
                        if self.state[data_type][file_key] == file_hash:
                            logger.debug(f"Skipping unchanged file: {file_key}")
                            continue
                
                discovered_files.append((file_path, division, subdivision))
                # Update state
                self.state[data_type][file_key] = file_hash
        
        logger.info(f"Discovered {len(discovered_files)} {data_type} files to process")
        return discovered_files
    
    def _clean_bills_data(self, df: dd.DataFrame) -> dd.DataFrame:
        """Clean and validate bills data"""
        logger.info("Cleaning bills data...")
        
        # Convert data types
        df['billing_month'] = dd.to_datetime(df['billing_month'], format='%Y-%m', errors='coerce')
        df['consumption_kwh'] = dd.to_numeric(df['consumption_kwh'], errors='coerce')
        df['energy_charges'] = dd.to_numeric(df['energy_charges'], errors='coerce')
        df['meter_rent'] = dd.to_numeric(df['meter_rent'], errors='coerce')
        df['gst'] = dd.to_numeric(df['gst'], errors='coerce')
        df['total_amount'] = dd.to_numeric(df['total_amount'], errors='coerce')
        
        # Remove duplicates based on bill_id
        df = df.drop_duplicates(subset=['bill_id'])
        
        # Filter out invalid records
        df = df[df['consumption_kwh'].notnull()]
        df = df[df['total_amount'] > 0]
        
        return df
    
    def _clean_readings_data(self, df: dd.DataFrame) -> dd.DataFrame:
        """Clean and validate readings data"""
        logger.info("Cleaning readings data...")
        
        # Convert data types
        df['timestamp'] = dd.to_datetime(df['timestamp'], errors='coerce')
        df['reading'] = dd.to_numeric(df['reading'], errors='coerce')
        df['consumption_kwh'] = dd.to_numeric(df['consumption_kwh'], errors='coerce')
        df['voltage'] = dd.to_numeric(df['voltage'], errors='coerce')
        df['current_amp'] = dd.to_numeric(df['current_amp'], errors='coerce')
        
        # Remove duplicates based on timestamp and meter_id
        df = df.drop_duplicates(subset=['timestamp', 'meter_id'])
        
        # Filter out invalid records
        df = df[df['timestamp'].notnull()]
        df = df[df['reading'].notnull()]
        
        # Data quality filters
        df = df[df['voltage'].between(180, 260)]  # Valid voltage range
        df = df[df['consumption_kwh'] >= 0]
        
        return df

    def process_bills(
        self,
        incremental: bool = False,
        compression: str = 'snappy'
    ) -> Path:
        """
        Process all bills files into consolidated Parquet
        
        Args:
            incremental: If True, only process new/changed files
            compression: Parquet compression (snappy, gzip, brotli)
        """
        logger.info(f"Starting bills processing (incremental={incremental})...")
        
        # Discover files
        files_to_process = self._discover_files('bills', incremental)
        
        if not files_to_process:
            logger.info("No bills files to process")
            return None
        
        # Read all files with metadata
        dfs = []
        for file_path, division, subdivision in files_to_process:
            try:
                logger.info(f"Reading: {file_path}")
                df = dd.read_csv(
                    file_path,
                    sep='\t',
                    dtype={
                        'bill_id': 'object',
                        'meter_id': 'object',
                        'billing_month': 'object',
                        'status': 'object'
                    },
                    blocksize='64MB'  # Optimize for large files
                )
                
                # Add metadata columns
                df['division'] = division
                df['subdivision'] = subdivision
                df['source_file'] = str(file_path.relative_to(self.source_dir))
                df['etl_timestamp'] = datetime.now()
                
                dfs.append(df)
                
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue
        
        if not dfs:
            logger.warning("No valid bills data to process")
            return None
        
        # Concatenate all dataframes
        logger.info("Concatenating bills data...")
        combined_df = dd.concat(dfs, ignore_index=True)
        
        # Clean data
        combined_df = self._clean_bills_data(combined_df)
        
        # Output path
        output_path = self.target_dir / "bills.parquet"
        
        # Handle incremental vs full load
        if incremental and output_path.exists():
            logger.info("Merging with existing bills data...")
            existing_df = dd.read_parquet(output_path)
            combined_df = dd.concat([existing_df, combined_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['bill_id'], keep='last')
        
        # Write to Parquet with compression
        logger.info(f"Writing bills to {output_path}...")
        with ProgressBar():
            combined_df.to_parquet(
                output_path,
                engine='pyarrow',
                compression=compression,
                write_index=False,
                overwrite=True
            )
        
        # Update state
        self.state['last_incremental_load' if incremental else 'last_full_load'] = datetime.now().isoformat()
        self._save_state()
        
        logger.info(f"✓ Bills processing complete: {output_path}")
        return output_path
    
    def process_readings(
        self,
        incremental: bool = False,
        compression: str = 'snappy'
    ) -> Path:
        """
        Process all readings files into consolidated Parquet
        
        Args:
            incremental: If True, only process new/changed files
            compression: Parquet compression (snappy, gzip, brotli)
        """
        logger.info(f"Starting readings processing (incremental={incremental})...")
        
        # Discover files
        files_to_process = self._discover_files('readings', incremental)
        
        if not files_to_process:
            logger.info("No readings files to process")
            return None
        
        # Read all files with metadata
        dfs = []
        for file_path, division, subdivision in files_to_process:
            try:
                logger.info(f"Reading: {file_path}")
                df = dd.read_csv(
                    file_path,
                    dtype={
                        'meter_id': 'object',
                        'quality_flag': 'object'
                    },
                    blocksize='128MB',  # Larger blocksize for readings
                    assume_missing=True
                )
                
                # Add metadata columns
                df['division'] = division
                df['subdivision'] = subdivision
                df['source_file'] = str(file_path.relative_to(self.source_dir))
                df['etl_timestamp'] = datetime.now()
                
                dfs.append(df)
                
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue
        
        if not dfs:
            logger.warning("No valid readings data to process")
            return None
        
        # Concatenate all dataframes
        logger.info("Concatenating readings data...")
        combined_df = dd.concat(dfs, ignore_index=True)
        
        # Clean data
        combined_df = self._clean_readings_data(combined_df)
        
        # Output path
        output_path = self.target_dir / "readings.parquet"
        
        # Handle incremental vs full load
        if incremental and output_path.exists():
            logger.info("Merging with existing readings data...")
            existing_df = dd.read_parquet(output_path)
            combined_df = dd.concat([existing_df, combined_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=['timestamp', 'meter_id'],
                keep='last'
            )
        
        # Write to Parquet with compression and partitioning
        logger.info(f"Writing readings to {output_path}...")
        with ProgressBar():
            combined_df.to_parquet(
                output_path,
                engine='pyarrow',
                compression=compression,
                write_index=False,
                overwrite=True
            )
        
        # Update state
        self.state['last_incremental_load' if incremental else 'last_full_load'] = datetime.now().isoformat()
        self._save_state()
        
        logger.info(f"✓ Readings processing complete: {output_path}")
        return output_path
    
    def run_full_load(self, compression: str = 'snappy'):
        """Run full ETL load (process all files)"""
        logger.info("=" * 60)
        logger.info("STARTING FULL LOAD")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Process both data types
        bills_path = self.process_bills(incremental=False, compression=compression)
        readings_path = self.process_readings(incremental=False, compression=compression)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"FULL LOAD COMPLETE in {duration:.2f} seconds")
        logger.info(f"Bills: {bills_path}")
        logger.info(f"Readings: {readings_path}")
        logger.info("=" * 60)
        
        return bills_path, readings_path
    
    def run_incremental_load(self, compression: str = 'snappy'):
        """Run incremental ETL load (process only new/changed files)"""
        logger.info("=" * 60)
        logger.info("STARTING INCREMENTAL LOAD")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Process both data types
        bills_path = self.process_bills(incremental=True, compression=compression)
        readings_path = self.process_readings(incremental=True, compression=compression)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"INCREMENTAL LOAD COMPLETE in {duration:.2f} seconds")
        logger.info(f"Bills: {bills_path}")
        logger.info(f"Readings: {readings_path}")
        logger.info("=" * 60)
        
        return bills_path, readings_path
    
    def get_statistics(self):
        """Get statistics about processed data"""
        stats = {}
        
        bills_path = self.target_dir / "bills.parquet"
        readings_path = self.target_dir / "readings.parquet"
        
        if bills_path.exists():
            bills_df = dd.read_parquet(bills_path)
            stats['bills'] = {
                'total_records': len(bills_df),
                'file_size_mb': bills_path.stat().st_size / (1024 * 1024),
                'divisions': bills_df['division'].nunique().compute(),
                'subdivisions': bills_df['subdivision'].nunique().compute(),
                'date_range': (
                    bills_df['billing_month'].min().compute(),
                    bills_df['billing_month'].max().compute()
                )
            }
        
        if readings_path.exists():
            readings_df = dd.read_parquet(readings_path)
            stats['readings'] = {
                'total_records': len(readings_df),
                'file_size_mb': readings_path.stat().st_size / (1024 * 1024),
                'divisions': readings_df['division'].nunique().compute(),
                'subdivisions': readings_df['subdivision'].nunique().compute(),
                'date_range': (
                    readings_df['timestamp'].min().compute(),
                    readings_df['timestamp'].max().compute()
                )
            }
        
        return stats


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Power Sector ETL Pipeline')
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental', 'stats'],
        default='full',
        help='ETL mode: full load, incremental load, or show statistics'
    )
    parser.add_argument(
        '--compression',
        choices=['snappy', 'gzip', 'brotli'],
        default='snappy',
        help='Parquet compression algorithm'
    )
    parser.add_argument(
        '--source',
        default=r'\\counter2\E\iesco\power_sector_data',
        help='Source data directory'
    )
    parser.add_argument(
        '--target',
        default='power_sector_data_silver_layer',
        help='Target directory for processed data'
    )
    
    args = parser.parse_args()
    
    # Initialize ETL pipeline
    etl = PowerSectorETL(
        source_dir=args.source,
        target_dir=args.target
    )
    
    # Execute based on mode
    if args.mode == 'full':
        etl.run_full_load(compression=args.compression)
    elif args.mode == 'incremental':
        etl.run_incremental_load(compression=args.compression)
    elif args.mode == 'stats':
        stats = etl.get_statistics()
        print("\n" + "=" * 60)
        print("DATA STATISTICS")
        print("=" * 60)
        for data_type, info in stats.items():
            print(f"\n{data_type.upper()}:")
            for key, value in info.items():
                print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
