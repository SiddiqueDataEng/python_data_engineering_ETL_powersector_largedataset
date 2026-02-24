"""
Incremental ETL Pipeline - Process files one by one with CDC tracking
Processes data from Bronze Layer to Silver Layer with file-level metadata
"""

import os
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IncrementalETL:
    """Incremental ETL Pipeline with file-level CDC tracking"""
    
    def __init__(
        self,
        source_dir: str = "power_sector_data_bronze_layer",
        target_dir: str = "power_sector_data_silver_layer",
        metadata_file: str = "file_metadata.json"
    ):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.metadata_file = Path(target_dir) / metadata_file
        
        # Create target directory
        self.target_dir.mkdir(parents=True, exist_ok=True)
        
        # Load file metadata for CDC
        self.metadata = self._load_metadata()
        
    def _load_metadata(self) -> Dict:
        """Load file processing metadata"""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {
            'bills': {},
            'readings': {},
            'last_run': None,
            'total_files_processed': 0
        }
    
    def _save_metadata(self):
        """Save file processing metadata"""
        self.metadata['last_run'] = datetime.now().isoformat()
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, indent=2, fp=f)
        logger.info(f"Metadata saved to {self.metadata_file}")
    
    def _get_file_metadata(self, file_path: Path) -> Dict:
        """Get file metadata for CDC tracking"""
        stat = file_path.stat()
        return {
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'hash': hashlib.md5(f"{stat.st_size}_{stat.st_mtime}".encode()).hexdigest(),
            'last_processed': datetime.now().isoformat()
        }
    
    def _discover_files(self, data_type: str, incremental: bool = True) -> List[Tuple[Path, str, str]]:
        """
        Discover files to process
        Returns: List of (file_path, division, subdivision)
        """
        pattern = "bills.tsv" if data_type == "bills" else "readings.csv"
        files_to_process = []
        
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
                    logger.debug(f"File not found: {file_path}")
                    continue
                
                file_key = str(file_path.relative_to(self.source_dir))
                
                try:
                    current_metadata = self._get_file_metadata(file_path)
                except Exception as e:
                    logger.warning(f"Could not get metadata for {file_path}: {e}")
                    continue
                
                # Check if file needs processing
                if incremental and file_key in self.metadata[data_type]:
                    stored_hash = self.metadata[data_type][file_key].get('hash')
                    if stored_hash == current_metadata['hash']:
                        logger.debug(f"Skipping unchanged file: {file_key}")
                        continue
                
                files_to_process.append((file_path, division, subdivision))
        
        logger.info(f"Discovered {len(files_to_process)} {data_type} files to process")
        return files_to_process
    
    def _clean_bills_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate bills data"""
        # Convert data types
        df['billing_month'] = pd.to_datetime(df['billing_month'], format='%Y-%m', errors='coerce')
        df['consumption_kwh'] = pd.to_numeric(df['consumption_kwh'], errors='coerce')
        df['energy_charges'] = pd.to_numeric(df['energy_charges'], errors='coerce')
        df['meter_rent'] = pd.to_numeric(df['meter_rent'], errors='coerce')
        df['gst'] = pd.to_numeric(df['gst'], errors='coerce')
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['bill_id'])
        
        # Filter invalid records
        df = df[df['consumption_kwh'].notnull()]
        df = df[df['total_amount'] > 0]
        
        return df
    
    def _clean_readings_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate readings data"""
        # Convert data types
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['reading'] = pd.to_numeric(df['reading'], errors='coerce')
        df['consumption_kwh'] = pd.to_numeric(df['consumption_kwh'], errors='coerce')
        df['voltage'] = pd.to_numeric(df['voltage'], errors='coerce')
        df['current_amp'] = pd.to_numeric(df['current_amp'], errors='coerce')
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['timestamp', 'meter_id'])
        
        # Filter invalid records
        df = df[df['timestamp'].notnull()]
        df = df[df['reading'].notnull()]
        df = df[df['voltage'].between(180, 260)]
        df = df[df['consumption_kwh'] >= 0]
        
        return df
    
    def _append_to_parquet(self, df: pd.DataFrame, output_path: Path):
        """Append dataframe to parquet file"""
        table = pa.Table.from_pandas(df)
        
        if output_path.exists():
            # Append to existing file
            pq.write_to_dataset(
                table,
                root_path=str(output_path),
                compression='snappy'
            )
        else:
            # Create new file
            pq.write_to_dataset(
                table,
                root_path=str(output_path),
                compression='snappy'
            )
    
    def process_bills(self, incremental: bool = True) -> int:
        """
        Process bills files one by one
        Returns: Number of files processed
        """
        logger.info(f"Starting bills processing (incremental={incremental})...")
        
        files_to_process = self._discover_files('bills', incremental)
        
        if not files_to_process:
            logger.info("No bills files to process")
            return 0
        
        output_path = self.target_dir / "bills.parquet"
        files_processed = 0
        records_processed = 0
        
        for file_path, division, subdivision in files_to_process:
            try:
                logger.info(f"Processing: {file_path}")
                
                # Read file
                df = pd.read_csv(file_path, sep='\t', dtype={
                    'bill_id': 'object',
                    'meter_id': 'object',
                    'billing_month': 'object',
                    'status': 'object'
                })
                
                # Add metadata
                df['division'] = division
                df['subdivision'] = subdivision
                df['source_file'] = str(file_path.relative_to(self.source_dir))
                df['etl_timestamp'] = datetime.now()
                
                # Clean data
                df = self._clean_bills_data(df)
                
                if len(df) == 0:
                    logger.warning(f"No valid records in {file_path}")
                    continue
                
                # Append to parquet
                self._append_to_parquet(df, output_path)
                
                # Update metadata
                file_key = str(file_path.relative_to(self.source_dir))
                self.metadata['bills'][file_key] = self._get_file_metadata(file_path)
                self.metadata['bills'][file_key]['records'] = len(df)
                
                files_processed += 1
                records_processed += len(df)
                
                logger.info(f"✓ Processed {len(df):,} records from {file_path.name}")
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        # Save metadata
        self._save_metadata()
        
        logger.info(f"✓ Bills processing complete: {files_processed} files, {records_processed:,} records")
        return files_processed
    
    def process_readings(self, incremental: bool = True) -> int:
        """
        Process readings files one by one
        Returns: Number of files processed
        """
        logger.info(f"Starting readings processing (incremental={incremental})...")
        
        files_to_process = self._discover_files('readings', incremental)
        
        if not files_to_process:
            logger.info("No readings files to process")
            return 0
        
        output_path = self.target_dir / "readings.parquet"
        files_processed = 0
        records_processed = 0
        
        for file_path, division, subdivision in files_to_process:
            try:
                logger.info(f"Processing: {file_path}")
                
                # Read file
                df = pd.read_csv(file_path, dtype={
                    'meter_id': 'object',
                    'quality_flag': 'object'
                })
                
                # Add metadata
                df['division'] = division
                df['subdivision'] = subdivision
                df['source_file'] = str(file_path.relative_to(self.source_dir))
                df['etl_timestamp'] = datetime.now()
                
                # Clean data
                df = self._clean_readings_data(df)
                
                if len(df) == 0:
                    logger.warning(f"No valid records in {file_path}")
                    continue
                
                # Append to parquet
                self._append_to_parquet(df, output_path)
                
                # Update metadata
                file_key = str(file_path.relative_to(self.source_dir))
                self.metadata['readings'][file_key] = self._get_file_metadata(file_path)
                self.metadata['readings'][file_key]['records'] = len(df)
                
                files_processed += 1
                records_processed += len(df)
                
                logger.info(f"✓ Processed {len(df):,} records from {file_path.name}")
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        # Save metadata
        self._save_metadata()
        
        logger.info(f"✓ Readings processing complete: {files_processed} files, {records_processed:,} records")
        return files_processed
    
    def run_full_load(self):
        """Run full ETL load"""
        logger.info("=" * 70)
        logger.info("STARTING FULL LOAD (File-by-File Processing)")
        logger.info("=" * 70)
        
        start_time = datetime.now()
        
        bills_count = self.process_bills(incremental=False)
        readings_count = self.process_readings(incremental=False)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info(f"FULL LOAD COMPLETE in {duration:.2f} seconds")
        logger.info(f"Bills files processed: {bills_count}")
        logger.info(f"Readings files processed: {readings_count}")
        logger.info("=" * 70)
    
    def run_incremental_load(self):
        """Run incremental ETL load"""
        logger.info("=" * 70)
        logger.info("STARTING INCREMENTAL LOAD (File-by-File Processing)")
        logger.info("=" * 70)
        
        start_time = datetime.now()
        
        bills_count = self.process_bills(incremental=True)
        readings_count = self.process_readings(incremental=True)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info(f"INCREMENTAL LOAD COMPLETE in {duration:.2f} seconds")
        logger.info(f"Bills files processed: {bills_count}")
        logger.info(f"Readings files processed: {readings_count}")
        logger.info("=" * 70)
    
    def get_statistics(self):
        """Get processing statistics"""
        stats = {
            'bills': {
                'files_tracked': len(self.metadata['bills']),
                'total_records': sum(f.get('records', 0) for f in self.metadata['bills'].values())
            },
            'readings': {
                'files_tracked': len(self.metadata['readings']),
                'total_records': sum(f.get('records', 0) for f in self.metadata['readings'].values())
            },
            'last_run': self.metadata.get('last_run', 'Never')
        }
        return stats


def main():
    """Main execution"""
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else 'incremental'
    
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║   Incremental ETL Pipeline (File-by-File Processing)      ║
    ║   Bronze Layer → Silver Layer                              ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    etl = IncrementalETL()
    
    if mode == 'full':
        etl.run_full_load()
    elif mode == 'incremental':
        etl.run_incremental_load()
    elif mode == 'stats':
        stats = etl.get_statistics()
        print("\n" + "=" * 70)
        print("PROCESSING STATISTICS")
        print("=" * 70)
        print(f"\nBills:")
        print(f"  Files tracked: {stats['bills']['files_tracked']}")
        print(f"  Total records: {stats['bills']['total_records']:,}")
        print(f"\nReadings:")
        print(f"  Files tracked: {stats['readings']['files_tracked']}")
        print(f"  Total records: {stats['readings']['total_records']:,}")
        print(f"\nLast run: {stats['last_run']}")
        print("=" * 70)
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python etl_pipeline_incremental.py [full|incremental|stats]")


if __name__ == "__main__":
    main()
