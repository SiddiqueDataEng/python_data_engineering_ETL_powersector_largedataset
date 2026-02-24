"""
Enhanced ETL Pipeline with Real-time Logging and Resume Capability
"""

import os
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Callable
import logging
import sys

import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar

# Configure logging
class StreamToLogger:
    """Redirect stdout/stderr to logger"""
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass


class PowerSectorETLWithLogging:
    """ETL Pipeline with real-time logging and resume capability"""
    
    def __init__(
        self,
        source_dir: str = r"\\counter2\E\iesco\power_sector_data_bronze_layer",
        target_dir: str = "power_sector_data_silver_layer",
        state_file: str = "etl_state.json",
        log_callback: Optional[Callable] = None
    ):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.state_file = Path(target_dir) / state_file
        self.checkpoint_file = Path(target_dir) / "etl_checkpoint.json"
        self.log_callback = log_callback
        
        # Create target directory
        self.target_dir.mkdir(parents=True, exist_ok=True)
        
        # Load processing state
        self.state = self._load_state()
        self.checkpoint = self._load_checkpoint()
        
        # Pause flag
        self.paused = False
        self.cancelled = False
        
        # Setup logging
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logger = logging.getLogger('ETL_Pipeline')
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        logger.handlers = []
        
        # File handler
        log_file = self.target_dir / 'etl_pipeline.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def log(self, message, level='info'):
        """Log message and call callback if provided"""
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'debug':
            self.logger.debug(message)
        
        # Call callback for real-time updates
        if self.log_callback:
            self.log_callback(message, level)
    
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
        self.log(f"State saved to {self.state_file}")
    
    def _load_checkpoint(self) -> Dict:
        """Load checkpoint for resume capability"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {
            'phase': None,
            'processed_files': [],
            'current_file': None,
            'timestamp': None
        }
    
    def _save_checkpoint(self, phase, processed_files, current_file=None):
        """Save checkpoint"""
        checkpoint = {
            'phase': phase,
            'processed_files': processed_files,
            'current_file': current_file,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, indent=2, fp=f)
        self.log(f"Checkpoint saved: {phase}")
    
    def _clear_checkpoint(self):
        """Clear checkpoint after successful completion"""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        self.checkpoint = {
            'phase': None,
            'processed_files': [],
            'current_file': None,
            'timestamp': None
        }
    
    def pause(self):
        """Pause ETL processing"""
        self.paused = True
        self.log("⏸️  ETL processing paused", 'warning')
    
    def resume(self):
        """Resume ETL processing"""
        self.paused = False
        self.log("▶️  ETL processing resumed", 'info')
    
    def cancel(self):
        """Cancel ETL processing"""
        self.cancelled = True
        self.log("🛑 ETL processing cancelled", 'warning')
    
    def _check_pause_cancel(self):
        """Check if paused or cancelled"""
        while self.paused and not self.cancelled:
            import time
            time.sleep(1)
        
        if self.cancelled:
            raise InterruptedError("ETL processing was cancelled")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """Generate hash for file"""
        stat = file_path.stat()
        return hashlib.md5(
            f"{stat.st_size}_{stat.st_mtime}".encode()
        ).hexdigest()

    
    def _discover_files(
        self,
        data_type: str,
        incremental: bool = False
    ) -> List[Tuple[Path, str, str]]:
        """Discover data files"""
        self.log(f"🔍 Discovering {data_type} files...")
        
        pattern = "bills.tsv" if data_type == "bills" else "readings.csv"
        discovered_files = []
        
        # Check if resuming
        processed_files = set(self.checkpoint.get('processed_files', []))
        
        for division_dir in self.source_dir.iterdir():
            if not division_dir.is_dir() or division_dir.name.startswith('.'):
                continue
            
            for subdivision_dir in division_dir.iterdir():
                if not subdivision_dir.is_dir():
                    continue
                
                data_dir = subdivision_dir / data_type
                if not data_dir.exists():
                    continue
                
                file_path = data_dir / pattern
                if not file_path.exists():
                    continue
                
                # Skip if already processed in this run
                file_key = str(file_path.relative_to(self.source_dir))
                if file_key in processed_files:
                    self.log(f"⏭️  Skipping already processed: {file_key}", 'debug')
                    continue
                
                # CDC check
                file_hash = self._get_file_hash(file_path)
                
                if incremental:
                    if file_key in self.state[data_type]:
                        if self.state[data_type][file_key] == file_hash:
                            self.log(f"⏭️  Skipping unchanged: {file_key}", 'debug')
                            continue
                
                discovered_files.append((file_path, division_dir.name, subdivision_dir.name))
                self.state[data_type][file_key] = file_hash
        
        self.log(f"✅ Discovered {len(discovered_files)} {data_type} files to process")
        return discovered_files
    
    def _clean_bills_data(self, df: dd.DataFrame) -> dd.DataFrame:
        """Clean and validate bills data"""
        self.log("🧹 Cleaning bills data...")
        
        # Convert data types
        df['billing_month'] = dd.to_datetime(df['billing_month'], format='%Y-%m', errors='coerce')
        df['consumption_kwh'] = dd.to_numeric(df['consumption_kwh'], errors='coerce')
        df['energy_charges'] = dd.to_numeric(df['energy_charges'], errors='coerce')
        df['meter_rent'] = dd.to_numeric(df['meter_rent'], errors='coerce')
        df['gst'] = dd.to_numeric(df['gst'], errors='coerce')
        df['total_amount'] = dd.to_numeric(df['total_amount'], errors='coerce')
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['bill_id'])
        
        # Filter invalid records
        df = df[df['consumption_kwh'].notnull()]
        df = df[df['total_amount'] > 0]
        
        return df
    
    def _clean_readings_data(self, df: dd.DataFrame) -> dd.DataFrame:
        """Clean and validate readings data"""
        self.log("🧹 Cleaning readings data...")
        
        # Convert data types
        df['timestamp'] = dd.to_datetime(df['timestamp'], errors='coerce')
        df['reading'] = dd.to_numeric(df['reading'], errors='coerce')
        df['consumption_kwh'] = dd.to_numeric(df['consumption_kwh'], errors='coerce')
        df['voltage'] = dd.to_numeric(df['voltage'], errors='coerce')
        df['current_amp'] = dd.to_numeric(df['current_amp'], errors='coerce')
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['timestamp', 'meter_id'])
        
        # Filter invalid records
        df = df[df['timestamp'].notnull()]
        df = df[df['reading'].notnull()]
        df = df[df['voltage'].between(180, 260)]
        df = df[df['consumption_kwh'] >= 0]
        
        return df
    
    def process_bills(
        self,
        incremental: bool = False,
        compression: str = 'snappy',
        resume: bool = False
    ) -> Path:
        """Process bills with logging and resume capability"""
        self.log("=" * 70)
        self.log(f"📊 PROCESSING BILLS ({'Incremental' if incremental else 'Full Load'})")
        self.log("=" * 70)
        
        start_time = datetime.now()
        processed_files = []
        
        try:
            # Check if resuming
            if resume and self.checkpoint.get('phase') == 'bills':
                self.log("🔄 Resuming from checkpoint...")
                processed_files = self.checkpoint.get('processed_files', [])
            
            # Discover files
            files_to_process = self._discover_files('bills', incremental)
            
            if not files_to_process:
                self.log("ℹ️  No bills files to process")
                return None
            
            # Read and process files
            dfs = []
            total_files = len(files_to_process)
            
            for idx, (file_path, division, subdivision) in enumerate(files_to_process, 1):
                self._check_pause_cancel()
                
                try:
                    self.log(f"📄 [{idx}/{total_files}] Reading: {file_path.name}")
                    self.log(f"   Division: {division}, Subdivision: {subdivision}")
                    
                    df = dd.read_csv(
                        file_path,
                        sep='\t',
                        dtype={
                            'bill_id': 'object',
                            'meter_id': 'object',
                            'billing_month': 'object',
                            'status': 'object'
                        },
                        blocksize='64MB'
                    )
                    
                    # Add metadata
                    df['division'] = division
                    df['subdivision'] = subdivision
                    df['source_file'] = str(file_path.relative_to(self.source_dir))
                    df['etl_timestamp'] = datetime.now()
                    
                    dfs.append(df)
                    
                    file_key = str(file_path.relative_to(self.source_dir))
                    processed_files.append(file_key)
                    
                    # Save checkpoint
                    self._save_checkpoint('bills', processed_files, file_key)
                    
                    self.log(f"✅ [{idx}/{total_files}] Processed successfully")
                    
                except Exception as e:
                    self.log(f"❌ Error reading {file_path}: {e}", 'error')
                    continue
            
            if not dfs:
                self.log("⚠️  No valid bills data to process", 'warning')
                return None
            
            # Concatenate
            self.log("🔗 Concatenating bills data...")
            combined_df = dd.concat(dfs, ignore_index=True)
            
            # Clean
            combined_df = self._clean_bills_data(combined_df)
            
            # Output
            output_path = self.target_dir / "bills.parquet"
            
            # Handle incremental
            if incremental and output_path.exists():
                self.log("🔄 Merging with existing bills data...")
                existing_df = dd.read_parquet(output_path)
                combined_df = dd.concat([existing_df, combined_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['bill_id'], keep='last')
            
            # Write
            self.log(f"💾 Writing bills to {output_path}...")
            
            # Extreme memory constraint workaround: Write partitions one by one
            self.log(f"📝 Writing {combined_df.npartitions} partitions one by one (memory-safe)...")
            
            import shutil
            if output_path.exists():
                shutil.rmtree(output_path)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Write each partition separately
            for i in range(combined_df.npartitions):
                partition = combined_df.get_partition(i)
                part_file = output_path / f"part.{i}.parquet"
                
                # Compute and write this partition only
                partition_df = partition.compute()
                partition_df.to_parquet(
                    part_file,
                    engine='pyarrow',
                    compression=compression,
                    index=False
                )
                
                if (i + 1) % 10 == 0 or i == combined_df.npartitions - 1:
                    self.log(f"  ✓ Written {i + 1}/{combined_df.npartitions} partitions...")
            
            self.log(f"✓ All partitions written to {output_path}")
            
            # Update state
            self.state['last_incremental_load' if incremental else 'last_full_load'] = datetime.now().isoformat()
            self._save_state()
            
            # Clear checkpoint
            self._clear_checkpoint()
            
            duration = (datetime.now() - start_time).total_seconds()
            self.log("=" * 70)
            self.log(f"✅ Bills processing complete in {duration:.2f} seconds")
            self.log(f"📁 Output: {output_path}")
            self.log("=" * 70)
            
            return output_path
            
        except InterruptedError as e:
            self.log(f"⏸️  {str(e)}", 'warning')
            return None
        except Exception as e:
            self.log(f"❌ Error processing bills: {e}", 'error')
            raise
    
    def process_readings(
        self,
        incremental: bool = False,
        compression: str = 'snappy',
        resume: bool = False
    ) -> Path:
        """Process readings with logging and resume capability"""
        self.log("=" * 70)
        self.log(f"📈 PROCESSING READINGS ({'Incremental' if incremental else 'Full Load'})")
        self.log("=" * 70)
        
        start_time = datetime.now()
        processed_files = []
        
        try:
            # Check if resuming
            if resume and self.checkpoint.get('phase') == 'readings':
                self.log("🔄 Resuming from checkpoint...")
                processed_files = self.checkpoint.get('processed_files', [])
            
            # Discover files
            files_to_process = self._discover_files('readings', incremental)
            
            if not files_to_process:
                self.log("ℹ️  No readings files to process")
                return None
            
            # Read and process files
            dfs = []
            total_files = len(files_to_process)
            
            for idx, (file_path, division, subdivision) in enumerate(files_to_process, 1):
                self._check_pause_cancel()
                
                try:
                    self.log(f"📄 [{idx}/{total_files}] Reading: {file_path.name}")
                    self.log(f"   Division: {division}, Subdivision: {subdivision}")
                    
                    df = dd.read_csv(
                        file_path,
                        dtype={
                            'meter_id': 'object',
                            'quality_flag': 'object'
                        },
                        blocksize='128MB',
                        assume_missing=True
                    )
                    
                    # Add metadata
                    df['division'] = division
                    df['subdivision'] = subdivision
                    df['source_file'] = str(file_path.relative_to(self.source_dir))
                    df['etl_timestamp'] = datetime.now()
                    
                    dfs.append(df)
                    
                    file_key = str(file_path.relative_to(self.source_dir))
                    processed_files.append(file_key)
                    
                    # Save checkpoint
                    self._save_checkpoint('readings', processed_files, file_key)
                    
                    self.log(f"✅ [{idx}/{total_files}] Processed successfully")
                    
                except Exception as e:
                    self.log(f"❌ Error reading {file_path}: {e}", 'error')
                    continue
            
            if not dfs:
                self.log("⚠️  No valid readings data to process", 'warning')
                return None
            
            # Concatenate
            self.log("🔗 Concatenating readings data...")
            combined_df = dd.concat(dfs, ignore_index=True)
            
            # Clean
            combined_df = self._clean_readings_data(combined_df)
            
            # Output
            output_path = self.target_dir / "readings.parquet"
            
            # Handle incremental
            if incremental and output_path.exists():
                self.log("🔄 Merging with existing readings data...")
                existing_df = dd.read_parquet(output_path)
                combined_df = dd.concat([existing_df, combined_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(
                    subset=['timestamp', 'meter_id'],
                    keep='last'
                )
            
            # Write
            self.log(f"💾 Writing readings to {output_path}...")
            
            # Extreme memory constraint workaround: Write partitions one by one
            self.log(f"📝 Writing {combined_df.npartitions} partitions one by one (memory-safe)...")
            
            import shutil
            if output_path.exists():
                shutil.rmtree(output_path)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Write each partition separately
            for i in range(combined_df.npartitions):
                partition = combined_df.get_partition(i)
                part_file = output_path / f"part.{i}.parquet"
                
                # Compute and write this partition only
                partition_df = partition.compute()
                partition_df.to_parquet(
                    part_file,
                    engine='pyarrow',
                    compression=compression,
                    index=False
                )
                
                if (i + 1) % 10 == 0 or i == combined_df.npartitions - 1:
                    self.log(f"  ✓ Written {i + 1}/{combined_df.npartitions} partitions...")
            
            self.log(f"✓ All partitions written to {output_path}")
            
            # Update state
            self.state['last_incremental_load' if incremental else 'last_full_load'] = datetime.now().isoformat()
            self._save_state()
            
            # Clear checkpoint
            self._clear_checkpoint()
            
            duration = (datetime.now() - start_time).total_seconds()
            self.log("=" * 70)
            self.log(f"✅ Readings processing complete in {duration:.2f} seconds")
            self.log(f"📁 Output: {output_path}")
            self.log("=" * 70)
            
            return output_path
            
        except InterruptedError as e:
            self.log(f"⏸️  {str(e)}", 'warning')
            return None
        except Exception as e:
            self.log(f"❌ Error processing readings: {e}", 'error')
            raise
    
    def run_full_load(self, compression: str = 'snappy', resume: bool = False):
        """Run full load with logging"""
        self.log("=" * 70)
        self.log("🚀 STARTING FULL LOAD")
        self.log("=" * 70)
        
        start_time = datetime.now()
        
        try:
            bills_path = self.process_bills(incremental=False, compression=compression, resume=resume)
            readings_path = self.process_readings(incremental=False, compression=compression, resume=resume)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            self.log("=" * 70)
            self.log(f"🎉 FULL LOAD COMPLETE in {duration:.2f} seconds ({duration/60:.2f} minutes)")
            self.log(f"📁 Bills: {bills_path}")
            self.log(f"📁 Readings: {readings_path}")
            self.log("=" * 70)
            
            return bills_path, readings_path
            
        except Exception as e:
            self.log(f"❌ Full load failed: {e}", 'error')
            raise
    
    def run_incremental_load(self, compression: str = 'snappy', resume: bool = False):
        """Run incremental load with logging"""
        self.log("=" * 70)
        self.log("🔄 STARTING INCREMENTAL LOAD")
        self.log("=" * 70)
        
        start_time = datetime.now()
        
        try:
            bills_path = self.process_bills(incremental=True, compression=compression, resume=resume)
            readings_path = self.process_readings(incremental=True, compression=compression, resume=resume)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            self.log("=" * 70)
            self.log(f"🎉 INCREMENTAL LOAD COMPLETE in {duration:.2f} seconds")
            self.log(f"📁 Bills: {bills_path}")
            self.log(f"📁 Readings: {readings_path}")
            self.log("=" * 70)
            
            return bills_path, readings_path
            
        except Exception as e:
            self.log(f"❌ Incremental load failed: {e}", 'error')
            raise
