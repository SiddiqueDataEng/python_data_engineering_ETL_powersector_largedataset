"""
Enhanced Command Line Interface for ETL Pipeline
Provides detailed progress tracking and statistics
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from etl_pipeline import PowerSectorETL
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def print_banner():
    """Print application banner"""
    banner = """
    ╔════════════════════════════════════════════════════════════╗
    ║                                                            ║
    ║        Power Sector Data ETL Pipeline                     ║
    ║        Extract → Transform → Load                         ║
    ║                                                            ║
    ╚════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_section(title):
    """Print section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def run_full_load(etl, compression='snappy'):
    """Run full load with progress tracking"""
    print_section("FULL LOAD - Processing All Data")
    
    start_time = datetime.now()
    
    print("\n📊 Phase 1: Processing Bills")
    print("-" * 70)
    bills_path = etl.process_bills(incremental=False, compression=compression)
    
    if bills_path:
        print(f"✅ Bills processed: {bills_path}")
        print(f"   Size: {bills_path.stat().st_size / (1024**2):.2f} MB")
    
    print("\n📈 Phase 2: Processing Readings")
    print("-" * 70)
    readings_path = etl.process_readings(incremental=False, compression=compression)
    
    if readings_path:
        print(f"✅ Readings processed: {readings_path}")
        print(f"   Size: {readings_path.stat().st_size / (1024**2):.2f} MB")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print_section("FULL LOAD COMPLETE")
    print(f"⏱️  Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    print(f"📁 Output: {etl.target_dir}")
    
    return bills_path, readings_path


def run_incremental_load(etl, compression='snappy'):
    """Run incremental load with progress tracking"""
    print_section("INCREMENTAL LOAD - Processing New/Changed Data")
    
    start_time = datetime.now()
    
    print("\n🔄 Phase 1: Processing Bills (Incremental)")
    print("-" * 70)
    bills_path = etl.process_bills(incremental=True, compression=compression)
    
    if bills_path:
        print(f"✅ Bills updated: {bills_path}")
    else:
        print("ℹ️  No new bills to process")
    
    print("\n🔄 Phase 2: Processing Readings (Incremental)")
    print("-" * 70)
    readings_path = etl.process_readings(incremental=True, compression=compression)
    
    if readings_path:
        print(f"✅ Readings updated: {readings_path}")
    else:
        print("ℹ️  No new readings to process")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print_section("INCREMENTAL LOAD COMPLETE")
    print(f"⏱️  Duration: {duration:.2f} seconds")
    print(f"📁 Output: {etl.target_dir}")
    
    return bills_path, readings_path


def show_statistics(etl):
    """Display detailed statistics"""
    print_section("DATA STATISTICS")
    
    stats = etl.get_statistics()
    
    if 'bills' in stats:
        print("\n📄 BILLS")
        print("-" * 70)
        print(f"  Total Records:     {stats['bills']['total_records']:,}")
        print(f"  File Size:         {stats['bills']['file_size_mb']:.2f} MB")
        print(f"  Divisions:         {stats['bills']['divisions']}")
        print(f"  Subdivisions:      {stats['bills']['subdivisions']}")
        print(f"  Date Range:        {stats['bills']['date_range'][0]} to {stats['bills']['date_range'][1]}")
    
    if 'readings' in stats:
        print("\n📊 READINGS")
        print("-" * 70)
        print(f"  Total Records:     {stats['readings']['total_records']:,}")
        print(f"  File Size:         {stats['readings']['file_size_mb']:.2f} MB")
        print(f"  Divisions:         {stats['readings']['divisions']}")
        print(f"  Subdivisions:      {stats['readings']['subdivisions']}")
        print(f"  Date Range:        {stats['readings']['date_range'][0]} to {stats['readings']['date_range'][1]}")
    
    # State information
    print("\n📋 ETL STATE")
    print("-" * 70)
    if etl.state.get('last_full_load'):
        print(f"  Last Full Load:    {etl.state['last_full_load']}")
    if etl.state.get('last_incremental_load'):
        print(f"  Last Incremental:  {etl.state['last_incremental_load']}")
    
    print(f"  Tracked Bills:     {len(etl.state.get('bills', {}))}")
    print(f"  Tracked Readings:  {len(etl.state.get('readings', {}))}")


def validate_data(etl):
    """Run data validation"""
    print_section("DATA VALIDATION")
    
    target_path = etl.target_dir
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    if not bills_path.exists() and not readings_path.exists():
        print("❌ No processed data found. Please run ETL pipeline first.")
        return
    
    import dask.dataframe as dd
    
    # Validate bills
    if bills_path.exists():
        print("\n📄 Bills Validation")
        print("-" * 70)
        
        bills_df = dd.read_parquet(bills_path)
        
        # Check duplicates
        duplicates = bills_df['bill_id'].duplicated().sum().compute()
        if duplicates == 0:
            print("  ✅ No duplicate bill_ids")
        else:
            print(f"  ❌ Found {duplicates} duplicate bill_ids")
        
        # Check nulls
        null_consumption = bills_df['consumption_kwh'].isnull().sum().compute()
        if null_consumption == 0:
            print("  ✅ No null consumption values")
        else:
            print(f"  ⚠️  Found {null_consumption} null consumption values")
        
        # Check amounts
        invalid_amounts = (bills_df['total_amount'] <= 0).sum().compute()
        if invalid_amounts == 0:
            print("  ✅ All amounts are valid")
        else:
            print(f"  ❌ Found {invalid_amounts} invalid amounts")
    
    # Validate readings
    if readings_path.exists():
        print("\n📊 Readings Validation")
        print("-" * 70)
        
        readings_df = dd.read_parquet(readings_path)
        
        # Check duplicates
        duplicates = readings_df.duplicated(subset=['timestamp', 'meter_id']).sum().compute()
        if duplicates == 0:
            print("  ✅ No duplicate records")
        else:
            print(f"  ❌ Found {duplicates} duplicate records")
        
        # Check voltage
        sample = readings_df.sample(frac=0.1).compute()
        voltage_issues = ((sample['voltage'] < 180) | (sample['voltage'] > 260)).sum()
        if voltage_issues == 0:
            print("  ✅ All voltage values in valid range (180-260V)")
        else:
            print(f"  ⚠️  Found {voltage_issues} voltage values out of range")
        
        # Check consumption
        negative_consumption = (readings_df['consumption_kwh'] < 0).sum().compute()
        if negative_consumption == 0:
            print("  ✅ All consumption values are non-negative")
        else:
            print(f"  ❌ Found {negative_consumption} negative consumption values")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Power Sector ETL Pipeline - Command Line Interface',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full load with default settings
  python etl_cli.py --mode full
  
  # Incremental load
  python etl_cli.py --mode incremental
  
  # Show statistics
  python etl_cli.py --mode stats
  
  # Validate data
  python etl_cli.py --mode validate
  
  # Full load with gzip compression
  python etl_cli.py --mode full --compression gzip
  
  # Custom directories
  python etl_cli.py --mode full --source "C:\\data\\source" --target "C:\\data\\output"
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental', 'stats', 'validate'],
        required=True,
        help='ETL mode to run'
    )
    
    parser.add_argument(
        '--compression',
        choices=['snappy', 'gzip', 'brotli'],
        default='snappy',
        help='Parquet compression algorithm (default: snappy)'
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
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Print banner
    print_banner()
    
    # Initialize ETL
    print(f"📂 Source: {args.source}")
    print(f"📁 Target: {args.target}")
    print(f"🗜️  Compression: {args.compression}")
    print()
    
    try:
        etl = PowerSectorETL(
            source_dir=args.source,
            target_dir=args.target
        )
        
        # Execute based on mode
        if args.mode == 'full':
            run_full_load(etl, args.compression)
        
        elif args.mode == 'incremental':
            run_incremental_load(etl, args.compression)
        
        elif args.mode == 'stats':
            show_statistics(etl)
        
        elif args.mode == 'validate':
            validate_data(etl)
        
        print("\n✅ Operation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
