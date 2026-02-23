"""
ETL Pipeline Validation Script
Validates the output Parquet files and provides data quality report
"""

import pandas as pd
import dask.dataframe as dd
from pathlib import Path
from datetime import datetime


def validate_parquet_files(target_dir: str = "power_sector_data_silver_layer"):
    """Validate ETL output files"""
    
    target_path = Path(target_dir)
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    print("=" * 80)
    print("ETL PIPELINE VALIDATION REPORT")
    print("=" * 80)
    print(f"Generated: {datetime.now()}")
    print("=" * 80)
    
    # Validate Bills
    if bills_path.exists():
        print("\n📄 BILLS VALIDATION")
        print("-" * 80)
        
        df = dd.read_parquet(bills_path)
        
        # Basic stats
        total_records = len(df)
        print(f"✓ Total Records: {total_records:,}")
        print(f"✓ File Size: {bills_path.stat().st_size / (1024*1024):.2f} MB")
        
        # Schema validation
        expected_cols = [
            'bill_id', 'meter_id', 'billing_month', 'consumption_kwh',
            'energy_charges', 'meter_rent', 'gst', 'total_amount', 'status',
            'division', 'subdivision', 'source_file', 'etl_timestamp'
        ]
        
        missing_cols = set(expected_cols) - set(df.columns)
        if missing_cols:
            print(f"⚠ Missing columns: {missing_cols}")
        else:
            print("✓ All expected columns present")
        
        # Data quality checks
        print("\nData Quality Checks:")
        
        # Duplicates
        duplicates = df['bill_id'].duplicated().sum().compute()
        print(f"  Duplicate bill_ids: {duplicates}")
        
        # Null values
        null_counts = df.isnull().sum().compute()
        critical_nulls = null_counts[['bill_id', 'meter_id', 'consumption_kwh']]
        if critical_nulls.sum() > 0:
            print(f"  ⚠ Critical null values found:")
            for col, count in critical_nulls.items():
                if count > 0:
                    print(f"    {col}: {count}")
        else:
            print("  ✓ No critical null values")
        
        # Value ranges
        sample = df.compute().head(1000)  # Sample for quick validation
        
        if (sample['consumption_kwh'] < 0).any():
            print("  ⚠ Negative consumption values found")
        else:
            print("  ✓ Consumption values valid")
        
        if (sample['total_amount'] <= 0).any():
            print("  ⚠ Invalid total_amount values found")
        else:
            print("  ✓ Total amount values valid")
        
        # Summary statistics
        print("\nSummary Statistics:")
        print(f"  Unique meters: {df['meter_id'].nunique().compute():,}")
        print(f"  Divisions: {df['division'].nunique().compute()}")
        print(f"  Subdivisions: {df['subdivision'].nunique().compute()}")
        print(f"  Date range: {df['billing_month'].min().compute()} to {df['billing_month'].max().compute()}")
        print(f"  Avg consumption: {df['consumption_kwh'].mean().compute():.2f} kWh")
        print(f"  Avg bill amount: {df['total_amount'].mean().compute():.2f}")
        
    else:
        print("\n⚠ Bills file not found!")
    
    # Validate Readings
    if readings_path.exists():
        print("\n\n📊 READINGS VALIDATION")
        print("-" * 80)
        
        df = dd.read_parquet(readings_path)
        
        # Basic stats
        total_records = len(df)
        print(f"✓ Total Records: {total_records:,}")
        print(f"✓ File Size: {readings_path.stat().st_size / (1024*1024):.2f} MB")
        
        # Schema validation
        expected_cols = [
            'timestamp', 'meter_id', 'reading', 'consumption_kwh',
            'voltage', 'current_amp', 'quality_flag',
            'division', 'subdivision', 'source_file', 'etl_timestamp'
        ]
        
        missing_cols = set(expected_cols) - set(df.columns)
        if missing_cols:
            print(f"⚠ Missing columns: {missing_cols}")
        else:
            print("✓ All expected columns present")
        
        # Data quality checks
        print("\nData Quality Checks:")
        
        # Duplicates
        duplicates = df.duplicated(subset=['timestamp', 'meter_id']).sum().compute()
        print(f"  Duplicate records: {duplicates}")
        
        # Null values
        null_counts = df.isnull().sum().compute()
        critical_nulls = null_counts[['timestamp', 'meter_id', 'reading']]
        if critical_nulls.sum() > 0:
            print(f"  ⚠ Critical null values found:")
            for col, count in critical_nulls.items():
                if count > 0:
                    print(f"    {col}: {count}")
        else:
            print("  ✓ No critical null values")
        
        # Value ranges
        sample = df.compute().head(1000)
        
        voltage_issues = ((sample['voltage'] < 180) | (sample['voltage'] > 260)).sum()
        if voltage_issues > 0:
            print(f"  ⚠ {voltage_issues} voltage readings out of range (180-260V)")
        else:
            print("  ✓ Voltage values within valid range")
        
        if (sample['consumption_kwh'] < 0).any():
            print("  ⚠ Negative consumption values found")
        else:
            print("  ✓ Consumption values valid")
        
        # Summary statistics
        print("\nSummary Statistics:")
        print(f"  Unique meters: {df['meter_id'].nunique().compute():,}")
        print(f"  Divisions: {df['division'].nunique().compute()}")
        print(f"  Subdivisions: {df['subdivision'].nunique().compute()}")
        print(f"  Date range: {df['timestamp'].min().compute()} to {df['timestamp'].max().compute()}")
        print(f"  Avg consumption: {df['consumption_kwh'].mean().compute():.4f} kWh")
        print(f"  Avg voltage: {df['voltage'].mean().compute():.2f} V")
        print(f"  Avg current: {df['current_amp'].mean().compute():.2f} A")
        
        # Quality flag distribution
        print("\nQuality Flag Distribution:")
        quality_dist = df['quality_flag'].value_counts().compute()
        for flag, count in quality_dist.items():
            print(f"  {flag}: {count:,} ({count/total_records*100:.2f}%)")
        
    else:
        print("\n⚠ Readings file not found!")
    
    print("\n" + "=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)


def compare_source_vs_output(source_dir: str = "power_sector_data", 
                             target_dir: str = "power_sector_data_silver_layer"):
    """Compare source file counts vs output records"""
    
    print("\n" + "=" * 80)
    print("SOURCE vs OUTPUT COMPARISON")
    print("=" * 80)
    
    source_path = Path(source_dir)
    
    # Count source files
    bills_files = list(source_path.rglob("bills/bills.tsv"))
    readings_files = list(source_path.rglob("readings/readings.csv"))
    
    print(f"\nSource Files:")
    print(f"  Bills files: {len(bills_files)}")
    print(f"  Readings files: {len(readings_files)}")
    
    # Count output records
    target_path = Path(target_dir)
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    if bills_path.exists():
        bills_df = dd.read_parquet(bills_path)
        unique_sources = bills_df['source_file'].nunique().compute()
        print(f"\nBills Output:")
        print(f"  Unique source files processed: {unique_sources}")
        print(f"  Coverage: {unique_sources/len(bills_files)*100:.1f}%")
    
    if readings_path.exists():
        readings_df = dd.read_parquet(readings_path)
        unique_sources = readings_df['source_file'].nunique().compute()
        print(f"\nReadings Output:")
        print(f"  Unique source files processed: {unique_sources}")
        print(f"  Coverage: {unique_sources/len(readings_files)*100:.1f}%")
    
    print("=" * 80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate ETL Pipeline Output')
    parser.add_argument(
        '--target',
        default='power_sector_data_silver_layer',
        help='Target directory with Parquet files'
    )
    parser.add_argument(
        '--compare',
        action='store_true',
        help='Compare source vs output coverage'
    )
    
    args = parser.parse_args()
    
    validate_parquet_files(args.target)
    
    if args.compare:
        compare_source_vs_output(target_dir=args.target)
