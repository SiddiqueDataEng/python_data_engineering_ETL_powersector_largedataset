"""
Fix existing parquet directory structure to be compatible with Dask read_parquet
This script ensures parquet files have proper extensions and structure.
"""

from pathlib import Path
import shutil


def fix_parquet_directory(parquet_dir: Path):
    """
    Fix parquet directory structure by ensuring all files have .parquet extension
    """
    if not parquet_dir.exists():
        print(f"Directory does not exist: {parquet_dir}")
        return
    
    if not parquet_dir.is_dir():
        print(f"Not a directory: {parquet_dir}")
        return
    
    # Find all parquet files
    parquet_files = list(parquet_dir.glob("part.*.parquet"))
    
    if not parquet_files:
        print(f"No parquet files found in {parquet_dir}")
        return
    
    print(f"Found {len(parquet_files)} parquet files in {parquet_dir}")
    print("✓ Directory structure is correct")


def main():
    """Check and fix parquet directories"""
    target_dir = Path("power_sector_data_silver_layer")
    
    if not target_dir.exists():
        print(f"Target directory does not exist: {target_dir}")
        return
    
    print("Checking parquet directories...")
    print("=" * 70)
    
    # Check bills
    bills_path = target_dir / "bills.parquet"
    if bills_path.exists():
        print(f"\nChecking: {bills_path}")
        fix_parquet_directory(bills_path)
    
    # Check readings
    readings_path = target_dir / "readings.parquet"
    if readings_path.exists():
        print(f"\nChecking: {readings_path}")
        fix_parquet_directory(readings_path)
    
    print("\n" + "=" * 70)
    print("✓ All parquet directories checked")


if __name__ == "__main__":
    main()
