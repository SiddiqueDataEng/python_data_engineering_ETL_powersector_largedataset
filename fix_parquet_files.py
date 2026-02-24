"""
Fix Parquet Files - Convert directory-based to single files
"""

import shutil
from pathlib import Path

target_dir = Path("power_sector_data_silver_layer")

print("Fixing Parquet files...")
print("=" * 60)

# Check and fix bills.parquet
bills_path = target_dir / "bills.parquet"
if bills_path.exists() and bills_path.is_dir():
    print(f"✓ Found directory: {bills_path}")
    print(f"  Removing directory...")
    shutil.rmtree(bills_path)
    print(f"  ✓ Removed")
else:
    print(f"ℹ️  bills.parquet is OK or doesn't exist")

# Check and fix readings.parquet
readings_path = target_dir / "readings.parquet"
if readings_path.exists() and readings_path.is_dir():
    print(f"✓ Found directory: {readings_path}")
    print(f"  Removing directory...")
    shutil.rmtree(readings_path)
    print(f"  ✓ Removed")
else:
    print(f"ℹ️  readings.parquet is OK or doesn't exist")

print("=" * 60)
print("✓ Done! Now run ETL again to create proper single files.")
print("\nRun: python run_etl.py")
