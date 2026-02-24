"""
Cleanup corrupted or incomplete parquet directories
Run this if ETL was interrupted and left partial files
"""

from pathlib import Path
import shutil


def cleanup_parquet_directories():
    """Remove incomplete parquet directories"""
    target_dir = Path("power_sector_data_silver_layer")
    
    if not target_dir.exists():
        print(f"Target directory does not exist: {target_dir}")
        return
    
    print("Cleaning up parquet directories...")
    print("=" * 70)
    
    # Check bills
    bills_path = target_dir / "bills.parquet"
    if bills_path.exists():
        print(f"\nRemoving: {bills_path}")
        try:
            shutil.rmtree(bills_path)
            print("✓ Removed bills.parquet")
        except Exception as e:
            print(f"✗ Error removing bills.parquet: {e}")
    
    # Check readings
    readings_path = target_dir / "readings.parquet"
    if readings_path.exists():
        print(f"\nRemoving: {readings_path}")
        try:
            shutil.rmtree(readings_path)
            print("✓ Removed readings.parquet")
        except Exception as e:
            print(f"✗ Error removing readings.parquet: {e}")
    
    # Check state file
    state_file = target_dir / "etl_state.json"
    if state_file.exists():
        print(f"\nRemoving: {state_file}")
        try:
            state_file.unlink()
            print("✓ Removed etl_state.json")
        except Exception as e:
            print(f"✗ Error removing state file: {e}")
    
    print("\n" + "=" * 70)
    print("✓ Cleanup complete!")
    print("\nYou can now run a fresh ETL:")
    print("  python run_etl_auto.py full")


if __name__ == "__main__":
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║        Parquet Cleanup Utility                             ║
    ║        Removes incomplete/corrupted parquet files          ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    confirm = input("This will delete all processed data. Continue? (yes/no): ").strip().lower()
    
    if confirm == 'yes':
        cleanup_parquet_directories()
    else:
        print("Cancelled.")
