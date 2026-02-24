"""
ETL Runner - Uses current folder name (power_sector_data)
"""

from etl_pipeline import PowerSectorETL
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║        Power Sector Data ETL Pipeline                      ║
    ║        Extract → Transform → Load                          ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    print("Configuration:")
    print("  Source: power_sector_data_bronze_layer")
    print("  Target: power_sector_data_silver_layer")
    print()
    
    print("Select ETL Mode:")
    print("1. Full Load (Process all files)")
    print("2. Incremental Load (Process only new/changed files)")
    print("3. Show Statistics")
    print("4. Exit")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == '4':
        print("Exiting...")
        return
    
    # Initialize ETL with bronze layer as source
    etl = PowerSectorETL(
        source_dir="power_sector_data_bronze_layer",
        target_dir="power_sector_data_silver_layer"
    )
    
    if choice == '1':
        print("\n🚀 Starting FULL LOAD...")
        print("This will process ALL files in the source directory.")
        confirm = input("Continue? (y/n): ").strip().lower()
        if confirm == 'y':
            etl.run_full_load(compression='snappy')
        else:
            print("Cancelled.")
    
    elif choice == '2':
        print("\n🔄 Starting INCREMENTAL LOAD...")
        print("This will process only new or changed files.")
        print("\nNote: Bills already processed, will only process readings.")
        confirm = input("Continue? (y/n): ").strip().lower()
        if confirm == 'y':
            etl.run_incremental_load(compression='snappy')
        else:
            print("Cancelled.")
    
    elif choice == '3':
        print("\n📊 Fetching Statistics...")
        stats = etl.get_statistics()
        
        print("\n" + "=" * 70)
        print("DATA STATISTICS")
        print("=" * 70)
        
        for data_type, info in stats.items():
            print(f"\n{data_type.upper()}:")
            print("-" * 70)
            for key, value in info.items():
                if key == 'file_size_mb':
                    print(f"  {key:20s}: {value:.2f} MB")
                elif key == 'total_records':
                    print(f"  {key:20s}: {value:,}")
                else:
                    print(f"  {key:20s}: {value}")
        print("=" * 70)
    
    else:
        print("Invalid choice!")


if __name__ == "__main__":
    main()
