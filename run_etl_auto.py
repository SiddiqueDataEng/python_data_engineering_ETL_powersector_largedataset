"""
ETL Runner - Automatic execution (no prompts)
"""

from etl_pipeline import PowerSectorETL
import logging
import sys

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
    
    # Get mode from command line or default to incremental
    mode = sys.argv[1] if len(sys.argv) > 1 else 'incremental'
    
    # Initialize ETL with bronze layer as source
    etl = PowerSectorETL(
        source_dir="power_sector_data_bronze_layer",
        target_dir="power_sector_data_silver_layer"
    )
    
    if mode == 'full':
        print("\n🚀 Starting FULL LOAD...")
        print("Processing ALL files in the source directory.")
        etl.run_full_load(compression='snappy')
    
    elif mode == 'incremental':
        print("\n🔄 Starting INCREMENTAL LOAD...")
        print("Processing only new or changed files.")
        etl.run_incremental_load(compression='snappy')
    
    elif mode == 'stats':
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
        print(f"Unknown mode: {mode}")
        print("Usage: python run_etl_auto.py [full|incremental|stats]")
        sys.exit(1)
    
    print("\n✓ ETL Complete!")


if __name__ == "__main__":
    main()
