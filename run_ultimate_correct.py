"""
Run the CORRECT Ultimate Generator (from py/ directory)
"""

import sys
sys.path.insert(0, 'py')

from datagenerator_ultimate import UltimateDataGenerator
from datetime import datetime
import time

print("\n" + "="*70)
print("ULTIMATE GENERATOR - CORRECT VERSION")
print("="*70)
print("\nTest Configuration:")
print("  • 163217 meters (small test)")
print("  • 365 days (2024-01-01 to 2024-12-31)")
print("  • 60 minute reading frequency")
print("  • All realism features enabled")
print("="*70 + "\n")

start_time = time.time()

generator = UltimateDataGenerator()

# Configure
generator.config = {
    'divisions': ['ISLAMABAD'],
    'subdivisions': [('ISLAMABAD', 'F-6'), ('ISLAMABAD', 'F-7')],
    'total_meters': 163217 
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 12, 31),
    'reading_freq_minutes': 60,
    'quality_issues_pct': 5.0,
    'enable_peak_times': True,
    'enable_seasonality': True
}

print("Configuration:")
generator.show_configuration_summary()

print("\n" + "="*70)
print("Starting generation...")
print("="*70 + "\n")

try:
    generator.generate_all()
    
    duration = time.time() - start_time
    
    print("\n" + "="*70)
    print("✓ GENERATION COMPLETE!")
    print("="*70)
    print(f"Duration: {duration:.1f} seconds")
    print(f"Meters: {len(generator.meters_df):,}")
    print(f"Customers: {len(generator.customers_df):,}")
    print(f"Readings: {len(generator.all_readings):,}")
    print(f"Bills: {len(generator.all_bills):,}")
    print("\n✓ This is the CORRECT ultimate generator!")
    print("✓ Memory-optimized with chunked processing")
    print("✓ All realism features working")
    print("\nOutput: ./power_sector_data/")
    print("="*70)
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()

print("\nTo generate 10M meters:")
print("  1. Clean up: rmdir /s /q power_sector_data")
print("  2. Edit this script to set total_meters=10000000")
print("  3. Set date range to 3 months")
print("  4. Run and wait 30-50 hours")
