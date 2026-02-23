"""
FULL PRODUCTION RUN - Ultimate Generator
ALL Divisions, ALL Subdivisions, 163,217 Meters, Full Year 2024
"""

import sys
sys.path.insert(0, 'py')

from datagenerator_ultimate import UltimateDataGenerator
from pakistani_data_constants import SUB_DIVISIONS
from datetime import datetime
import time

print("\n" + "="*70)
print("ULTIMATE GENERATOR - FULL PRODUCTION RUN")
print("="*70)

# Get ALL subdivisions from constants
all_subdivisions = []
for division, subdivs in SUB_DIVISIONS.items():
    for subdiv in subdivs:
        all_subdivisions.append((division, subdiv))

print(f"\nLoaded from pakistani_data_constants.py:")
print(f"  Total divisions: {len(SUB_DIVISIONS)}")
print(f"  Total subdivisions: {len(all_subdivisions)}")
print("\nDivision breakdown:")
for div in SUB_DIVISIONS.keys():
    print(f"  • {div}: {len(SUB_DIVISIONS[div])} subdivisions")

print("\n" + "="*70)
print("CONFIGURATION")
print("="*70)
print("  • Meters: 163,217")
print("  • Divisions: ALL 5 (ISLAMABAD, RAWALPINDI, ATTOCK, JHELUM, CHAKWAL)")
print("  • Subdivisions: ALL 90")
print("  • Date range: 2024-01-01 to 2024-12-31 (365 days)")
print("  • Reading frequency: 60 minutes (24 readings/day)")
print("  • Quality issues: 5%")
print("  • Peak times: Enabled")
print("  • Seasonality: Enabled")
print("="*70 + "\n")

start_time = time.time()

generator = UltimateDataGenerator()

# Configure with ALL divisions and subdivisions from constants
generator.config = {
    'divisions': list(SUB_DIVISIONS.keys()),  # ALL 5 divisions
    'subdivisions': all_subdivisions,  # ALL 90 subdivisions
    'total_meters': 163217,
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 12, 31),
    'reading_freq_minutes': 60,
    'quality_issues_pct': 5.0,
    'enable_peak_times': True,
    'enable_seasonality': True
}

print("Configuration Summary:")
generator.show_configuration_summary()

confirm = input("\n⚠️  This will generate ~1.4 billion readings. Proceed? (Y/n): ").strip().lower()
if confirm and confirm != 'y':
    print("Cancelled.")
    sys.exit(0)

print("\n" + "="*70)
print("STARTING GENERATION")
print("="*70)
print("\nThis will take approximately 2-4 hours.")
print("Progress will be shown for each step.")
print("You can monitor the output folder: ./power_sector_data/")
print("="*70 + "\n")

try:
    generator.generate_all()
    
    duration = time.time() - start_time
    hours = duration / 3600
    
    print("\n" + "="*70)
    print("✓ GENERATION COMPLETE!")
    print("="*70)
    print(f"Duration: {duration:.1f} seconds ({hours:.2f} hours)")
    print(f"Meters: {len(generator.meters_df):,}")
    print(f"Customers: {len(generator.customers_df):,}")
    print(f"Readings: {len(generator.all_readings):,}")
    print(f"Bills: {len(generator.all_bills):,}")
    print(f"\nDivisions: {len(generator.config['divisions'])}")
    print(f"Subdivisions: {len(generator.config['subdivisions'])}")
    print("\n✓ All divisions and subdivisions from constants included!")
    print("✓ Memory-optimized chunked processing")
    print("✓ All realism features applied")
    print("\nOutput: ./power_sector_data/")
    print("="*70)
    
except KeyboardInterrupt:
    print("\n\n" + "="*70)
    print("⚠️  GENERATION INTERRUPTED")
    print("="*70)
    print("The generation was stopped by user (Ctrl+C)")
    print("Partial data may have been saved.")
    print("="*70)
    
except Exception as e:
    print(f"\n\n" + "="*70)
    print("✗ ERROR OCCURRED")
    print("="*70)
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    print("="*70)

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print("Configuration used:")
print(f"  • Divisions: {', '.join(generator.config['divisions'])}")
print(f"  • Subdivisions: {len(generator.config['subdivisions'])}")
print(f"  • Meters: {generator.config['total_meters']:,}")
print(f"  • Date range: {generator.config['start_date'].date()} to {generator.config['end_date'].date()}")
print(f"  • Reading frequency: {generator.config['reading_freq_minutes']} minutes")
print("="*70)
