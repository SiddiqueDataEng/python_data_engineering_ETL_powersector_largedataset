"""
Demo: Ultimate Generator with Realistic Data
"""

from datagenerator_ultimate import UltimateDataGenerator

print("\n" + "="*70)
print("ULTIMATE IESCO DATA GENERATOR - DEMO")
print("="*70)
print("\nThis demo will generate a small realistic dataset with:")
print("  ✓ All divisions and subdivisions")
print("  ✓ Configurable date range and reading frequency")
print("  ✓ Peak time patterns and seasonal variations")
print("  ✓ Data quality issues (2-8%)")
print("  ✓ Realistic billing with slab-based calculations")
print("  ✓ Multi-format output (SQLite, Parquet, JSON, CSV, TSV)")
print("\nYou'll be prompted for all parameters.")
print("="*70 + "\n")

generator = UltimateDataGenerator()
generator.prompt_configuration()
generator.generate_all()

print("\n✓ Demo complete! Check ./power_sector_data/ folder")
