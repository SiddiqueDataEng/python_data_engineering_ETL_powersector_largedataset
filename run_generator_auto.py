"""
STREAMING VERSION - Auto-run with resume capability
Generates and saves data on-the-fly to avoid memory issues
"""

import sys
sys.path.insert(0, 'py')

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path
import time
import gc

from pakistani_data_constants import (
    PAKISTANI_NAMES, FATHER_NAMES, PAKISTANI_AREAS, SUB_DIVISIONS, 
    IESCO_TARIFF_DETAILS, PAYMENT_BEHAVIORS
)

print("\n" + "="*70)
print("STREAMING GENERATOR - Memory Optimized (AUTO-RUN)")
print("="*70)
print("\nGenerating data with meter-level resume capability")
print("="*70 + "\n")

# Configuration
TOTAL_METERS = 163217
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
READING_FREQ_MINUTES = 60
QUALITY_ISSUES_PCT = 5.0

# Get all subdivisions
all_subdivisions = []
for division, subdivs in SUB_DIVISIONS.items():
    for subdiv in subdivs:
        all_subdivisions.append((division, subdiv))

print(f"Configuration:")
print(f"  • Meters: {TOTAL_METERS:,}")
print(f"  • Divisions: {len(SUB_DIVISIONS)}")
print(f"  • Subdivisions: {len(all_subdivisions)}")
print(f"  • Date range: {START_DATE.date()} to {END_DATE.date()}")
print(f"  • Reading frequency: {READING_FREQ_MINUTES} minutes")
print(f"  • Quality issues: {QUALITY_ISSUES_PCT}%")

days = (END_DATE - START_DATE).days + 1
readings_per_day = 24 * 60 / READING_FREQ_MINUTES
total_readings = int(TOTAL_METERS * days * readings_per_day)

print(f"\n  • Total readings: {total_readings:,}")
print(f"  • Estimated size: ~{total_readings * 0.0001 / 1024:.1f} GB")
print(f"  • Estimated time: 2-4 hours")
print(f"\n✅ AUTO-PROCEEDING...\n")

# Use network path directly
base_dir = Path(r'\\counter2\E\iesco\power_sector_data')
base_dir.mkdir(exist_ok=True)

start_time = time.time()

print("\n" + "="*70)
print("[1/3] Generating meters and customers...")
print("="*70)

# Generate meter numbers
meter_numbers = np.random.randint(10000, 999999, TOTAL_METERS)
meter_numbers = np.unique(meter_numbers)
if len(meter_numbers) < TOTAL_METERS:
    additional = TOTAL_METERS - len(meter_numbers)
    meter_numbers = np.concatenate([meter_numbers, np.arange(1000000, 1000000 + additional)])
meter_numbers = meter_numbers[:TOTAL_METERS]

# Assign subdivisions randomly
subdiv_indices = np.random.randint(0, len(all_subdivisions), TOTAL_METERS)

# Generate meters in chunks
print("  Creating meters...")
meter_chunks = []
chunk_size = 50000

for i in range(0, TOTAL_METERS, chunk_size):
    chunk_end = min(i + chunk_size, TOTAL_METERS)
    chunk_meters = []
    
    for j in range(i, chunk_end):
        meter_no = meter_numbers[j]
        subdiv_idx = subdiv_indices[j]
        division, subdivision = all_subdivisions[subdiv_idx]
        
        chunk_meters.append({
            'meter_id': f'{meter_no:012d}',
            'division': division,
            'subdivision': subdivision,
            'area': random.choice(PAKISTANI_AREAS),
            'meter_type': random.choice(['Single Phase', 'Three Phase']),
            'tariff_code': random.choice(list(IESCO_TARIFF_DETAILS.keys())),
            'status': 'Active'
        })
    
    meter_chunks.append(pd.DataFrame(chunk_meters))
    print(f"    Progress: {chunk_end:,} / {TOTAL_METERS:,}")

meters_df = pd.concat(meter_chunks, ignore_index=True)
print(f"  ✓ Created {len(meters_df):,} meters")

# Save meters to SQLite
print("  Saving meters to SQLite...")
import sqlite3
conn = sqlite3.connect(base_dir / 'meters.db')
meters_df.to_sql('meters', conn, if_exists='replace', index=False)
conn.close()
print("  ✓ Meters saved")

# Generate customers
print("  Creating customers...")
customer_chunks = []

for i in range(0, TOTAL_METERS, chunk_size):
    chunk_end = min(i + chunk_size, TOTAL_METERS)
    chunk_count = chunk_end - i
    
    customers_chunk = {
        'customer_id': [f'CUST{j:08d}' for j in range(i + 1, chunk_end + 1)],
        'meter_id': meters_df['meter_id'].values[i:chunk_end],
        'name': np.random.choice(PAKISTANI_NAMES, chunk_count),
        'father_name': np.random.choice(FATHER_NAMES, chunk_count),
        'address': [f'House {j % 1000 + 1}, {PAKISTANI_AREAS[j % len(PAKISTANI_AREAS)]}' 
                   for j in range(i, chunk_end)],
        'phone': [f'03{random.randint(10,99)}{random.randint(1000000,9999999)}' 
                 for _ in range(chunk_count)],
        'cnic': [f'{random.randint(10000,99999)}-{random.randint(1000000,9999999)}-{random.randint(1,9)}' 
                for _ in range(chunk_count)],
        'payment_behavior': np.random.choice(
            list(PAYMENT_BEHAVIORS.keys()),
            chunk_count,
            p=[PAYMENT_BEHAVIORS[k]['prob'] for k in PAYMENT_BEHAVIORS.keys()]
        )
    }
    
    customer_chunks.append(pd.DataFrame(customers_chunk))
    print(f"    Progress: {chunk_end:,} / {TOTAL_METERS:,}")

customers_df = pd.concat(customer_chunks, ignore_index=True)
print(f"  ✓ Created {len(customers_df):,} customers")

# Save customers
print("  Saving customers to Parquet...")
customers_df.to_parquet(base_dir / 'customers.parquet', compression='snappy')
print("  ✓ Customers saved")

print("\n" + "="*70)
print("[2/3] Generating readings and bills (streaming)...")
print("="*70)
print("  Processing by subdivision with meter-level resume...")

# Group meters by subdivision FIRST
grouped = meters_df.groupby(['division', 'subdivision'])

# Build a set of already-processed meters by checking existing files
print("  Checking for existing data (meter-level)...")
processed_meter_ids = set()

for (division, subdivision), group in grouped:
    readings_file = base_dir / division / subdivision / 'readings' / 'readings.csv'
    bills_file = base_dir / division / subdivision / 'bills' / 'bills.tsv'
    
    # Check which meters are already in the files
    if readings_file.exists() and readings_file.stat().st_size > 1024:
        try:
            # Read last 100 lines to get meter IDs (much faster than reading entire file)
            with open(readings_file, 'r', encoding='utf-8') as f:
                # Seek to near end of file
                f.seek(max(0, readings_file.stat().st_size - 10000))
                lines = f.readlines()
                
                # Extract meter IDs from last lines
                for line in lines[-100:]:
                    parts = line.strip().split(',')
                    if len(parts) > 1 and parts[1].startswith('0'):  # meter_id is 2nd column
                        processed_meter_ids.add(parts[1])
        except:
            pass

existing_meters = len(processed_meter_ids)

if existing_meters > 0:
    print(f"\n  ✓ Found existing data:")
    print(f"    - {existing_meters:,} meters already completed (~{existing_meters/TOTAL_METERS*100:.1f}%)")
    print(f"    - {TOTAL_METERS - existing_meters:,} meters remaining (~{(TOTAL_METERS-existing_meters)/TOTAL_METERS*100:.1f}%)")
    print(f"    - Will skip completed meters and continue\n")
else:
    print(f"  • Starting fresh generation\n")

processed_meters = 0
total_readings_generated = 0
total_bills_generated = 0

for (division, subdivision), group in grouped:
    subdiv_start = time.time()
    
    # Create folders
    subdiv_path = base_dir / division / subdivision
    readings_path = subdiv_path / 'readings'
    bills_path = subdiv_path / 'bills'
    readings_path.mkdir(parents=True, exist_ok=True)
    bills_path.mkdir(parents=True, exist_ok=True)
    
    meters_count = len(group)
    
    # Open files for streaming write
    readings_file = readings_path / 'readings.csv'
    bills_file = bills_path / 'bills.tsv'
    
    # Check if files exist to determine append mode
    readings_written = readings_file.exists() and readings_file.stat().st_size > 0
    bills_written = bills_file.exists() and bills_file.stat().st_size > 0
    
    # Count meters to process in this subdivision
    meters_to_process = [m for _, m in group.iterrows() if m['meter_id'] not in processed_meter_ids]
    meters_skipped = len(group) - len(meters_to_process)
    
    if len(meters_to_process) == 0:
        # All meters in this subdivision already processed
        processed_meters += meters_count
        print(f"    ✓ SKIPPED [{processed_meters:,}/{TOTAL_METERS:,}] "
              f"{division}/{subdivision} ({meters_count} meters) - All completed")
        continue
    
    if meters_skipped > 0:
        print(f"    🔄 RESUMING [{processed_meters + meters_skipped:,}/{TOTAL_METERS:,}] "
              f"{division}/{subdivision} ({len(meters_to_process)}/{meters_count} meters remaining)")
        processed_meters += meters_skipped
    
    # Process each meter (only those not already processed)
    for _, meter_row in enumerate(meters_to_process):
        meter_id = meter_row['meter_id']
        tariff_code = meter_row['tariff_code']
        
        # Generate readings for this meter (streaming)
        meter_readings = []
        current_reading = random.randint(1000, 5000)
        current_date = START_DATE
        
        while current_date <= END_DATE:
            for reading_num in range(int(24 * 60 / READING_FREQ_MINUTES)):
                timestamp = current_date + timedelta(minutes=reading_num * READING_FREQ_MINUTES)
                
                # Base consumption
                if 'Residential' in IESCO_TARIFF_DETAILS[tariff_code]['name']:
                    consumption = random.uniform(0.3, 1.5)
                elif 'Commercial' in IESCO_TARIFF_DETAILS[tariff_code]['name']:
                    consumption = random.uniform(1.0, 3.0)
                else:
                    consumption = random.uniform(2.0, 5.0)
                
                # Peak times
                hour = timestamp.hour
                if 7 <= hour <= 10 or 18 <= hour <= 23:
                    consumption *= random.uniform(1.3, 1.8)
                elif 0 <= hour <= 5:
                    consumption *= random.uniform(0.4, 0.7)
                
                # Seasonality
                month = timestamp.month
                if month in [6, 7, 8]:
                    consumption *= random.uniform(1.5, 2.0)
                elif month in [12, 1, 2]:
                    consumption *= random.uniform(1.2, 1.5)
                
                consumption *= random.uniform(0.8, 1.2)
                current_reading += consumption
                
                # Quality issues
                has_issue = random.random() < (QUALITY_ISSUES_PCT / 100)
                if has_issue:
                    issue_type = random.choice(['missing', 'spike', 'zero'])
                    if issue_type == 'missing':
                        continue
                    elif issue_type == 'spike':
                        consumption *= random.uniform(5, 10)
                    elif issue_type == 'zero':
                        consumption = 0
                
                voltage = random.uniform(220, 240)
                current_amp = consumption / (voltage / 1000) if voltage > 0 else 0
                
                meter_readings.append({
                    'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'meter_id': meter_id,
                    'reading': round(current_reading, 2),
                    'consumption_kwh': round(consumption, 3),
                    'voltage': round(voltage, 2),
                    'current_amp': round(current_amp, 2),
                    'quality_flag': 'ISSUE' if has_issue else 'OK'
                })
            
            current_date += timedelta(days=1)
        
        # Write readings to file (append mode) - optimized for memory
        if meter_readings:
            readings_df = pd.DataFrame(meter_readings)
            # Optimize dtypes to reduce memory
            readings_df['meter_id'] = readings_df['meter_id'].astype('string')
            readings_df['quality_flag'] = readings_df['quality_flag'].astype('category')
            # Write in smaller chunks to avoid memory issues
            readings_df.to_csv(readings_file, mode='a', header=not readings_written, index=False, chunksize=5000)
            readings_written = True
            total_readings_generated += len(meter_readings)
            # Clear memory
            del readings_df
            meter_readings.clear()
        
        # Generate monthly bills
        meter_bills = []
        for month in range(1, 13):
            month_consumption = random.randint(150, 600)
            
            # Calculate charges (simplified)
            energy_charges = month_consumption * 15.0
            tariff = IESCO_TARIFF_DETAILS[tariff_code]
            meter_rent = tariff['meter_rent']
            gst = (energy_charges + meter_rent) * 0.17
            total_amount = energy_charges + meter_rent + gst
            
            meter_bills.append({
                'bill_id': f'BILL{meter_id}{month:02d}',
                'meter_id': meter_id,
                'billing_month': f'2024-{month:02d}',
                'consumption_kwh': month_consumption,
                'energy_charges': round(energy_charges, 2),
                'meter_rent': meter_rent,
                'gst': round(gst, 2),
                'total_amount': round(total_amount, 2),
                'status': random.choice(['Paid', 'Unpaid', 'Late'])
            })
        
        # Write bills to file (append mode) - optimized for memory
        if meter_bills:
            bills_df = pd.DataFrame(meter_bills)
            # Optimize dtypes
            bills_df['meter_id'] = bills_df['meter_id'].astype('string')
            bills_df['bill_id'] = bills_df['bill_id'].astype('string')
            bills_df['status'] = bills_df['status'].astype('category')
            # Write with chunksize
            bills_df.to_csv(bills_file, sep='\t', mode='a', header=not bills_written, index=False, chunksize=1000)
            bills_written = True
            total_bills_generated += len(meter_bills)
            # Clear memory
            del bills_df
            meter_bills.clear()
    
    processed_meters += meters_count
    elapsed = time.time() - start_time
    rate = processed_meters / elapsed if elapsed > 0 else 0
    remaining = (TOTAL_METERS - processed_meters) / rate if rate > 0 else 0
    pct = (processed_meters / TOTAL_METERS) * 100
    subdiv_time = time.time() - subdiv_start
    
    # Force garbage collection after each subdivision
    gc.collect()
    
    print(f"    [{processed_meters:,}/{TOTAL_METERS:,}] {pct:.1f}% | "
          f"{division}/{subdivision} ({meters_count} meters) | "
          f"Subdiv: {subdiv_time:.1f}s | Total: {elapsed/60:.1f}min | "
          f"Remaining: {remaining/60:.1f}min | Rate: {rate:.0f} m/s")

duration = time.time() - start_time

print("\n" + "="*70)
print("✓ GENERATION COMPLETE!")
print("="*70)
print(f"Duration: {duration:.1f} seconds ({duration/3600:.2f} hours)")
print(f"Total meters: {TOTAL_METERS:,}")
print(f"  • Already completed: {existing_meters:,}")
print(f"  • Newly generated: {processed_meters - existing_meters:,}")
print(f"Readings generated: {total_readings_generated:,}")
print(f"Bills generated: {total_bills_generated:,}")
print(f"\nOutput: {base_dir.absolute()}")
print("="*70)
