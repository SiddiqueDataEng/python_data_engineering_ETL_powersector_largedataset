# IESCO Power Sector Data Generator

## Overview

A production-ready data generator for IESCO (Islamabad Electric Supply Company) power sector with **interactive prompts**, **resume capability**, **progress tracking**, and **multi-format storage**.

## 🎯 Key Features

✅ **Interactive User Prompts** - Guides you through configuration  
✅ **Resume Capability** - Automatically resumes from interruptions  
✅ **Progress Tracking** - Real-time progress with time estimation  
✅ **Multi-Format Storage** - SQLite, JSON, Parquet, CSV, TSV  
✅ **Pakistani Data** - 400+ names, 200+ areas, 100+ feeders  
✅ **IESCO Tariffs** - Accurate slab-based billing  
✅ **Organized Structure** - Hierarchical folder organization  
✅ **Monthly Billing** - Automatic bill generation per cycle  

## 🚀 Quick Start

### Installation

```bash
pip install pandas numpy python-dateutil pyarrow
```

### Run the Generator

```bash
python datagenerator_powersector_enhanced.py
```

**Just press Enter for all prompts to use defaults!**

### What You'll See

```
======================================================================
GENERATION PARAMETERS - Please provide the following:
======================================================================

Available Districts: ISLAMABAD, RAWALPINDI, ATTOCK, JHELUM, CHAKWAL

Enter divisions (comma-separated) [ISLAMABAD,RAWALPINDI]: 
Subdivisions per division [2]: 
Meters per subdivision [10]: 
Start date [2024-01-01]: 
End date [2024-03-31]: 

----------------------------------------------------------------------
SUMMARY OF CONFIGURATION:
----------------------------------------------------------------------
  Divisions: ISLAMABAD, RAWALPINDI
  Total meters: 40
  Billing months: 3
  Total bills: 120
  Estimated time: ~4.8 seconds
----------------------------------------------------------------------

Proceed? (Y/n): 
```

## 📊 Output Structure

```
power_sector_data/
├── generation_config.json       # Your parameters
├── generation_progress.json     # Resume tracker
├── meters.db                     # SQLite database
├── transformers.json             # JSON file
├── feeders.json                  # JSON file
├── customers.parquet             # Parquet file (compressed)
└── DIVISION/
      └── SUBDIVISION/
            ├── readings/
            │     └── METER_NO/
            │           └── YYYY-MM.csv
            └── bills/
                  └── METER_NO/
                        └── YYYY-MM.txt
```

## 💡 Usage Examples

### Use Defaults (Fastest)

Press Enter for all prompts:
- **Result**: 40 meters, 3 months, 120 bills in ~5 seconds

### Custom Configuration

```
Divisions: ISLAMABAD,RAWALPINDI,ATTOCK
Subdivisions: 5
Meters: 100
Date range: 2024-01-01 to 2024-12-31
```
- **Result**: 1,500 meters, 12 months, 18,000 bills in ~5 minutes

### Resume After Interruption

```bash
# Just run again - automatically resumes
python datagenerator_powersector_enhanced.py
```

### Reset and Start Fresh

```bash
python datagenerator_powersector_enhanced.py --reset
```

## 📈 Performance

| Scale | Meters | Months | Time | Bills |
|-------|--------|--------|------|-------|
| Quick Test | 10 | 1 | 1 sec | 10 |
| Small | 40 | 3 | 5 sec | 120 |
| Medium | 400 | 12 | 5 min | 4,800 |
| Large | 4,000 | 12 | 50 min | 48,000 |

## 🔍 Verify Output

```bash
python verify_all_features.py
```

Shows:
- Configuration details
- Progress status
- Sample data
- File statistics

## 📚 Documentation

| File | Description |
|------|-------------|
| **INTERACTIVE_MODE_GUIDE.md** | Complete guide to prompts |
| **INTERACTIVE_DEMO.md** | Visual demo of prompts |
| **QUICK_START.md** | Quick reference |
| **README_ENHANCED_GENERATOR.md** | Full documentation |
| **PAKISTANI_DATA_INTEGRATION.md** | Data details |
| **COMPLETE_FEATURES.md** | Feature summary |

## 🎨 Sample Data

### Pakistani Customer Names
```
          name     father_name
  Abdul Kareem  Muhammad Aslam
        Maliha    Bashir Ahmed
        Kamran     Allah Ditta
 Farhan Abbasi     Yousaf Khan
```

### IESCO Slab-Based Billing
```
Consumption: 497 kWh
Energy Charges: Rs. 5,687.7
Meter Rent: Rs. 18.06
GST (17%): Rs. 970.98
Total: Rs. 6,676.74
```

## 🛠️ Advanced Usage

### Non-Interactive Mode

```python
from datagenerator_powersector_enhanced import PowerSectorDataGenerator

config = {
    'divisions': ['ISLAMABAD', 'RAWALPINDI'],
    'subdivisions_per_division': 2,
    'meters_per_subdivision': 10,
    'start_date': '2024-01-01',
    'end_date': '2024-03-31'
}

generator = PowerSectorDataGenerator(config=config)
generator.generate_all()
```

### Query Generated Data

```python
import pandas as pd
import sqlite3

# Meters (SQLite)
conn = sqlite3.connect('power_sector_data/meters.db')
meters = pd.read_sql('SELECT * FROM meters', conn)

# Customers (Parquet)
customers = pd.read_parquet('power_sector_data/customers.parquet')

# Readings (CSV)
readings = pd.read_csv('power_sector_data/ISLAMABAD/ISLAMABAD_SUB1/readings/000000000001/2024-01.csv')

# Bills (TSV)
bills = pd.read_csv('power_sector_data/ISLAMABAD/ISLAMABAD_SUB1/bills/000000000001/2024-01.txt', sep='\t')
```

## 🔧 Troubleshooting

### Generation Too Slow
- Reduce number of meters
- Reduce date range
- Use fewer divisions

### Out of Disk Space
- Estimate: ~1-2 MB per meter per year
- Use `--reset` to clean up
- Use external storage

### Resume Not Working
- Check `generation_progress.json` exists
- Verify `generation_config.json` is valid
- Use `--reset` to start fresh

## 📦 What's Included

### Core Files
- `datagenerator_powersector_enhanced.py` - Main generator
- `pakistani_data_constants.py` - Pakistani data (400+ names, 200+ areas)
- `test_enhanced_generator.py` - Non-interactive test
- `demo_resume_capability.py` - Resume demo
- `verify_all_features.py` - Verification script

### Data Generated
- **Meters**: SQLite database with all meter information
- **Transformers**: JSON file with transformer details
- **Feeders**: JSON file with feeder information
- **Customers**: Parquet file with customer data (compressed)
- **Readings**: CSV files per meter per month
- **Bills**: TSV files per meter per month

## 🌟 Features in Detail

### Interactive Prompts
- Clear descriptions for each parameter
- Sensible defaults (just press Enter)
- Configuration summary before generation
- Confirmation prompt
- Estimated time calculation

### Resume Capability
- Saves progress every 5 meters
- Handles Ctrl+C gracefully
- Automatic resume on next run
- No data loss on interruption

### Progress Tracking
- Real-time progress bar
- Percentage complete
- Elapsed time
- Remaining time estimate
- Updates every meter

### Multi-Format Storage
- **SQLite**: Fast queries, relational integrity
- **JSON**: Human-readable, easy to parse
- **Parquet**: Compressed, efficient for analytics
- **CSV**: Universal compatibility
- **TSV**: Clear separation, human-readable

## 🎯 Use Cases

1. **Testing & Development** - Generate test data for applications
2. **Analytics** - Practice data analysis on realistic data
3. **Machine Learning** - Train models on consumption patterns
4. **Billing Systems** - Test billing calculations
5. **Reporting** - Generate sample reports
6. **Load Forecasting** - Analyze consumption trends
7. **Customer Segmentation** - Study payment behaviors

## 📝 License

This is a data generation tool for educational and testing purposes.

## 🤝 Contributing

Feel free to enhance the generator with:
- Additional tariff categories
- More regional data
- Advanced consumption patterns
- Load shedding schedules
- Peak/off-peak rates

## 📞 Support

For issues or questions:
1. Check the documentation files
2. Run `python verify_all_features.py`
3. Review `generation_progress.json` for status
4. Use `--reset` to start fresh

## 🎉 Quick Commands

```bash
# Generate with prompts
python datagenerator_powersector_enhanced.py

# Reset and start fresh
python datagenerator_powersector_enhanced.py --reset

# Verify output
python verify_all_features.py

# Check progress
python demo_resume_capability.py

# Non-interactive test
python test_enhanced_generator.py
```

---

**Ready to generate realistic IESCO power sector data!** 🚀

Just run `python datagenerator_powersector_enhanced.py` and press Enter for all prompts!
