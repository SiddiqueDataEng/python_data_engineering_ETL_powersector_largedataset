"""
Streamlit ETL Dashboard for Power Sector Data
- Run ETL pipelines
- Monitor progress
- View statistics
- EDA of source and processed data
- Schedule automated runs
"""

import streamlit as st
import pandas as pd
import dask.dataframe as dd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
from datetime import datetime, timedelta
import time
import threading
from etl_pipeline import PowerSectorETL
import os

# Page configuration
st.set_page_config(
    page_title="Power Sector ETL Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'etl' not in st.session_state:
    st.session_state.etl = PowerSectorETL(
        source_dir=r"\\counter2\E\iesco\power_sector_data",
        target_dir="power_sector_data_silver_layer"
    )

if 'processing' not in st.session_state:
    st.session_state.processing = False

if 'last_run' not in st.session_state:
    st.session_state.last_run = None


def get_source_statistics():
    """Get statistics from source data"""
    source_path = Path(r"\\counter2\E\iesco\power_sector_data")
    
    stats = {
        'divisions': [],
        'subdivisions': [],
        'bills_files': [],
        'readings_files': [],
        'total_size': 0
    }
    
    for division_dir in source_path.iterdir():
        if not division_dir.is_dir() or division_dir.name.startswith('.'):
            continue
        
        stats['divisions'].append(division_dir.name)
        
        for subdivision_dir in division_dir.iterdir():
            if not subdivision_dir.is_dir():
                continue
            
            stats['subdivisions'].append(f"{division_dir.name}/{subdivision_dir.name}")
            
            # Check bills
            bills_file = subdivision_dir / "bills" / "bills.tsv"
            if bills_file.exists():
                stats['bills_files'].append(str(bills_file))
                stats['total_size'] += bills_file.stat().st_size
            
            # Check readings
            readings_file = subdivision_dir / "readings" / "readings.csv"
            if readings_file.exists():
                stats['readings_files'].append(str(readings_file))
                stats['total_size'] += readings_file.stat().st_size
    
    return stats


def get_processed_statistics():
    """Get statistics from processed data"""
    target_path = Path("power_sector_data_silver_layer")
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    stats = {}
    
    if bills_path.exists():
        bills_df = dd.read_parquet(bills_path)
        stats['bills'] = {
            'records': len(bills_df),
            'size_mb': bills_path.stat().st_size / (1024 * 1024),
            'divisions': bills_df['division'].nunique().compute(),
            'subdivisions': bills_df['subdivision'].nunique().compute(),
            'meters': bills_df['meter_id'].nunique().compute()
        }
    
    if readings_path.exists():
        readings_df = dd.read_parquet(readings_path)
        stats['readings'] = {
            'records': len(readings_df),
            'size_mb': readings_path.stat().st_size / (1024 * 1024),
            'divisions': readings_df['division'].nunique().compute(),
            'subdivisions': readings_df['subdivision'].nunique().compute(),
            'meters': readings_df['meter_id'].nunique().compute()
        }
    
    return stats


def run_etl_pipeline(mode='full'):
    """Run ETL pipeline in background"""
    st.session_state.processing = True
    
    try:
        if mode == 'full':
            st.session_state.etl.run_full_load()
        else:
            st.session_state.etl.run_incremental_load()
        
        st.session_state.last_run = datetime.now()
        st.success(f"✅ {mode.title()} load completed successfully!")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
    finally:
        st.session_state.processing = False


# Sidebar navigation
st.sidebar.title("⚡ ETL Dashboard")
page = st.sidebar.radio(
    "Navigation",
    ["🏠 Home", "📊 Source EDA", "📈 Processed EDA", "🚀 Run ETL", "📅 Scheduler", "✅ Validation"]
)

# Main content
if page == "🏠 Home":
    st.markdown('<div class="main-header">⚡ Power Sector ETL Dashboard</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    source_stats = get_source_statistics()
    
    with col1:
        st.metric("Divisions", len(source_stats['divisions']))
    
    with col2:
        st.metric("Subdivisions", len(source_stats['subdivisions']))
    
    with col3:
        st.metric("Source Files", len(source_stats['bills_files']) + len(source_stats['readings_files']))
    
    with col4:
        st.metric("Source Size", f"{source_stats['total_size'] / (1024**3):.2f} GB")
    
    st.markdown("---")
    
    # Processing status
    st.subheader("📊 Processing Status")
    
    target_path = Path("power_sector_data_silver_layer")
    bills_exists = (target_path / "bills.parquet").exists()
    readings_exists = (target_path / "readings.parquet").exists()
    
    col1, col2 = st.columns(2)
    
    with col1:
        if bills_exists:
            st.markdown('<div class="success-box">✅ Bills Processed</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="warning-box">⚠️ Bills Not Processed</div>', unsafe_allow_html=True)
    
    with col2:
        if readings_exists:
            st.markdown('<div class="success-box">✅ Readings Processed</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="warning-box">⚠️ Readings Not Processed</div>', unsafe_allow_html=True)
    
    if bills_exists or readings_exists:
        st.markdown("---")
        st.subheader("📈 Processed Data Statistics")
        
        processed_stats = get_processed_statistics()
        
        if 'bills' in processed_stats:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Bills Records", f"{processed_stats['bills']['records']:,}")
            with col2:
                st.metric("Bills Size", f"{processed_stats['bills']['size_mb']:.2f} MB")
            with col3:
                compression = (1 - processed_stats['bills']['size_mb'] / (source_stats['total_size'] / (1024**2) / 2)) * 100
                st.metric("Compression", f"{compression:.1f}%")
        
        if 'readings' in processed_stats:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Readings Records", f"{processed_stats['readings']['records']:,}")
            with col2:
                st.metric("Readings Size", f"{processed_stats['readings']['size_mb']:.2f} MB")
            with col3:
                compression = (1 - processed_stats['readings']['size_mb'] / (source_stats['total_size'] / (1024**2) / 2)) * 100
                st.metric("Compression", f"{compression:.1f}%")
    
    # Last run info
    if st.session_state.last_run:
        st.markdown("---")
        st.info(f"🕐 Last run: {st.session_state.last_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Quick actions
    st.markdown("---")
    st.subheader("⚡ Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Run Full Load", use_container_width=True, disabled=st.session_state.processing):
            with st.spinner("Processing..."):
                run_etl_pipeline('full')
                st.rerun()
    
    with col2:
        if st.button("🔄 Run Incremental", use_container_width=True, disabled=st.session_state.processing):
            with st.spinner("Processing..."):
                run_etl_pipeline('incremental')
                st.rerun()
    
    with col3:
        if st.button("🔍 Validate Data", use_container_width=True):
            st.switch_page = "✅ Validation"

elif page == "📊 Source EDA":
    st.title("📊 Source Data Exploratory Analysis")
    
    source_stats = get_source_statistics()
    
    # Division distribution
    st.subheader("Division Distribution")
    
    division_counts = {}
    for subdivision in source_stats['subdivisions']:
        division = subdivision.split('/')[0]
        division_counts[division] = division_counts.get(division, 0) + 1
    
    fig = px.bar(
        x=list(division_counts.keys()),
        y=list(division_counts.values()),
        labels={'x': 'Division', 'y': 'Number of Subdivisions'},
        title='Subdivisions per Division'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # File size analysis
    st.subheader("File Size Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Bills Files", len(source_stats['bills_files']))
        st.metric("Total Readings Files", len(source_stats['readings_files']))
    
    with col2:
        st.metric("Total Source Size", f"{source_stats['total_size'] / (1024**3):.2f} GB")
        st.metric("Average File Size", f"{source_stats['total_size'] / len(source_stats['bills_files'] + source_stats['readings_files']) / (1024**2):.2f} MB")
    
    # Sample data preview
    st.subheader("Sample Data Preview")
    
    tab1, tab2 = st.tabs(["Bills Sample", "Readings Sample"])
    
    with tab1:
        if source_stats['bills_files']:
            sample_file = source_stats['bills_files'][0]
            try:
                df = pd.read_csv(sample_file, sep='\t', nrows=100)
                st.dataframe(df.head(10), use_container_width=True)
                
                st.write("**Schema:**")
                st.write(df.dtypes)
            except Exception as e:
                st.error(f"Error reading file: {e}")
    
    with tab2:
        if source_stats['readings_files']:
            sample_file = source_stats['readings_files'][0]
            try:
                df = pd.read_csv(sample_file, nrows=100)
                st.dataframe(df.head(10), use_container_width=True)
                
                st.write("**Schema:**")
                st.write(df.dtypes)
            except Exception as e:
                st.error(f"Error reading file: {e}")

elif page == "📈 Processed EDA":
    st.title("📈 Processed Data Exploratory Analysis")
    
    target_path = Path("power_sector_data_silver_layer")
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    if not bills_path.exists() and not readings_path.exists():
        st.warning("⚠️ No processed data found. Please run ETL pipeline first.")
        if st.button("🚀 Run Full Load Now"):
            with st.spinner("Processing..."):
                run_etl_pipeline('full')
                st.rerun()
    else:
        tab1, tab2 = st.tabs(["📄 Bills Analysis", "📊 Readings Analysis"])
        
        with tab1:
            if bills_path.exists():
                st.subheader("Bills Data Analysis")
                
                bills_df = dd.read_parquet(bills_path)
                
                # Basic stats
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Records", f"{len(bills_df):,}")
                with col2:
                    st.metric("Unique Meters", f"{bills_df['meter_id'].nunique().compute():,}")
                with col3:
                    st.metric("Divisions", bills_df['division'].nunique().compute())
                with col4:
                    st.metric("Subdivisions", bills_df['subdivision'].nunique().compute())
                
                # Sample data
                st.write("**Sample Data:**")
                st.dataframe(bills_df.head(10).compute(), use_container_width=True)
                
                # Revenue by division
                st.subheader("Revenue by Division")
                revenue_by_div = bills_df.groupby('division')['total_amount'].sum().compute().sort_values(ascending=False)
                
                fig = px.bar(
                    x=revenue_by_div.index,
                    y=revenue_by_div.values,
                    labels={'x': 'Division', 'y': 'Total Revenue'},
                    title='Total Revenue by Division'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Consumption distribution
                st.subheader("Consumption Distribution")
                sample = bills_df.sample(frac=0.1).compute()
                
                fig = px.histogram(
                    sample,
                    x='consumption_kwh',
                    nbins=50,
                    title='Consumption Distribution (kWh)'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Payment status
                st.subheader("Payment Status")
                status_counts = bills_df['status'].value_counts().compute()
                
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title='Payment Status Distribution'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            if readings_path.exists():
                st.subheader("Readings Data Analysis")
                
                readings_df = dd.read_parquet(readings_path)
                
                # Basic stats
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Records", f"{len(readings_df):,}")
                with col2:
                    st.metric("Unique Meters", f"{readings_df['meter_id'].nunique().compute():,}")
                with col3:
                    st.metric("Divisions", readings_df['division'].nunique().compute())
                with col4:
                    st.metric("Subdivisions", readings_df['subdivision'].nunique().compute())
                
                # Sample data
                st.write("**Sample Data:**")
                st.dataframe(readings_df.head(10).compute(), use_container_width=True)
                
                # Voltage analysis
                st.subheader("Voltage Quality Analysis")
                sample = readings_df.sample(frac=0.05).compute()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.histogram(
                        sample,
                        x='voltage',
                        nbins=50,
                        title='Voltage Distribution'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    fig = px.box(
                        sample,
                        y='voltage',
                        title='Voltage Box Plot'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Quality flag distribution
                st.subheader("Data Quality Flags")
                quality_counts = readings_df['quality_flag'].value_counts().compute()
                
                fig = px.pie(
                    values=quality_counts.values,
                    names=quality_counts.index,
                    title='Quality Flag Distribution'
                )
                st.plotly_chart(fig, use_container_width=True)

elif page == "🚀 Run ETL":
    st.title("🚀 Run ETL Pipeline")
    
    st.markdown("---")
    
    # ETL mode selection
    mode = st.radio(
        "Select ETL Mode",
        ["Full Load", "Incremental Load"],
        help="Full Load: Process all files | Incremental: Process only new/changed files"
    )
    
    # Compression selection
    compression = st.selectbox(
        "Compression Algorithm",
        ["snappy", "gzip", "brotli"],
        help="Snappy: Fast | Gzip: Balanced | Brotli: Best compression"
    )
    
    st.markdown("---")
    
    if st.session_state.processing:
        st.warning("⏳ Processing in progress...")
        st.spinner("Please wait...")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("▶️ Start Processing", use_container_width=True, type="primary"):
                st.session_state.processing = True
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    status_text.text("🔍 Discovering files...")
                    progress_bar.progress(20)
                    
                    if mode == "Full Load":
                        status_text.text("📊 Processing bills...")
                        progress_bar.progress(40)
                        st.session_state.etl.process_bills(incremental=False, compression=compression)
                        
                        status_text.text("📈 Processing readings...")
                        progress_bar.progress(70)
                        st.session_state.etl.process_readings(incremental=False, compression=compression)
                    else:
                        status_text.text("🔄 Processing incremental bills...")
                        progress_bar.progress(40)
                        st.session_state.etl.process_bills(incremental=True, compression=compression)
                        
                        status_text.text("🔄 Processing incremental readings...")
                        progress_bar.progress(70)
                        st.session_state.etl.process_readings(incremental=True, compression=compression)
                    
                    progress_bar.progress(100)
                    status_text.text("✅ Complete!")
                    
                    st.session_state.last_run = datetime.now()
                    st.success(f"✅ {mode} completed successfully!")
                    
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                finally:
                    st.session_state.processing = False
        
        with col2:
            if st.button("📊 View Statistics", use_container_width=True):
                stats = st.session_state.etl.get_statistics()
                st.json(stats)

elif page == "📅 Scheduler":
    st.title("📅 ETL Scheduler")
    
    st.markdown("Configure automated ETL runs")
    
    st.markdown("---")
    
    # Scheduler configuration
    col1, col2 = st.columns(2)
    
    with col1:
        schedule_enabled = st.checkbox("Enable Scheduler")
        interval_minutes = st.number_input("Interval (minutes)", min_value=5, max_value=1440, value=60)
    
    with col2:
        schedule_mode = st.selectbox("Mode", ["Incremental Load", "Full Load"])
        start_time = st.time_input("Start Time")
    
    if schedule_enabled:
        st.info(f"📅 Scheduler will run {schedule_mode} every {interval_minutes} minutes starting at {start_time}")
        
        if st.button("▶️ Start Scheduler"):
            st.success("✅ Scheduler started!")
            st.info("Note: Scheduler runs in background. Close this tab to stop.")
    
    st.markdown("---")
    
    # Manual command line
    st.subheader("💻 Command Line Usage")
    
    st.code(f"""
# Full load
python etl_pipeline.py --mode full --compression {compression}

# Incremental load
python etl_pipeline.py --mode incremental

# Automated scheduler
python etl_scheduler.py --interval {interval_minutes}

# Statistics
python etl_pipeline.py --mode stats
    """, language="bash")

elif page == "✅ Validation":
    st.title("✅ Data Validation")
    
    target_path = Path("power_sector_data_silver_layer")
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    if not bills_path.exists() and not readings_path.exists():
        st.warning("⚠️ No processed data found. Please run ETL pipeline first.")
    else:
        if st.button("🔍 Run Validation"):
            with st.spinner("Validating..."):
                # Bills validation
                if bills_path.exists():
                    st.subheader("📄 Bills Validation")
                    
                    bills_df = dd.read_parquet(bills_path)
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        duplicates = bills_df['bill_id'].duplicated().sum().compute()
                        if duplicates == 0:
                            st.success(f"✅ No duplicates")
                        else:
                            st.error(f"❌ {duplicates} duplicates found")
                    
                    with col2:
                        null_count = bills_df['consumption_kwh'].isnull().sum().compute()
                        if null_count == 0:
                            st.success(f"✅ No null consumption")
                        else:
                            st.warning(f"⚠️ {null_count} null values")
                    
                    with col3:
                        negative = (bills_df['total_amount'] <= 0).sum().compute()
                        if negative == 0:
                            st.success(f"✅ All amounts valid")
                        else:
                            st.error(f"❌ {negative} invalid amounts")
                
                # Readings validation
                if readings_path.exists():
                    st.subheader("📊 Readings Validation")
                    
                    readings_df = dd.read_parquet(readings_path)
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        duplicates = readings_df.duplicated(subset=['timestamp', 'meter_id']).sum().compute()
                        if duplicates == 0:
                            st.success(f"✅ No duplicates")
                        else:
                            st.error(f"❌ {duplicates} duplicates found")
                    
                    with col2:
                        sample = readings_df.sample(frac=0.1).compute()
                        voltage_issues = ((sample['voltage'] < 180) | (sample['voltage'] > 260)).sum()
                        if voltage_issues == 0:
                            st.success(f"✅ Voltage in range")
                        else:
                            st.warning(f"⚠️ {voltage_issues} voltage issues")
                    
                    with col3:
                        negative = (readings_df['consumption_kwh'] < 0).sum().compute()
                        if negative == 0:
                            st.success(f"✅ All consumption valid")
                        else:
                            st.error(f"❌ {negative} negative values")

# Footer
st.sidebar.markdown("---")
st.sidebar.info("""
**Power Sector ETL Dashboard**  
Version 1.0  
© 2026
""")
