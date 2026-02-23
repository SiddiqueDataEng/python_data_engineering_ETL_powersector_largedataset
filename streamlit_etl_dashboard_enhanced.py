"""
Enhanced Streamlit ETL Dashboard for Power Sector Data
- Configurable source paths
- Comprehensive source data exploration
- Data quality analysis
- Metadata and schema inspection
- Interactive visualizations
"""

import streamlit as st
import pandas as pd
import dask.dataframe as dd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
from datetime import datetime
import time
import os
import numpy as np
from etl_pipeline import PowerSectorETL

# Page configuration
st.set_page_config(
    page_title="Power Sector ETL Dashboard - Enhanced",
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
        background: linear-gradient(90deg, #1f77b4 0%, #2ca02c 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
        margin: 0.5rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
        margin: 0.5rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
        margin: 0.5rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #17a2b8;
        margin: 0.5rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'source_path' not in st.session_state:
    st.session_state.source_path = r"\\counter2\E\iesco\power_sector_data"

if 'target_path' not in st.session_state:
    st.session_state.target_path = "power_sector_data_silver_layer"

if 'etl' not in st.session_state:
    st.session_state.etl = None

if 'processing' not in st.session_state:
    st.session_state.processing = False

if 'last_run' not in st.session_state:
    st.session_state.last_run = None

if 'source_cache' not in st.session_state:
    st.session_state.source_cache = {}


@st.cache_data(ttl=300)
def load_sample_data(file_path, file_type='csv', nrows=1000):
    """Load sample data from file"""
    try:
        if file_type == 'tsv':
            return pd.read_csv(file_path, sep='\t', nrows=nrows)
        else:
            return pd.read_csv(file_path, nrows=nrows)
    except Exception as e:
        st.error(f"Error loading file: {e}")
        return None


def analyze_data_quality(df, data_type='bills'):
    """Comprehensive data quality analysis"""
    quality_report = {
        'total_records': len(df),
        'total_columns': len(df.columns),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024**2),
        'duplicates': {},
        'missing_values': {},
        'data_types': {},
        'value_ranges': {},
        'outliers': {},
        'issues': []
    }
    
    # Duplicate analysis
    if data_type == 'bills':
        quality_report['duplicates']['bill_id'] = df['bill_id'].duplicated().sum()
    else:
        quality_report['duplicates']['timestamp_meter'] = df.duplicated(
            subset=['timestamp', 'meter_id']
        ).sum()
    
    # Missing values
    for col in df.columns:
        missing = df[col].isnull().sum()
        if missing > 0:
            quality_report['missing_values'][col] = {
                'count': int(missing),
                'percentage': float(missing / len(df) * 100)
            }
    
    # Data types
    quality_report['data_types'] = df.dtypes.astype(str).to_dict()
    
    # Numeric columns analysis
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        quality_report['value_ranges'][col] = {
            'min': float(df[col].min()),
            'max': float(df[col].max()),
            'mean': float(df[col].mean()),
            'median': float(df[col].median()),
            'std': float(df[col].std())
        }
        
        # Outlier detection using IQR
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
        if outliers > 0:
            quality_report['outliers'][col] = int(outliers)
    
    # Specific quality checks
    if data_type == 'bills':
        # Check for negative amounts
        if 'total_amount' in df.columns:
            neg_amounts = (df['total_amount'] <= 0).sum()
            if neg_amounts > 0:
                quality_report['issues'].append(
                    f"⚠️ {neg_amounts} records with invalid total_amount (≤0)"
                )
        
        # Check for negative consumption
        if 'consumption_kwh' in df.columns:
            neg_consumption = (df['consumption_kwh'] < 0).sum()
            if neg_consumption > 0:
                quality_report['issues'].append(
                    f"⚠️ {neg_consumption} records with negative consumption"
                )
    
    else:  # readings
        # Check voltage range
        if 'voltage' in df.columns:
            voltage_issues = ((df['voltage'] < 180) | (df['voltage'] > 260)).sum()
            if voltage_issues > 0:
                quality_report['issues'].append(
                    f"⚠️ {voltage_issues} readings with voltage out of range (180-260V)"
                )
        
        # Check for negative consumption
        if 'consumption_kwh' in df.columns:
            neg_consumption = (df['consumption_kwh'] < 0).sum()
            if neg_consumption > 0:
                quality_report['issues'].append(
                    f"⚠️ {neg_consumption} readings with negative consumption"
                )
    
    return quality_report


def get_file_metadata(file_path):
    """Get file metadata"""
    try:
        stat = file_path.stat()
        return {
            'size_bytes': stat.st_size,
            'size_mb': stat.st_size / (1024**2),
            'size_gb': stat.st_size / (1024**3),
            'created': datetime.fromtimestamp(stat.st_ctime),
            'modified': datetime.fromtimestamp(stat.st_mtime),
            'accessed': datetime.fromtimestamp(stat.st_atime)
        }
    except Exception as e:
        return {'error': str(e)}


def discover_source_structure(source_path):
    """Discover and analyze source directory structure"""
    source_path = Path(source_path)
    
    structure = {
        'divisions': [],
        'subdivisions': [],
        'bills_files': [],
        'readings_files': [],
        'total_size': 0,
        'file_count': 0,
        'errors': []
    }
    
    if not source_path.exists():
        structure['errors'].append(f"Path does not exist: {source_path}")
        return structure
    
    try:
        for division_dir in source_path.iterdir():
            if not division_dir.is_dir() or division_dir.name.startswith('.'):
                continue
            
            structure['divisions'].append(division_dir.name)
            
            for subdivision_dir in division_dir.iterdir():
                if not subdivision_dir.is_dir():
                    continue
                
                subdivision_name = f"{division_dir.name}/{subdivision_dir.name}"
                structure['subdivisions'].append(subdivision_name)
                
                # Check bills
                bills_file = subdivision_dir / "bills" / "bills.tsv"
                if bills_file.exists():
                    structure['bills_files'].append({
                        'path': str(bills_file),
                        'division': division_dir.name,
                        'subdivision': subdivision_dir.name,
                        'size': bills_file.stat().st_size
                    })
                    structure['total_size'] += bills_file.stat().st_size
                    structure['file_count'] += 1
                
                # Check readings
                readings_file = subdivision_dir / "readings" / "readings.csv"
                if readings_file.exists():
                    structure['readings_files'].append({
                        'path': str(readings_file),
                        'division': division_dir.name,
                        'subdivision': subdivision_dir.name,
                        'size': readings_file.stat().st_size
                    })
                    structure['total_size'] += readings_file.stat().st_size
                    structure['file_count'] += 1
    
    except Exception as e:
        structure['errors'].append(f"Error scanning directory: {e}")
    
    return structure


# Sidebar - Configuration
st.sidebar.title("⚡ ETL Dashboard")
st.sidebar.markdown("---")

# Path Configuration
with st.sidebar.expander("⚙️ Configuration", expanded=False):
    st.subheader("Data Paths")
    
    new_source = st.text_input(
        "Source Path",
        value=st.session_state.source_path,
        help="Path to source data directory"
    )
    
    new_target = st.text_input(
        "Target Path",
        value=st.session_state.target_path,
        help="Path to target directory for processed data"
    )
    
    if st.button("💾 Save Configuration"):
        st.session_state.source_path = new_source
        st.session_state.target_path = new_target
        st.session_state.etl = PowerSectorETL(
            source_dir=new_source,
            target_dir=new_target
        )
        st.success("✅ Configuration saved!")
        st.rerun()

# Initialize ETL if not done
if st.session_state.etl is None:
    st.session_state.etl = PowerSectorETL(
        source_dir=st.session_state.source_path,
        target_dir=st.session_state.target_path
    )

# Navigation
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigation",
    [
        "🏠 Home",
        "🔍 Source Explorer",
        "📊 Data Quality",
        "📈 Processed EDA",
        "🚀 Run ETL",
        "📅 Scheduler",
        "✅ Validation"
    ]
)

# Display current paths
st.sidebar.markdown("---")
st.sidebar.markdown("**Current Configuration:**")
st.sidebar.code(f"Source: {st.session_state.source_path}", language="text")
st.sidebar.code(f"Target: {st.session_state.target_path}", language="text")


# Main content
if page == "🏠 Home":
    st.markdown('<div class="main-header">⚡ Power Sector ETL Dashboard</div>', unsafe_allow_html=True)
    st.markdown("### Enhanced with Source Data Explorer & Quality Analysis")
    st.markdown("---")
    
    # Discover source structure
    with st.spinner("Scanning source directory..."):
        structure = discover_source_structure(st.session_state.source_path)
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Divisions", len(structure['divisions']))
    
    with col2:
        st.metric("Subdivisions", len(structure['subdivisions']))
    
    with col3:
        st.metric("Total Files", structure['file_count'])
    
    with col4:
        st.metric("Total Size", f"{structure['total_size'] / (1024**3):.2f} GB")
    
    # Errors
    if structure['errors']:
        st.markdown("---")
        st.error("⚠️ **Errors Found:**")
        for error in structure['errors']:
            st.write(f"- {error}")
    
    st.markdown("---")
    
    # Processing status
    st.subheader("📊 Processing Status")
    
    target_path = Path(st.session_state.target_path)
    bills_exists = (target_path / "bills.parquet").exists()
    readings_exists = (target_path / "readings.parquet").exists()
    
    col1, col2 = st.columns(2)
    
    with col1:
        if bills_exists:
            st.markdown('<div class="success-box">✅ Bills Processed</div>', unsafe_allow_html=True)
            bills_size = (target_path / "bills.parquet").stat().st_size / (1024**2)
            st.write(f"Size: {bills_size:.2f} MB")
        else:
            st.markdown('<div class="warning-box">⚠️ Bills Not Processed</div>', unsafe_allow_html=True)
    
    with col2:
        if readings_exists:
            st.markdown('<div class="success-box">✅ Readings Processed</div>', unsafe_allow_html=True)
            readings_size = (target_path / "readings.parquet").stat().st_size / (1024**2)
            st.write(f"Size: {readings_size:.2f} MB")
        else:
            st.markdown('<div class="warning-box">⚠️ Readings Not Processed</div>', unsafe_allow_html=True)
    
    # Quick actions
    st.markdown("---")
    st.subheader("⚡ Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Run Full Load", use_container_width=True):
            st.switch_page = "🚀 Run ETL"
    
    with col2:
        if st.button("🔄 Run Incremental", use_container_width=True):
            st.switch_page = "🚀 Run ETL"
    
    with col3:
        if st.button("🔍 Explore Source", use_container_width=True):
            st.switch_page = "🔍 Source Explorer"


elif page == "🔍 Source Explorer":
    st.title("🔍 Source Data Explorer")
    st.markdown("Comprehensive exploration of source data files")
    st.markdown("---")
    
    # Discover structure
    with st.spinner("Scanning source directory..."):
        structure = discover_source_structure(st.session_state.source_path)
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📁 Directory Structure",
        "📄 Bills Explorer",
        "📊 Readings Explorer",
        "📈 Statistics"
    ])
    
    with tab1:
        st.subheader("Directory Structure")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Divisions:**")
            for div in sorted(structure['divisions']):
                st.write(f"📁 {div}")
        
        with col2:
            st.markdown("**File Distribution:**")
            st.write(f"Bills files: {len(structure['bills_files'])}")
            st.write(f"Readings files: {len(structure['readings_files'])}")
            st.write(f"Total size: {structure['total_size'] / (1024**3):.2f} GB")
        
        # Division breakdown
        st.markdown("---")
        st.subheader("Files by Division")
        
        division_data = {}
        for file_info in structure['bills_files'] + structure['readings_files']:
            div = file_info['division']
            if div not in division_data:
                division_data[div] = {'count': 0, 'size': 0}
            division_data[div]['count'] += 1
            division_data[div]['size'] += file_info['size']
        
        df_divisions = pd.DataFrame([
            {
                'Division': div,
                'Files': data['count'],
                'Size (MB)': data['size'] / (1024**2)
            }
            for div, data in division_data.items()
        ])
        
        st.dataframe(df_divisions, use_container_width=True)
        
        # Visualization
        fig = px.bar(
            df_divisions,
            x='Division',
            y='Size (MB)',
            title='Data Size by Division',
            color='Files',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("📄 Bills Data Explorer")
        
        if not structure['bills_files']:
            st.warning("No bills files found")
        else:
            # File selector
            selected_division = st.selectbox(
                "Select Division",
                options=sorted(set([f['division'] for f in structure['bills_files']]))
            )
            
            division_files = [
                f for f in structure['bills_files']
                if f['division'] == selected_division
            ]
            
            selected_subdivision = st.selectbox(
                "Select Subdivision",
                options=sorted([f['subdivision'] for f in division_files])
            )
            
            selected_file = next(
                f for f in division_files
                if f['subdivision'] == selected_subdivision
            )
            
            file_path = Path(selected_file['path'])
            
            # File metadata
            st.markdown("---")
            st.markdown("**File Metadata:**")
            metadata = get_file_metadata(file_path)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("File Size", f"{metadata['size_mb']:.2f} MB")
            with col2:
                st.metric("Modified", metadata['modified'].strftime("%Y-%m-%d"))
            with col3:
                st.metric("Created", metadata['created'].strftime("%Y-%m-%d"))
            
            # Load and display data
            st.markdown("---")
            st.markdown("**Data Preview:**")
            
            sample_size = st.slider("Sample size", 100, 10000, 1000, 100)
            
            with st.spinner("Loading data..."):
                df = load_sample_data(file_path, 'tsv', sample_size)
            
            if df is not None:
                # Display sample
                st.dataframe(df.head(20), use_container_width=True)
                
                # Schema
                st.markdown("---")
                st.markdown("**Schema:**")
                schema_df = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.astype(str),
                    'Non-Null': df.count(),
                    'Null': df.isnull().sum(),
                    'Unique': df.nunique()
                })
                st.dataframe(schema_df, use_container_width=True)
                
                # Basic statistics
                st.markdown("---")
                st.markdown("**Statistics:**")
                st.dataframe(df.describe(), use_container_width=True)

    
    with tab3:
        st.subheader("📊 Readings Data Explorer")
        
        if not structure['readings_files']:
            st.warning("No readings files found")
        else:
            # File selector
            selected_division = st.selectbox(
                "Select Division ",
                options=sorted(set([f['division'] for f in structure['readings_files']])),
                key="readings_div"
            )
            
            division_files = [
                f for f in structure['readings_files']
                if f['division'] == selected_division
            ]
            
            selected_subdivision = st.selectbox(
                "Select Subdivision ",
                options=sorted([f['subdivision'] for f in division_files]),
                key="readings_subdiv"
            )
            
            selected_file = next(
                f for f in division_files
                if f['subdivision'] == selected_subdivision
            )
            
            file_path = Path(selected_file['path'])
            
            # File metadata
            st.markdown("---")
            st.markdown("**File Metadata:**")
            metadata = get_file_metadata(file_path)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("File Size", f"{metadata['size_mb']:.2f} MB")
            with col2:
                st.metric("Modified", metadata['modified'].strftime("%Y-%m-%d"))
            with col3:
                st.metric("Created", metadata['created'].strftime("%Y-%m-%d"))
            
            # Load and display data
            st.markdown("---")
            st.markdown("**Data Preview:**")
            
            sample_size = st.slider("Sample size ", 100, 10000, 1000, 100, key="readings_sample")
            
            with st.spinner("Loading data..."):
                df = load_sample_data(file_path, 'csv', sample_size)
            
            if df is not None:
                # Display sample
                st.dataframe(df.head(20), use_container_width=True)
                
                # Schema
                st.markdown("---")
                st.markdown("**Schema:**")
                schema_df = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.astype(str),
                    'Non-Null': df.count(),
                    'Null': df.isnull().sum(),
                    'Unique': df.nunique()
                })
                st.dataframe(schema_df, use_container_width=True)
                
                # Basic statistics
                st.markdown("---")
                st.markdown("**Statistics:**")
                st.dataframe(df.describe(), use_container_width=True)
                
                # Voltage analysis
                if 'voltage' in df.columns:
                    st.markdown("---")
                    st.markdown("**Voltage Analysis:**")
                    
                    fig = px.histogram(
                        df,
                        x='voltage',
                        nbins=50,
                        title='Voltage Distribution'
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("📈 Overall Statistics")
        
        # File size distribution
        st.markdown("**File Size Distribution:**")
        
        all_files = []
        for f in structure['bills_files']:
            all_files.append({
                'Type': 'Bills',
                'Division': f['division'],
                'Subdivision': f['subdivision'],
                'Size (MB)': f['size'] / (1024**2)
            })
        
        for f in structure['readings_files']:
            all_files.append({
                'Type': 'Readings',
                'Division': f['division'],
                'Subdivision': f['subdivision'],
                'Size (MB)': f['size'] / (1024**2)
            })
        
        df_files = pd.DataFrame(all_files)
        
        # Summary statistics
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Bills Files:**")
            bills_df = df_files[df_files['Type'] == 'Bills']
            st.write(f"Count: {len(bills_df)}")
            st.write(f"Total Size: {bills_df['Size (MB)'].sum():.2f} MB")
            st.write(f"Avg Size: {bills_df['Size (MB)'].mean():.2f} MB")
            st.write(f"Min Size: {bills_df['Size (MB)'].min():.2f} MB")
            st.write(f"Max Size: {bills_df['Size (MB)'].max():.2f} MB")
        
        with col2:
            st.markdown("**Readings Files:**")
            readings_df = df_files[df_files['Type'] == 'Readings']
            st.write(f"Count: {len(readings_df)}")
            st.write(f"Total Size: {readings_df['Size (MB)'].sum():.2f} MB")
            st.write(f"Avg Size: {readings_df['Size (MB)'].mean():.2f} MB")
            st.write(f"Min Size: {readings_df['Size (MB)'].min():.2f} MB")
            st.write(f"Max Size: {readings_df['Size (MB)'].max():.2f} MB")
        
        # Visualizations
        st.markdown("---")
        
        fig = px.box(
            df_files,
            x='Type',
            y='Size (MB)',
            color='Type',
            title='File Size Distribution by Type'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        fig2 = px.scatter(
            df_files,
            x='Division',
            y='Size (MB)',
            color='Type',
            size='Size (MB)',
            title='File Sizes by Division and Type'
        )
        st.plotly_chart(fig2, use_container_width=True)


elif page == "📊 Data Quality":
    st.title("📊 Data Quality Analysis")
    st.markdown("Comprehensive data quality assessment")
    st.markdown("---")
    
    # Discover structure
    with st.spinner("Scanning source directory..."):
        structure = discover_source_structure(st.session_state.source_path)
    
    # Data type selector
    data_type = st.radio("Select Data Type", ["Bills", "Readings"], horizontal=True)
    
    if data_type == "Bills":
        files = structure['bills_files']
        file_type = 'tsv'
    else:
        files = structure['readings_files']
        file_type = 'csv'
    
    if not files:
        st.warning(f"No {data_type.lower()} files found")
    else:
        # File selector
        selected_division = st.selectbox(
            "Select Division",
            options=sorted(set([f['division'] for f in files])),
            key="quality_div"
        )
        
        division_files = [f for f in files if f['division'] == selected_division]
        
        selected_subdivision = st.selectbox(
            "Select Subdivision",
            options=sorted([f['subdivision'] for f in division_files]),
            key="quality_subdiv"
        )
        
        selected_file = next(
            f for f in division_files
            if f['subdivision'] == selected_subdivision
        )
        
        file_path = Path(selected_file['path'])
        
        # Load data
        st.markdown("---")
        sample_size = st.slider("Analysis sample size", 1000, 50000, 10000, 1000)
        
        with st.spinner("Loading and analyzing data..."):
            df = load_sample_data(file_path, file_type, sample_size)
            
            if df is not None:
                quality_report = analyze_data_quality(
                    df,
                    'bills' if data_type == 'Bills' else 'readings'
                )
                
                # Overview metrics
                st.subheader("📋 Quality Overview")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Records", f"{quality_report['total_records']:,}")
                
                with col2:
                    st.metric("Total Columns", quality_report['total_columns'])
                
                with col3:
                    st.metric("Memory Usage", f"{quality_report['memory_usage_mb']:.2f} MB")
                
                with col4:
                    issues_count = len(quality_report['issues'])
                    st.metric("Issues Found", issues_count)
                
                # Issues
                if quality_report['issues']:
                    st.markdown("---")
                    st.markdown("### ⚠️ Data Quality Issues")
                    for issue in quality_report['issues']:
                        st.markdown(f'<div class="warning-box">{issue}</div>', unsafe_allow_html=True)
                else:
                    st.markdown("---")
                    st.markdown('<div class="success-box">✅ No critical issues found!</div>', unsafe_allow_html=True)
                
                # Duplicates
                st.markdown("---")
                st.subheader("🔄 Duplicate Analysis")
                
                if quality_report['duplicates']:
                    for key, count in quality_report['duplicates'].items():
                        if count > 0:
                            st.warning(f"⚠️ {count} duplicate records found ({key})")
                        else:
                            st.success(f"✅ No duplicates found ({key})")
                else:
                    st.info("No duplicate analysis available")
                
                # Missing values
                st.markdown("---")
                st.subheader("❓ Missing Values Analysis")
                
                if quality_report['missing_values']:
                    missing_df = pd.DataFrame([
                        {
                            'Column': col,
                            'Missing Count': data['count'],
                            'Missing %': f"{data['percentage']:.2f}%"
                        }
                        for col, data in quality_report['missing_values'].items()
                    ]).sort_values('Missing Count', ascending=False)
                    
                    st.dataframe(missing_df, use_container_width=True)
                    
                    # Visualization
                    fig = px.bar(
                        missing_df,
                        x='Column',
                        y='Missing Count',
                        title='Missing Values by Column',
                        color='Missing Count',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.success("✅ No missing values found!")
                
                # Value ranges
                st.markdown("---")
                st.subheader("📏 Value Ranges")
                
                if quality_report['value_ranges']:
                    ranges_df = pd.DataFrame([
                        {
                            'Column': col,
                            'Min': f"{data['min']:.2f}",
                            'Max': f"{data['max']:.2f}",
                            'Mean': f"{data['mean']:.2f}",
                            'Median': f"{data['median']:.2f}",
                            'Std Dev': f"{data['std']:.2f}"
                        }
                        for col, data in quality_report['value_ranges'].items()
                    ])
                    
                    st.dataframe(ranges_df, use_container_width=True)
                else:
                    st.info("No numeric columns found")
                
                # Outliers
                st.markdown("---")
                st.subheader("🎯 Outlier Detection")
                
                if quality_report['outliers']:
                    outliers_df = pd.DataFrame([
                        {'Column': col, 'Outliers': count}
                        for col, count in quality_report['outliers'].items()
                    ]).sort_values('Outliers', ascending=False)
                    
                    st.dataframe(outliers_df, use_container_width=True)
                    
                    # Visualization
                    fig = px.bar(
                        outliers_df,
                        x='Column',
                        y='Outliers',
                        title='Outliers by Column (IQR method)',
                        color='Outliers',
                        color_continuous_scale='Oranges'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.success("✅ No outliers detected!")
                
                # Data types
                st.markdown("---")
                st.subheader("🔤 Data Types")
                
                types_df = pd.DataFrame([
                    {'Column': col, 'Data Type': dtype}
                    for col, dtype in quality_report['data_types'].items()
                ])
                
                st.dataframe(types_df, use_container_width=True)


elif page == "📈 Processed EDA":
    st.title("📈 Processed Data Analysis")
    st.markdown("Explore consolidated and processed data")
    st.markdown("---")
    
    target_path = Path(st.session_state.target_path)
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    if not bills_path.exists() and not readings_path.exists():
        st.warning("⚠️ No processed data found. Please run ETL pipeline first.")
        if st.button("🚀 Run ETL Now"):
            st.switch_page = "🚀 Run ETL"
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
                st.markdown("---")
                st.markdown("**Sample Data:**")
                st.dataframe(bills_df.head(20).compute(), use_container_width=True)
                
                # Revenue analysis
                st.markdown("---")
                st.subheader("💰 Revenue Analysis")
                
                revenue_by_div = bills_df.groupby('division')['total_amount'].sum().compute().sort_values(ascending=False)
                
                fig = px.bar(
                    x=revenue_by_div.index,
                    y=revenue_by_div.values,
                    labels={'x': 'Division', 'y': 'Total Revenue'},
                    title='Total Revenue by Division',
                    color=revenue_by_div.values,
                    color_continuous_scale='Greens'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Consumption analysis
                st.markdown("---")
                st.subheader("⚡ Consumption Analysis")
                
                sample = bills_df.sample(frac=0.1).compute()
                
                fig = px.histogram(
                    sample,
                    x='consumption_kwh',
                    nbins=50,
                    title='Consumption Distribution (kWh)',
                    color_discrete_sequence=['#1f77b4']
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Payment status
                st.markdown("---")
                st.subheader("💳 Payment Status")
                
                status_counts = bills_df['status'].value_counts().compute()
                
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title='Payment Status Distribution',
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Bills data not yet processed")
        
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
                st.markdown("---")
                st.markdown("**Sample Data:**")
                st.dataframe(readings_df.head(20).compute(), use_container_width=True)
                
                # Voltage analysis
                st.markdown("---")
                st.subheader("⚡ Voltage Quality Analysis")
                
                sample = readings_df.sample(frac=0.05).compute()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.histogram(
                        sample,
                        x='voltage',
                        nbins=50,
                        title='Voltage Distribution',
                        color_discrete_sequence=['#ff7f0e']
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    fig = px.box(
                        sample,
                        y='voltage',
                        title='Voltage Box Plot',
                        color_discrete_sequence=['#2ca02c']
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Quality flags
                st.markdown("---")
                st.subheader("🏷️ Data Quality Flags")
                
                quality_counts = readings_df['quality_flag'].value_counts().compute()
                
                fig = px.pie(
                    values=quality_counts.values,
                    names=quality_counts.index,
                    title='Quality Flag Distribution',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Readings data not yet processed")

elif page == "🚀 Run ETL":
    st.title("🚀 Run ETL Pipeline")
    st.markdown("Execute data processing pipeline")
    st.markdown("---")
    
    # ETL configuration
    col1, col2 = st.columns(2)
    
    with col1:
        mode = st.radio(
            "ETL Mode",
            ["Full Load", "Incremental Load"],
            help="Full: Process all files | Incremental: Process only new/changed files"
        )
    
    with col2:
        compression = st.selectbox(
            "Compression",
            ["snappy", "gzip", "brotli"],
            help="Snappy: Fast | Gzip: Balanced | Brotli: Best compression"
        )
    
    st.markdown("---")
    
    if st.session_state.processing:
        st.warning("⏳ Processing in progress...")
    else:
        if st.button("▶️ Start Processing", type="primary", use_container_width=True):
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

elif page == "📅 Scheduler":
    st.title("📅 ETL Scheduler")
    st.markdown("Configure automated ETL execution")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        schedule_enabled = st.checkbox("Enable Scheduler")
        interval_minutes = st.number_input("Interval (minutes)", min_value=5, max_value=1440, value=60)
    
    with col2:
        schedule_mode = st.selectbox("Mode", ["Incremental Load", "Full Load"])
        start_time = st.time_input("Start Time")
    
    if schedule_enabled:
        st.info(f"📅 Scheduler will run {schedule_mode} every {interval_minutes} minutes")
    
    st.markdown("---")
    st.subheader("💻 Command Line Usage")
    
    st.code(f"""
# Full load
python etl_cli.py --mode full --compression snappy

# Incremental load
python etl_cli.py --mode incremental

# Automated scheduler
python etl_scheduler.py --interval {interval_minutes}

# Statistics
python etl_cli.py --mode stats
    """, language="bash")

elif page == "✅ Validation":
    st.title("✅ Data Validation")
    st.markdown("Validate processed data quality")
    st.markdown("---")
    
    target_path = Path(st.session_state.target_path)
    bills_path = target_path / "bills.parquet"
    readings_path = target_path / "readings.parquet"
    
    if not bills_path.exists() and not readings_path.exists():
        st.warning("⚠️ No processed data found")
    else:
        if st.button("🔍 Run Validation", type="primary"):
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
                            st.error(f"❌ {duplicates} duplicates")
                    
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
                    st.markdown("---")
                    st.subheader("📊 Readings Validation")
                    
                    readings_df = dd.read_parquet(readings_path)
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        duplicates = readings_df.duplicated(subset=['timestamp', 'meter_id']).sum().compute()
                        if duplicates == 0:
                            st.success(f"✅ No duplicates")
                        else:
                            st.error(f"❌ {duplicates} duplicates")
                    
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
Enhanced Version 2.0  
© 2026
""")
