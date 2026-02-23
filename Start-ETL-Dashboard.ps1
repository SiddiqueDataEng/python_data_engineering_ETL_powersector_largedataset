# PowerShell Script to Launch Streamlit ETL Dashboard

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Power Sector ETL Dashboard" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Python is installed
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✓ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ ERROR: Python is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Python 3.8+ and try again" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if Streamlit is installed
Write-Host "Checking dependencies..." -ForegroundColor Yellow
try {
    python -c "import streamlit" 2>&1 | Out-Null
    Write-Host "✓ Streamlit is installed" -ForegroundColor Green
} catch {
    Write-Host "Installing required packages..." -ForegroundColor Yellow
    pip install -r requirements_etl.txt
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Starting Dashboard..." -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Dashboard will open in your browser at: http://localhost:8501" -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
Write-Host ""

# Launch Enhanced Streamlit Dashboard
python -m streamlit run streamlit_etl_dashboard_enhanced.py

Write-Host ""
Write-Host "Dashboard stopped." -ForegroundColor Yellow
Read-Host "Press Enter to exit"
