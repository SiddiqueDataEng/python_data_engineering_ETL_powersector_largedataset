@echo off
REM Launch Streamlit ETL Dashboard

echo ========================================
echo Power Sector ETL Dashboard
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if Streamlit is installed
python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install -r requirements_etl.txt
)

echo.
echo Starting dashboard...
echo Dashboard will open in your browser automatically.
echo Press Ctrl+C to stop the server.
echo.

REM Launch Enhanced Streamlit Dashboard using Python module
python -m streamlit run streamlit_etl_dashboard_enhanced.py

pause
