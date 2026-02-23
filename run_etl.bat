@echo off
REM Power Sector ETL Pipeline Runner

echo ========================================
echo Power Sector ETL Pipeline
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if required packages are installed
python -c "import dask" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install -r requirements_etl.txt
)

REM Run the ETL pipeline
python run_etl.py

pause
