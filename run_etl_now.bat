@echo off
REM Run ETL with current folder structure

echo ========================================
echo Power Sector ETL Pipeline
echo ========================================
echo.
echo This will run the ETL to process readings
echo (Bills already completed)
echo.
echo Memory Available: ~4 GB
echo Expected Time: 5-10 minutes
echo.

python run_etl_current.py

pause
