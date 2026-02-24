@echo off
REM Fix Parquet files and run ETL

echo ========================================
echo Fix Parquet Files and Run ETL
echo ========================================
echo.

echo Step 1: Removing old directory-based Parquet files...
python fix_parquet_files.py

echo.
echo Step 2: Running ETL to create proper single files...
python run_etl.py

echo.
echo ========================================
echo Done! You can now run the dashboard.
echo ========================================
pause
