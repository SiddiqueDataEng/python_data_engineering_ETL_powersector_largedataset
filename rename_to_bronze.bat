@echo off
REM Rename power_sector_data to power_sector_data_bronze_layer

echo ========================================
echo Rename to Bronze Layer
echo ========================================
echo.

echo This will rename:
echo   FROM: power_sector_data
echo   TO:   power_sector_data_bronze_layer
echo.

echo IMPORTANT: Close all programs accessing the folder first!
echo   - Close Python scripts
echo   - Close File Explorer
echo   - Close command prompts
echo.

pause

echo.
echo Attempting rename...

if exist "power_sector_data" (
    ren "power_sector_data" "power_sector_data_bronze_layer"
    
    if exist "power_sector_data_bronze_layer" (
        echo.
        echo ========================================
        echo SUCCESS! Folder renamed.
        echo ========================================
        echo.
        echo New folder: power_sector_data_bronze_layer
        echo.
        echo All scripts have been updated to use the new path.
        echo You can now run the data generator or ETL.
    ) else (
        echo.
        echo ERROR: Rename failed.
        echo Make sure no programs are accessing the folder.
    )
) else (
    if exist "power_sector_data_bronze_layer" (
        echo.
        echo Folder already renamed to power_sector_data_bronze_layer
        echo Nothing to do.
    ) else (
        echo.
        echo ERROR: power_sector_data folder not found!
    )
)

echo.
pause
