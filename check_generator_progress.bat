@echo off
REM Check data generator progress

echo ========================================
echo Data Generator Progress Monitor
echo ========================================
echo.

REM Check if process is running
tasklist /FI "IMAGENAME eq python.exe" 2>NUL | find /I /N "python.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo Status: ✅ Generator is RUNNING
) else (
    echo Status: ⏸️  Generator is NOT running
)

echo.
echo Checking data size...
echo.

REM Check data folder size
dir "\\counter2\E\iesco\power_sector_data" /s | find "File(s)"

echo.
echo ========================================
echo.
echo To view live logs, run:
echo   python -c "import time; exec('while True: print(open(\"generator.log\").read()[-2000:]); time.sleep(5)')"
echo.
echo Or check specific subdivision:
echo   dir "\\counter2\E\iesco\power_sector_data\ISLAMABAD\I-10\readings"
echo.

pause
