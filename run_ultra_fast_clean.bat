@echo off
echo Stopping any running Python processes...
taskkill /F /IM python.exe 2>nul
timeout /t 2 /nobreak >nul

echo.
echo Starting Ultra-Fast Generator (1 GB test)...
echo.
python demo_ultra_fast.py

pause
