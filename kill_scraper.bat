@echo off
echo ============================================================
echo KILL SCRAPER PROCESSES
echo ============================================================
echo This will force-kill all Chrome and scraper processes.
echo.

REM Kill Chrome
taskkill /F /IM chrome.exe 2>nul
taskkill /F /IM chromedriver.exe 2>nul

REM Kill Python scraper
taskkill /F /FI "WINDOWTITLE eq *main.py*" 2>nul

echo.
echo Done! All processes killed.
echo You can now restart the scraper.
echo.
pause
