@echo off
REM Clear backend cache
echo Clearing backend cache...
curl -X POST http://localhost:8000/api/stats/cache/clear
echo.
echo Cache cleared successfully!
echo.
echo Note: Frontend cache will auto-refresh in 1 minute, or use the Refresh button on the Home page.
pause
