@echo off
echo ============================================================
echo BACKEND API SERVER
echo ============================================================
echo.
echo Starting FastAPI backend on http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo.
echo Press Ctrl+C to stop gracefully
echo.
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
pause
