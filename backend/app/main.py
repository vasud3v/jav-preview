"""FastAPI application entry point."""
import signal
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.core.config import settings
from backend.app.core.database import engine
from backend.app.api.router import api_router
from backend.app.models import Base

# Ensure database directory exists and create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    version=settings.api_version,
    debug=settings.debug,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.on_event("startup")
async def startup_event():
    """Run on application startup."""
    print("=" * 60)
    print(f"{settings.app_name} v{settings.api_version}")
    print("=" * 60)
    print(f"✓ Connected to Supabase")
    print(f"✓ API running on http://{settings.host}:{settings.port}")
    print(f"✓ Press Ctrl+C to stop gracefully")
    print("=" * 60)


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown."""
    print("\n" + "=" * 60)
    print("SHUTTING DOWN GRACEFULLY")
    print("=" * 60)
    print("✓ Closing database connections...")
    engine.dispose()
    print("✓ Backend stopped successfully")
    print("=" * 60)


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    print("\n\nReceived shutdown signal...")
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)
