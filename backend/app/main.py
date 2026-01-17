"""FastAPI application entry point - REST API mode."""
import signal
import sys
import traceback
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.supabase_rest_client import get_supabase_rest, close_supabase_rest
from app.api.router import api_router

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


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler to log all errors."""
    print(f"ERROR: {request.method} {request.url.path}")
    print(f"Exception: {type(exc).__name__}: {exc}")
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__}
    )


app.include_router(api_router)


@app.on_event("startup")
async def startup_event():
    """Run on application startup."""
    print("=" * 60)
    print(f"{settings.app_name} v{settings.api_version}")
    print("=" * 60)
    
    # Initialize Supabase REST client
    try:
        client = get_supabase_rest()
        print(f"✓ Connected to Supabase REST API")
    except Exception as e:
        print(f"⚠ Supabase connection warning: {e}")
    
    print(f"✓ CORS origins: {settings.cors_origins_list}")
    print(f"✓ API running on http://{settings.host}:{settings.port}")
    print(f"✓ Press Ctrl+C to stop gracefully")
    print("=" * 60)


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown."""
    print("\n" + "=" * 60)
    print("SHUTTING DOWN GRACEFULLY")
    print("=" * 60)
    print("✓ Closing Supabase REST client...")
    await close_supabase_rest()
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
