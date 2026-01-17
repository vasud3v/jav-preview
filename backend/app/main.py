"""FastAPI application entry point - REST API mode."""
import signal
import sys
import traceback
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings

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


# Simple health check before any complex imports
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": settings.api_version}


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Prevue API", "version": settings.api_version}


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


# Import and include router AFTER health check is registered
try:
    from app.core.supabase_rest_client import get_supabase_rest, close_supabase_rest
    from app.api.router import api_router
    app.include_router(api_router)
    _router_loaded = True
except Exception as e:
    print(f"ERROR loading router: {e}")
    traceback.print_exc()
    _router_loaded = False
    close_supabase_rest = None


@app.on_event("startup")
async def startup_event():
    """Run on application startup."""
    print("=" * 60)
    print(f"{settings.app_name} v{settings.api_version}")
    print("=" * 60)
    print(f"✓ CORS origins: {settings.cors_origins_list}")
    print(f"✓ Router loaded: {_router_loaded}")
    
    # Initialize Supabase REST client
    if _router_loaded:
        try:
            client = get_supabase_rest()
            print(f"✓ Connected to Supabase REST API")
        except Exception as e:
            print(f"⚠ Supabase connection warning: {e}")
    
    print(f"✓ API ready on http://0.0.0.0:{settings.port}")
    print("=" * 60)


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown."""
    print("\nShutting down...")
    if close_supabase_rest:
        await close_supabase_rest()
    print("✓ Stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)
