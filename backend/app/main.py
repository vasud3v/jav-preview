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
            
    # Determine base URL for display
    # Railway provides RAILWAY_PUBLIC_DOMAIN
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
    if domain:
        base_url = f"https://{domain}"
    else:
        # Fallback to local or manually provided
        base_url = f"http://{settings.host}:{settings.port}"
        if "jav-preview-production.up.railway.app" in settings.cors_origins:
             base_url = "https://jav-preview-production.up.railway.app"

    print("-" * 60)
    print(f"🚀 Application is READY at: {base_url}")
    print("-" * 60)
    print(f"📱 Frontend:    {base_url}/")
    print(f"📚 API Docs:    {base_url}/docs")
    print(f"💓 Health:      {base_url}/api/health")
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

# Serve React Frontend in Production
# This effectively allows backend and frontend to run in the same service/port
import os
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

# Calculate path to frontend/dist (assuming running from root or backend)
# transform: /app/backend/app/main.py -> /app/frontend/dist
# We use resolve() to get absolute path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
frontend_dist = project_root / "frontend" / "dist"
assets_path = frontend_dist / "assets"

print(f"Frontend dist path: {frontend_dist}")
print(f"Assets path: {assets_path}")

if frontend_dist.exists():
    print("✓ Found frontend build directory, serving static files")
    
    # Mount assets directory explicitly
    # Check if assets dir exists first to avoid errors if build is partial
    if assets_path.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_path)), name="assets")
    
    # Catch-all route for SPA (React Router)
    # This must be the LAST route defined
    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        # Allow API calls to pass through (though they should be caught by earlier routes)
        if full_path.startswith("api/"):
            return JSONResponse(status_code=404, content={"detail": "Not Found"})
            
        # Check if a file exists specifically (e.g. favicon.ico, manifest.json)
        file_path = frontend_dist / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
            
        # Otherwise return index.html for SPA routing
        return FileResponse(frontend_dist / "index.html")
else:
    print(f"⚠ Frontend build directory not found at {frontend_dist}")
    print("  (This is expected during local development if not built, or if running separate services)")
