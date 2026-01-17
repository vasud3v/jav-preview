"""Railway startup script."""
import os
import uvicorn

if __name__ == "__main__":
    # Railway sets PORT env var - use it, default to 8000 to match Railway networking config
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting server on port {port}...")
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
