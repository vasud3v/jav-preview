"""Run the backend server."""
import uvicorn
from backend.app.core.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "backend.app.main:app",
        host=settings.host,
        port=settings.port,
        reload=True
    )
