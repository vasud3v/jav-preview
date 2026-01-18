"""Main API router - aggregates all route modules."""
from fastapi import APIRouter

from app.api.routes import videos, categories, studios, series, cast, stats, auth, upload, proxy, comments, likes

api_router = APIRouter(prefix="/api")

api_router.include_router(auth.router)
api_router.include_router(upload.router)
api_router.include_router(proxy.router)
api_router.include_router(stats.router)
api_router.include_router(videos.router)
api_router.include_router(comments.router)
api_router.include_router(likes.router)
api_router.include_router(categories.router)
api_router.include_router(studios.router)
api_router.include_router(series.router)
api_router.include_router(cast.router)


@api_router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
