"""Stats routes with caching."""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db
from backend.app.schemas import StatsResponse
from backend.app.services import metadata_service
from backend.app.core.cache import stats_cache, get_all_cache_stats, clear_all_caches

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Get database statistics."""
    # Check cache
    cached = stats_cache.get("stats")
    if cached:
        return cached
    
    # Fetch and cache
    result = metadata_service.get_stats(db)
    stats_cache.set("stats", result)
    return result


@router.get("/cache")
def get_cache_stats():
    """Get cache statistics for monitoring."""
    return get_all_cache_stats()


@router.post("/cache/clear")
def clear_cache():
    """Clear all caches."""
    clear_all_caches()
    return {"status": "cleared", "message": "All caches cleared"}
