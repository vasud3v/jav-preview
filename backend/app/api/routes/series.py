"""Series routes with caching."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db, settings
from backend.app.schemas import PaginatedResponse
from backend.app.services import video_service
from backend.app.core.cache import series_cache, series_videos_cache, generate_cache_key

router = APIRouter(prefix="/series", tags=["series"])


@router.get("", response_model=list[dict])
def list_series(db: Session = Depends(get_db)):
    """Get all series with video counts."""
    cached = series_cache.get("all_series")
    if cached:
        return cached
    
    result = video_service.get_all_series(db)
    series_cache.set("all_series", result)
    return result


@router.get("/{series_name}/videos", response_model=PaginatedResponse)
def get_videos_by_series(
    series_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
):
    """Get videos from a series."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    cache_key = generate_cache_key("series_videos", series_name, page, page_size)
    cached = series_videos_cache.get(cache_key)
    if cached:
        return cached
    
    result = video_service.get_videos_by_series(db, series_name, page, page_size)
    series_videos_cache.set(cache_key, result)
    return result
