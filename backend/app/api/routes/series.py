"""Series routes using Supabase REST API."""
from fastapi import APIRouter, Query

from app.core.config import settings
from app.schemas import PaginatedResponse
from app.services import video_service_rest as video_service
from app.core.cache import series_cache, series_videos_cache, generate_cache_key

router = APIRouter(prefix="/series", tags=["series"])


@router.get("", response_model=list[dict])
async def list_series():
    """Get all series with video counts."""
    cached = series_cache.get("all_series")
    if cached:
        return cached
    
    result = await video_service.get_all_series()
    series_cache.set("all_series", result)
    return result


@router.get("/{series_name}/videos", response_model=PaginatedResponse)
async def get_videos_by_series(
    series_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None)
):
    """Get videos from a series."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    cache_key = generate_cache_key("series_videos", series_name, page, page_size)
    cached = series_videos_cache.get(cache_key)
    if cached:
        return cached
    
    result = await video_service.get_videos_by_series(series_name, page, page_size)
    series_videos_cache.set(cache_key, result)
    return result
