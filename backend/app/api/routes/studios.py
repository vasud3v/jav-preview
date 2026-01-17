"""Studio routes using Supabase REST API."""
from fastapi import APIRouter, Query

from app.core.config import settings
from app.schemas import StudioResponse, PaginatedResponse
from app.services import video_service_rest as video_service
from app.core.cache import studios_cache, studio_videos_cache, generate_cache_key

router = APIRouter(prefix="/studios", tags=["studios"])


@router.get("", response_model=list[StudioResponse])
async def list_studios():
    """Get all studios with video counts."""
    # Check cache
    cached = studios_cache.get("all_studios")
    if cached:
        return cached
    
    # Fetch and cache
    result = await video_service.get_all_studios()
    studios_cache.set("all_studios", result)
    return result


@router.get("/{studio}/videos", response_model=PaginatedResponse)
async def get_videos_by_studio(
    studio: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None)
):
    """Get videos from a studio."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    # Check cache
    cache_key = generate_cache_key("studio_videos", studio, page, page_size)
    cached = studio_videos_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = await video_service.get_videos_by_studio(studio, page, page_size)
    studio_videos_cache.set(cache_key, result)
    return result
