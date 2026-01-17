"""Cast routes using Supabase REST API."""
from fastapi import APIRouter, Query

from app.core.config import settings
from app.schemas import CastResponse, PaginatedResponse
from app.schemas.metadata import CastWithImageResponse
from app.services import video_service_rest as video_service
from app.core.cache import cast_cache, cast_featured_cache, cast_videos_cache, generate_cache_key

router = APIRouter(prefix="/cast", tags=["cast"])


@router.get("", response_model=list[CastResponse])
async def list_cast():
    """Get all cast members with video counts."""
    # Check cache
    cached = cast_cache.get("all_cast")
    if cached:
        return cached
    
    # Fetch and cache
    result = await video_service.get_all_cast()
    cast_cache.set("all_cast", result)
    return result


@router.get("/all", response_model=list[CastWithImageResponse])
async def list_all_cast_with_images():
    """Get all cast members with images and video counts."""
    cached = cast_cache.get("all_cast_images")
    if cached:
        return cached
    
    # Use get_cast_with_images to fetch cast with images from videos
    result = await video_service.get_cast_with_images(100)
    cast_cache.set("all_cast_images", result)
    return result


@router.get("/featured", response_model=list[CastWithImageResponse])
async def get_featured_cast(
    limit: int = Query(20, ge=1, le=50)
):
    """Get featured cast members with images."""
    # Check cache
    cache_key = f"featured:{limit}"
    cached = cast_featured_cache.get(cache_key)
    if cached:
        return cached
    
    # Use get_cast_with_images to fetch cast with images from videos
    result = await video_service.get_cast_with_images(limit)
    cast_featured_cache.set(cache_key, result)
    return result


@router.get("/{cast_name}/videos", response_model=PaginatedResponse)
async def get_videos_by_cast(
    cast_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None)
):
    """Get videos featuring a cast member."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    # Check cache
    cache_key = generate_cache_key("cast_videos", cast_name, page, page_size)
    cached = cast_videos_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = await video_service.get_videos_by_cast(cast_name, page, page_size)
    cast_videos_cache.set(cache_key, result)
    return result
