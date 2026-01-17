"""Cast routes with caching."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db, settings
from backend.app.schemas import CastResponse, PaginatedResponse
from backend.app.schemas.metadata import CastWithImageResponse
from backend.app.services import metadata_service, video_service
from backend.app.core.cache import cast_cache, cast_featured_cache, cast_videos_cache, generate_cache_key

router = APIRouter(prefix="/cast", tags=["cast"])


@router.get("", response_model=list[CastResponse])
def list_cast(db: Session = Depends(get_db)):
    """Get all cast members with video counts."""
    # Check cache
    cached = cast_cache.get("all_cast")
    if cached:
        return cached
    
    # Fetch and cache
    result = metadata_service.get_all_cast(db)
    cast_cache.set("all_cast", result)
    return result


@router.get("/all", response_model=list[CastWithImageResponse])
def list_all_cast_with_images(db: Session = Depends(get_db)):
    """Get all cast members with images and video counts."""
    cached = cast_cache.get("all_cast_images")
    if cached:
        return cached
    
    result = metadata_service.get_all_cast_with_images(db)
    cast_cache.set("all_cast_images", result)
    return result


@router.get("/featured", response_model=list[CastWithImageResponse])
def get_featured_cast(
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get featured cast members with images."""
    # Check cache
    cache_key = f"featured:{limit}"
    cached = cast_featured_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = metadata_service.get_cast_with_images(db, limit)
    cast_featured_cache.set(cache_key, result)
    return result


@router.get("/{cast_name}/videos", response_model=PaginatedResponse)
def get_videos_by_cast(
    cast_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
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
    result = video_service.get_videos_by_cast(db, cast_name, page, page_size)
    cast_videos_cache.set(cache_key, result)
    return result
