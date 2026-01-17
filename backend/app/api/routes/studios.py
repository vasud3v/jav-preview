"""Studio routes with caching."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db, settings
from backend.app.schemas import StudioResponse, PaginatedResponse
from backend.app.services import metadata_service, video_service
from backend.app.core.cache import studios_cache, studio_videos_cache, generate_cache_key

router = APIRouter(prefix="/studios", tags=["studios"])


@router.get("", response_model=list[StudioResponse])
def list_studios(db: Session = Depends(get_db)):
    """Get all studios with video counts."""
    # Check cache
    cached = studios_cache.get("all_studios")
    if cached:
        return cached
    
    # Fetch and cache
    result = metadata_service.get_all_studios(db)
    studios_cache.set("all_studios", result)
    return result


@router.get("/{studio}/videos", response_model=PaginatedResponse)
def get_videos_by_studio(
    studio: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
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
    result = video_service.get_videos_by_studio(db, studio, page, page_size)
    studio_videos_cache.set(cache_key, result)
    return result
