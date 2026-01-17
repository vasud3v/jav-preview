"""Category routes with caching."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db, settings
from backend.app.schemas import CategoryResponse, PaginatedResponse
from backend.app.services import metadata_service, video_service
from backend.app.core.cache import categories_cache, category_videos_cache, generate_cache_key

router = APIRouter(prefix="/categories", tags=["categories"])


@router.get("", response_model=list[CategoryResponse])
def list_categories(db: Session = Depends(get_db)):
    """Get all categories with video counts."""
    # Check cache
    cached = categories_cache.get("all_categories")
    if cached:
        return cached
    
    # Fetch and cache
    result = metadata_service.get_all_categories(db)
    categories_cache.set("all_categories", result)
    return result


@router.get("/{category}/videos", response_model=PaginatedResponse)
def get_videos_by_category(
    category: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
):
    """Get videos in a category."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    # Check cache
    cache_key = generate_cache_key("cat_videos", category, page, page_size)
    cached = category_videos_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_videos_by_category(db, category, page, page_size)
    category_videos_cache.set(cache_key, result)
    return result
