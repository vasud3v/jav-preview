"""Video routes with caching."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db, settings
from backend.app.schemas import VideoResponse, PaginatedResponse
from backend.app.services import video_service
from backend.app.core.cache import (
    videos_list_cache, video_detail_cache, search_cache, generate_cache_key
)

router = APIRouter(prefix="/videos", tags=["videos"])


# ============================================
# User-specific routes (must be before /{code} routes)
# ============================================

@router.get("/user/bookmarks", response_model=PaginatedResponse)
def get_bookmarks(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
):
    """Get user's bookmarked videos."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    return video_service.get_user_bookmarks(db, user_id, page, page_size)


@router.get("/user/for-you", response_model=PaginatedResponse)
def get_for_you(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=24),
    db: Session = Depends(get_db)
):
    """Get personalized 'For You' recommendations based on watch history and preferences."""
    return video_service.get_personalized_recommendations(db, user_id, page, page_size)


@router.get("/user/discover")
def get_discover_more(
    user_id: str = Query(...),
    batch: int = Query(0, ge=0, description="Batch number for infinite scroll"),
    batch_size: int = Query(12, ge=6, le=24),
    seen: str = Query('', description="Comma-separated list of already seen video codes"),
    db: Session = Depends(get_db)
):
    """
    Get infinite scroll recommendations.
    Each batch uses different strategy for variety.
    Returns: items, has_more, batch, strategy
    """
    from backend.app.services.recommendation_service import RecommendationEngine
    
    seen_codes = [c.strip() for c in seen.split(',') if c.strip()] if seen else []
    
    engine = RecommendationEngine(db)
    return engine.get_infinite_recommendations(user_id, batch, batch_size, seen_codes)


@router.get("/user/bookmarks/count")
def get_bookmark_count(
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Get user's total bookmark count."""
    return {"count": video_service.get_bookmark_count(db, user_id)}


@router.get("/user/history", response_model=PaginatedResponse)
def get_watch_history(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get user's watch history."""
    return video_service.get_watch_history(db, user_id, page, page_size)


@router.delete("/user/history")
def clear_watch_history(
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Clear user's watch history."""
    return video_service.clear_watch_history(db, user_id)


@router.post("/user/merge-history")
def merge_history(
    from_user_id: str = Query(..., description="Anonymous user ID to merge from"),
    to_user_id: str = Query(..., description="Logged-in user ID to merge to"),
    db: Session = Depends(get_db)
):
    """Merge anonymous watch history and ratings into logged-in user account."""
    return video_service.merge_watch_history(db, from_user_id, to_user_id)


# ============================================
# Video list routes
# ============================================

@router.get("", response_model=PaginatedResponse)
def list_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    sort_by: str = Query("release_date"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    db: Session = Depends(get_db)
):
    """Get paginated list of videos."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    # Check cache
    cache_key = generate_cache_key("videos", page, page_size, sort_by, sort_order)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_videos(db, page, page_size, sort_by, sort_order)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/random")
def get_random_video(
    exclude: str = Query(None, description="Comma-separated list of video codes to exclude"),
    db: Session = Depends(get_db)
):
    """Get a random video code for the random button, excluding recently viewed."""
    exclude_list = []
    if exclude:
        exclude_list = [code.strip().upper() for code in exclude.split(',') if code.strip()]
    
    code = video_service.get_random_video_code(db, exclude_list)
    if not code:
        raise HTTPException(status_code=404, detail="No videos found")
    return {"code": code}


@router.get("/search", response_model=PaginatedResponse)
def search_videos(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
):
    """Search videos by title, code, or description."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    # Check cache
    cache_key = generate_cache_key("search", q.lower(), page, page_size)
    cached = search_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.search_videos(db, q, page, page_size)
    search_cache.set(cache_key, result)
    return result


@router.get("/search/advanced", response_model=PaginatedResponse)
def advanced_search(
    q: str = Query(None, description="Search query"),
    category: str = Query(None, description="Filter by category"),
    studio: str = Query(None, description="Filter by studio"),
    cast: str = Query(None, description="Filter by cast member"),
    series: str = Query(None, description="Filter by series"),
    date_from: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: str = Query(None, description="Filter to date (YYYY-MM-DD)"),
    min_rating: float = Query(None, ge=0, le=5, description="Minimum rating"),
    sort_by: str = Query("relevance", description="Sort by: relevance, date, rating, views, title"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    db: Session = Depends(get_db)
):
    """
    Advanced search with multiple filters and sorting options.
    
    - q: Text search across title, code, description, studio, series
    - category: Filter by category name
    - studio: Filter by studio name
    - cast: Filter by cast member name
    - series: Filter by series name
    - date_from/date_to: Date range filter
    - min_rating: Minimum average rating
    - sort_by: relevance (default), date, rating, views, title
    - sort_order: desc (default) or asc
    """
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    # Build cache key from all parameters
    cache_key = generate_cache_key(
        "adv_search", 
        q or "", category or "", studio or "", cast or "", series or "",
        date_from or "", date_to or "", min_rating or 0,
        sort_by, sort_order, page, page_size
    )
    cached = search_cache.get(cache_key)
    if cached:
        return cached
    
    result = video_service.advanced_search(
        db, 
        query=q,
        category=category,
        studio=studio,
        cast_name=cast,
        series=series,
        date_from=date_from,
        date_to=date_to,
        min_rating=min_rating,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size
    )
    search_cache.set(cache_key, result)
    return result


@router.get("/search/suggestions")
def get_search_suggestions(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=20),
    db: Session = Depends(get_db)
):
    """
    Get search suggestions for autocomplete.
    Returns suggestions from videos, cast, studios, categories, and series.
    """
    return video_service.get_search_suggestions(db, q, limit)


@router.get("/search/facets")
def get_search_facets(
    q: str = Query(None, description="Optional search query to filter facets"),
    db: Session = Depends(get_db)
):
    """
    Get available filter facets for search refinement.
    Returns counts for categories, studios, cast, and years.
    """
    return video_service.get_search_facets(db, q)


# ============================================
# Homepage Category Endpoints
# ============================================

@router.get("/trending", response_model=PaginatedResponse)
def get_trending_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get trending videos based on views and recency."""
    # Check cache
    cache_key = generate_cache_key("trending", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_trending_videos(db, page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/popular", response_model=PaginatedResponse)
def get_popular_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get most popular videos sorted by view count."""
    # Check cache
    cache_key = generate_cache_key("popular", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_popular_videos(db, page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/top-rated", response_model=PaginatedResponse)
def get_top_rated_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get top-rated videos with minimum rating threshold."""
    # Check cache
    cache_key = generate_cache_key("top-rated", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_top_rated_videos(db, page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/featured", response_model=PaginatedResponse)
def get_featured_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get featured videos based on quality score."""
    # Check cache
    cache_key = generate_cache_key("featured", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_featured_videos(db, page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/new-releases", response_model=PaginatedResponse)
def get_new_releases(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get new releases within the last 90 days."""
    # Check cache
    cache_key = generate_cache_key("new-releases", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_new_releases(db, page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/classics", response_model=PaginatedResponse)
def get_classics(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get classic videos (older than 1 year with good ratings)."""
    # Check cache
    cache_key = generate_cache_key("classics", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = video_service.get_classics(db, page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/{code}", response_model=VideoResponse)
def get_video(code: str, db: Session = Depends(get_db)):
    """Get a single video by code."""
    # Check cache
    cache_key = f"video:{code.upper()}"
    cached = video_detail_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    video = video_service.get_video(db, code)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    
    video_detail_cache.set(cache_key, video)
    return video


@router.get("/{code}/related")
def get_related_videos(
    code: str,
    user_id: str = Query(None, description="User ID for personalized recommendations"),
    limit: int = Query(12, ge=1, le=24),
    strategy: str = Query('balanced', description="Strategy: balanced, similar, personalized, popular, explore"),
    db: Session = Depends(get_db)
):
    """Get personalized video recommendations based on content, watch history, and user preferences."""
    from backend.app.services.recommendation_service import RecommendationEngine
    engine = RecommendationEngine(db)
    return engine.get_recommendations(code, user_id, limit, strategy=strategy)


@router.post("/{code}/view")
def increment_view(code: str, db: Session = Depends(get_db)):
    """Increment view count for a video."""
    success = video_service.increment_views(db, code)
    if not success:
        raise HTTPException(status_code=404, detail="Video not found")
    
    # Invalidate this video's cache
    video_detail_cache.delete(f"video:{code.upper()}")
    
    return {"success": True}


@router.post("/{code}/watch")
def record_watch(
    code: str,
    user_id: str = Query(..., description="User ID for tracking"),
    duration: int = Query(0, ge=0, description="Seconds watched"),
    completed: bool = Query(False, description="Whether video was completed"),
    db: Session = Depends(get_db)
):
    """Record a watch event for recommendation tracking."""
    success = video_service.record_watch(db, code, user_id, duration, completed)
    if not success:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"success": True}


@router.get("/{code}/rating")
def get_rating(code: str, user_id: str = Query(None), db: Session = Depends(get_db)):
    """Get rating statistics for a video, optionally including user's rating."""
    stats = video_service.get_video_rating(db, code)
    
    if user_id:
        stats["user_rating"] = video_service.get_user_rating(db, code, user_id)
    
    return stats


@router.post("/{code}/rating")
def set_rating(
    code: str, 
    rating: int = Query(..., ge=1, le=5),
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Set or update a user's rating for a video."""
    try:
        result = video_service.set_video_rating(db, code, user_id, rating)
        # Invalidate video list caches since ratings changed
        videos_list_cache.clear()
        search_cache.clear()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{code}/rating")
def delete_rating(
    code: str,
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Delete a user's rating for a video."""
    success = video_service.delete_video_rating(db, code, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Rating not found")
    
    # Invalidate video list caches since ratings changed
    videos_list_cache.clear()
    search_cache.clear()
    
    return {"success": True}


# ============================================
# Bookmark Endpoints
# ============================================

@router.get("/{code}/bookmark")
def check_bookmark(
    code: str,
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Check if a video is bookmarked by user."""
    return {"bookmarked": video_service.is_bookmarked(db, code, user_id)}


@router.post("/{code}/bookmark")
def add_bookmark(
    code: str,
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Add a bookmark for a video."""
    try:
        added = video_service.add_bookmark(db, code, user_id)
        return {"success": True, "added": added}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{code}/bookmark")
def remove_bookmark(
    code: str,
    user_id: str = Query(...),
    db: Session = Depends(get_db)
):
    """Remove a bookmark for a video."""
    success = video_service.remove_bookmark(db, code, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Bookmark not found")
    return {"success": True}
