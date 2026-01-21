"""Video routes using Supabase REST API."""
from fastapi import APIRouter, HTTPException, Query

from app.core.config import settings
from app.schemas import VideoResponse, PaginatedResponse, HomeFeedResponse
from app.services import video_service_rest as video_service
from app.core.cache import (
    videos_list_cache, video_detail_cache, search_cache, generate_cache_key
)

router = APIRouter(prefix="/videos", tags=["videos"])


# ============================================
# User-specific routes (must be before /{code} routes)
# ============================================

@router.get("/feed/home", response_model=HomeFeedResponse)
async def get_home_feed(
    user_id: str = Query(...)
):
    """
    Get unified home feed with distinct videos for each section.
    Prevents duplicates across Featured, Trending, Popular, New, and Classics.
    Cached for better performance.
    """
    # Use cache for home feed
    cache_key = f"home_feed:{user_id}"
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    result = await video_service.get_home_feed(user_id)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/user/bookmarks", response_model=PaginatedResponse)
async def get_bookmarks(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(None)
):
    """Get user's bookmarked videos."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)
    
    return await video_service.get_user_bookmarks(user_id, page, page_size)


@router.get("/user/for-you", response_model=PaginatedResponse)
async def get_for_you(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=24)
):
    """Get personalized 'For You' recommendations based on watch history and preferences."""
    return await video_service.get_personalized_recommendations(user_id, page, page_size)


@router.get("/user/discover")
async def get_discover_more(
    user_id: str = Query(...),
    batch: int = Query(0, ge=0, description="Batch number for infinite scroll"),
    batch_size: int = Query(12, ge=6, le=24),
    seen: str = Query('', description="Comma-separated list of already seen video codes")
):
    """
    Get infinite scroll recommendations.
    Each batch uses different strategy for variety.
    Returns: items, has_more, batch, strategy
    """
    # Simplified for REST API - just return popular videos
    page = batch + 1
    result = await video_service.get_popular_videos(page, batch_size)
    return {
        "items": [item.model_dump() for item in result.items],
        "has_more": result.page < result.total_pages,
        "batch": batch,
        "strategy": "popular"
    }


@router.get("/user/bookmarks/count")
async def get_bookmark_count(user_id: str = Query(...)):
    """Get user's total bookmark count."""
    count = await video_service.get_bookmark_count(user_id)
    return {"count": count}


@router.get("/user/history", response_model=PaginatedResponse)
async def get_watch_history(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50)
):
    """Get user's watch history."""
    return await video_service.get_watch_history(user_id, page, page_size)


@router.delete("/user/history")
async def clear_watch_history(user_id: str = Query(...)):
    """Clear user's watch history."""
    return await video_service.clear_watch_history(user_id)


@router.post("/user/merge-history")
async def merge_history(
    from_user_id: str = Query(..., description="Anonymous user ID to merge from"),
    to_user_id: str = Query(..., description="Logged-in user ID to merge to")
):
    """Merge anonymous watch history and ratings into logged-in user account."""
    return await video_service.merge_watch_history(from_user_id, to_user_id)


# ============================================
# Video list routes
# ============================================

@router.get("", response_model=PaginatedResponse)
async def list_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(None),
    sort_by: str = Query("release_date"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$")
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
    result = await video_service.get_videos(page, page_size, sort_by, sort_order)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/random")
async def get_random_video(
    exclude: str = Query(None, description="Comma-separated list of video codes to exclude")
):
    """Get a random video code for the random button, excluding recently viewed."""
    exclude_list = []
    if exclude:
        exclude_list = [code.strip().upper() for code in exclude.split(',') if code.strip()]
    
    code = await video_service.get_random_video_code(exclude_list)
    if not code:
        raise HTTPException(status_code=404, detail="No videos found")
    return {"code": code}


@router.get("/search", response_model=PaginatedResponse)
async def search_videos(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(None)
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
    result = await video_service.search_videos(q, page, page_size)
    search_cache.set(cache_key, result)
    return result


@router.get("/search/advanced", response_model=PaginatedResponse)
async def advanced_search(
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
    page_size: int = Query(None)
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
    
    result = await video_service.advanced_search(
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
async def get_search_suggestions(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=20)
):
    """
    Get search suggestions for autocomplete.
    Returns suggestions from videos, cast, studios, categories, and series.
    """
    return await video_service.get_search_suggestions(q, limit)


@router.get("/search/facets")
async def get_search_facets(
    q: str = Query(None, description="Optional search query to filter facets")
):
    """
    Get available filter facets for search refinement.
    Returns counts for categories, studios, cast, and years.
    """
    return await video_service.get_search_facets(q)


# ============================================
# Homepage Category Endpoints
# ============================================

@router.get("/trending", response_model=PaginatedResponse)
async def get_trending_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50)
):
    """Get trending videos based on views and recency."""
    print(f"DEBUG: Route get_trending_videos hit. Service file: {video_service.__file__}")
    # Fetch directly without caching for now
    result = await video_service.get_trending_videos(page, page_size)
    return result


@router.get("/popular", response_model=PaginatedResponse)
async def get_popular_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50)
):
    """Get most popular videos sorted by view count."""
    # Fetch directly without caching for now
    result = await video_service.get_popular_videos(page, page_size)
    return result


@router.get("/top-rated", response_model=PaginatedResponse)
async def get_top_rated_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50)
):
    """Get top-rated videos with minimum rating threshold."""
    # Fetch directly without caching for now
    result = await video_service.get_top_rated_videos(page, page_size)
    return result


@router.get("/featured", response_model=PaginatedResponse)
async def get_featured_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50)
):
    """Get featured videos based on quality score."""
    # Fetch directly without caching for now
    result = await video_service.get_featured_videos(page, page_size)
    return result


@router.get("/new-releases", response_model=PaginatedResponse)
async def get_new_releases(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50)
):
    """Get new releases within the last 90 days."""
    # Check cache
    cache_key = generate_cache_key("new-releases", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = await video_service.get_new_releases(page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/classics", response_model=PaginatedResponse)
async def get_classics(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50)
):
    """Get classic videos (older than 1 year with good ratings)."""
    # Check cache
    cache_key = generate_cache_key("classics", page, page_size)
    cached = videos_list_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    result = await video_service.get_classics(page, page_size)
    videos_list_cache.set(cache_key, result)
    return result


@router.get("/{code}", response_model=VideoResponse)
async def get_video(code: str):
    """Get a single video by code."""
    # Check cache
    cache_key = f"video:{code.upper()}"
    cached = video_detail_cache.get(cache_key)
    if cached:
        return cached
    
    # Fetch and cache
    video = await video_service.get_video(code)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    
    video_detail_cache.set(cache_key, video)
    return video


@router.get("/{code}/related")
async def get_related_videos(
    code: str,
    user_id: str = Query(None, description="User ID for personalized recommendations"),
    limit: int = Query(12, ge=1, le=24),
    strategy: str = Query('balanced', description="Strategy: balanced, similar, personalized, popular, explore")
):
    """Get personalized video recommendations based on content, watch history, and user preferences."""
    result = await video_service.get_related_videos(code, user_id, limit, strategy)
    # Return array directly to match frontend expectations
    return [item.model_dump() for item in result.items]


@router.post("/{code}/view")
async def increment_view(code: str):
    """Increment view count for a video."""
    success = await video_service.increment_views(code)
    if not success:
        raise HTTPException(status_code=404, detail="Video not found")
    
    # Invalidate this video's cache
    video_detail_cache.delete(f"video:{code.upper()}")
    
    return {"success": True}


@router.post("/{code}/watch")
async def record_watch(
    code: str,
    user_id: str = Query(..., description="User ID for tracking"),
    duration: int = Query(0, ge=0, description="Seconds watched"),
    completed: bool = Query(False, description="Whether video was completed")
):
    """Record a watch event for recommendation tracking."""
    # Fire and forget - don't fail the request if tracking fails
    await video_service.record_watch(code, user_id, duration, completed)
    return {"success": True}


@router.get("/{code}/rating")
async def get_rating(code: str, user_id: str = Query(None)):
    """Get rating statistics for a video, optionally including user's rating."""
    stats = await video_service.get_video_rating(code)
    
    if user_id:
        stats["user_rating"] = await video_service.get_user_rating(code, user_id)
    
    return stats


@router.post("/{code}/rating")
async def set_rating(
    code: str, 
    rating: int = Query(..., ge=1, le=5),
    user_id: str = Query(...)
):
    """Set or update a user's rating for a video."""
    try:
        result = await video_service.set_video_rating(code, user_id, rating)
        # Invalidate video list caches since ratings changed
        videos_list_cache.clear()
        search_cache.clear()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{code}/rating")
async def delete_rating(
    code: str,
    user_id: str = Query(...)
):
    """Delete a user's rating for a video."""
    success = await video_service.delete_video_rating(code, user_id)
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
async def check_bookmark(
    code: str,
    user_id: str = Query(...)
):
    """Check if a video is bookmarked by user."""
    bookmarked = await video_service.is_bookmarked(code, user_id)
    return {"bookmarked": bookmarked}


@router.post("/{code}/bookmark")
async def add_bookmark(
    code: str,
    user_id: str = Query(...)
):
    """Add a bookmark for a video."""
    try:
        added = await video_service.add_bookmark(code, user_id)
        return {"success": True, "added": added}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{code}/bookmark")
async def remove_bookmark(
    code: str,
    user_id: str = Query(...)
):
    """Remove a bookmark for a video."""
    success = await video_service.remove_bookmark(code, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Bookmark not found")
    return {"success": True}
