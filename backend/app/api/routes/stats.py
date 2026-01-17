"""Stats routes using Supabase REST API."""
from fastapi import APIRouter

from app.schemas import StatsResponse
from app.core.supabase_rest_client import get_supabase_rest
from app.core.cache import stats_cache, get_all_cache_stats, clear_all_caches

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("", response_model=StatsResponse)
async def get_stats():
    """Get database statistics."""
    # Check cache
    cached = stats_cache.get("stats")
    if cached:
        return cached
    
    # Fetch from Supabase REST API
    client = get_supabase_rest()
    
    video_count = await client.count("videos")
    category_count = await client.count("categories")
    cast_count = await client.count("cast_members")
    
    # Get studio count from videos
    videos = await client.get("videos", select="studio")
    studios = set(v.get("studio") for v in videos if v.get("studio"))
    
    result = {
        "total_videos": video_count,
        "categories_count": category_count,
        "cast_count": cast_count,
        "studios_count": len(studios),
        "oldest_video": None,
        "newest_video": None,
        "database_size_bytes": 0
    }
    
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
