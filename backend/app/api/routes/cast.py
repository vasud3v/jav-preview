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
    """Get all cast members with images and video counts (no limit)."""
    cached = cast_cache.get("all_cast_images")
    if cached:
        return cached
    
    # Use get_all_cast_with_images to fetch ALL cast with images
    result = await video_service.get_all_cast_with_images()
    cast_cache.set("all_cast_images", result)
    return result


@router.get("/featured", response_model=list[CastWithImageResponse])
async def get_featured_cast(
    limit: int = Query(20, ge=1, le=50),
    no_cache: bool = Query(False, description="Skip cache for testing")
):
    """Get featured cast members with images."""
    # Check cache (unless no_cache is True)
    cache_key = f"featured:{limit}"
    if not no_cache:
        cached = cast_featured_cache.get(cache_key)
        if cached:
            return cached
    
    # Use get_cast_with_images to fetch cast with images from videos
    result = await video_service.get_cast_with_images(limit)
    cast_featured_cache.set(cache_key, result)
    return result


@router.get("/debug/stats")
async def get_cast_debug_stats():
    """Debug endpoint to check cast data quality."""
    from app.core.supabase_rest_client import get_supabase_rest
    client = get_supabase_rest()
    
    # Count total cast members
    cast_members = await client.get('cast_members', select='id')
    total_cast = len(cast_members) if cast_members else 0
    
    # Count cast with videos
    video_cast = await client.get('video_cast', select='cast_id')
    unique_cast_with_videos = len(set(vc['cast_id'] for vc in video_cast if vc.get('cast_id'))) if video_cast else 0
    
    # Count videos with cast_images
    videos_with_images = await client.get(
        'videos',
        select='cast_images',
        filters={'cast_images': 'not.is.null'}
    )
    videos_with_cast_images = len(videos_with_images) if videos_with_images else 0
    
    # Count unique cast names in cast_images
    unique_cast_in_images = set()
    cast_name_to_image = {}
    for v in videos_with_images or []:
        cast_images = v.get('cast_images') or {}
        if isinstance(cast_images, dict):
            for name, url in cast_images.items():
                unique_cast_in_images.add(name)
                if name not in cast_name_to_image:
                    cast_name_to_image[name] = url
    
    # Get cast members from database
    all_cast_members = await client.get('cast_members', select='id,name')
    cast_member_names = {cm['name'] for cm in all_cast_members} if all_cast_members else set()
    
    # Find cast names that are in images but NOT in cast_members table
    missing_from_db = unique_cast_in_images - cast_member_names
    
    # Find cast names that are in cast_members but NOT in images
    missing_images = cast_member_names - unique_cast_in_images
    
    return {
        "total_cast_members": total_cast,
        "cast_with_videos": unique_cast_with_videos,
        "videos_with_cast_images": videos_with_cast_images,
        "unique_cast_with_images": len(unique_cast_in_images),
        "cast_in_db_with_images": len(cast_member_names & unique_cast_in_images),
        "cast_in_images_but_not_in_db": len(missing_from_db),
        "cast_in_db_but_no_images": len(missing_images),
        "sample_missing_from_db": list(missing_from_db)[:20],
        "sample_cast_with_images": list(unique_cast_in_images)[:20]
    }


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
