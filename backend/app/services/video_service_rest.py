"""
Video service using Supabase REST API.
Replaces SQLAlchemy-based video_service.py for Railway deployment.
"""
import math
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from backend.app.core.supabase_rest_client import get_supabase_rest
from backend.app.schemas import VideoListItem, VideoResponse, PaginatedResponse


async def _video_to_list_item(video: dict, rating_info: dict = None) -> dict:
    """Convert video dict to list item format."""
    release_date = video.get('release_date', '')
    if release_date and isinstance(release_date, str):
        # Already ISO format
        pass
    elif release_date:
        release_date = release_date.isoformat() if hasattr(release_date, 'isoformat') else str(release_date)
    
    result = {
        "code": video.get('code', ''),
        "title": video.get('title', ''),
        "thumbnail_url": video.get('thumbnail_url') or "",
        "duration": video.get('duration') or "",
        "release_date": release_date or "",
        "studio": video.get('studio') or "",
        "views": video.get('views') or 0,
    }
    if rating_info:
        result["rating_avg"] = rating_info.get("average", 0)
        result["rating_count"] = rating_info.get("count", 0)
    return result


async def _video_to_response(video: dict) -> dict:
    """Convert video dict to full response format."""
    release_date = video.get('release_date', '')
    scraped_at = video.get('scraped_at', '')
    
    # Ensure dates are strings
    if release_date and hasattr(release_date, 'isoformat'):
        release_date = release_date.isoformat()
    if scraped_at and hasattr(scraped_at, 'isoformat'):
        scraped_at = scraped_at.isoformat()
    
    return {
        "code": video.get('code', ''),
        "title": video.get('title', ''),
        "content_id": video.get('content_id') or "",
        "duration": video.get('duration') or "",
        "release_date": release_date or "",
        "thumbnail_url": video.get('thumbnail_url') or "",
        "cover_url": video.get('cover_url') or "",
        "studio": video.get('studio') or "",
        "series": video.get('series') or "",
        "description": video.get('description') or "",
        "embed_urls": video.get('embed_urls') or [],
        "gallery_images": video.get('gallery_images') or [],
        "categories": video.get('_categories', []),  # Will be populated separately
        "cast": video.get('_cast', []),  # Will be populated separately
        "cast_images": video.get('cast_images') or {},
        "scraped_at": scraped_at or "",
        "source_url": video.get('source_url') or "",
        "views": video.get('views') or 0,
    }


async def _get_video_categories(client, video_code: str) -> List[str]:
    """Get categories for a video via REST API."""
    # Query video_categories junction table with category join
    data = await client.get(
        'video_categories',
        select='category_id,categories(name)',
        filters={'video_code': f'eq.{video_code}'}
    )
    if data:
        return [r['categories']['name'] for r in data if r.get('categories')]
    return []


async def _get_video_cast(client, video_code: str) -> List[str]:
    """Get cast members for a video via REST API."""
    data = await client.get(
        'video_cast',
        select='cast_id,cast_members(name)',
        filters={'video_code': f'eq.{video_code}'}
    )
    if data:
        return [r['cast_members']['name'] for r in data if r.get('cast_members')]
    return []


async def _paginate(items: List[dict], total: int, page: int, page_size: int) -> PaginatedResponse:
    """Create paginated response."""
    total_pages = math.ceil(total / page_size) if total > 0 else 1
    
    video_items = [VideoListItem(**item) for item in items]
    
    return PaginatedResponse(
        items=video_items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


async def get_video(code: str) -> Optional[VideoResponse]:
    """Get single video by code."""
    client = get_supabase_rest()
    
    video = await client.get(
        'videos',
        filters={'code': f'eq.{code}'},
        single=True
    )
    
    if not video:
        return None
    
    # Get categories and cast
    categories = await _get_video_categories(client, code)
    cast = await _get_video_cast(client, code)
    video['_categories'] = categories
    video['_cast'] = cast
    
    return VideoResponse(**await _video_to_response(video))


async def get_random_video_code(exclude: List[str] = None) -> Optional[str]:
    """Get a random video code."""
    client = get_supabase_rest()
    
    # Get count first
    count = await client.count('videos')
    if count == 0:
        return None
    
    # Get random offset
    import random
    offset = random.randint(0, max(0, count - 1))
    
    videos = await client.get(
        'videos',
        select='code',
        limit=1,
        offset=offset
    )
    
    if videos and len(videos) > 0:
        code = videos[0].get('code')
        if exclude and code in exclude:
            # Try again with different offset
            offset = random.randint(0, max(0, count - 1))
            videos = await client.get('videos', select='code', limit=1, offset=offset)
            if videos:
                return videos[0].get('code')
        return code
    return None


async def get_videos(
    page: int = 1,
    page_size: int = 20,
    sort_by: str = "release_date",
    sort_order: str = "desc"
) -> PaginatedResponse:
    """Get paginated list of videos."""
    client = get_supabase_rest()
    
    # Map sort_by to actual columns
    sort_column = sort_by if sort_by in ['release_date', 'title', 'views', 'scraped_at'] else 'release_date'
    order = f"{sort_column}.{sort_order}"
    
    offset = (page - 1) * page_size
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order=order,
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def search_videos(query: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Search videos by title, code, or description."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Use ilike for case-insensitive search
    # Search in title, code, description
    # Note: Supabase REST API doesn't support OR filters directly, so we'll use code or title
    search_term = f'*{query}*'
    
    # Try to search by code first (exact-ish match)
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'or': f'(code.ilike.{search_term},title.ilike.{search_term},description.ilike.{search_term})'},
        order='release_date.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_videos_by_category(category: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos in a category."""
    client = get_supabase_rest()
    
    # First get category ID
    cat_data = await client.get('categories', filters={'name': f'eq.{category}'}, single=True)
    if not cat_data:
        return await _paginate([], 0, page, page_size)
    
    cat_id = cat_data['id']
    offset = (page - 1) * page_size
    
    # Get video codes from junction table
    junctions, total = await client.get_with_count(
        'video_categories',
        select='video_code',
        filters={'category_id': f'eq.{cat_id}'},
        limit=page_size,
        offset=offset
    )
    
    if not junctions:
        return await _paginate([], 0, page, page_size)
    
    # Get videos by codes
    codes = [j['video_code'] for j in junctions]
    codes_filter = ','.join(f'"{c}"' for c in codes)
    
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'code': f'in.({codes_filter})'},
        order='release_date.desc'
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_videos_by_cast(cast_name: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos featuring a cast member."""
    client = get_supabase_rest()
    
    # First get cast ID
    cast_data = await client.get('cast_members', filters={'name': f'eq.{cast_name}'}, single=True)
    if not cast_data:
        return await _paginate([], 0, page, page_size)
    
    cast_id = cast_data['id']
    offset = (page - 1) * page_size
    
    # Get video codes from junction table
    junctions, total = await client.get_with_count(
        'video_cast',
        select='video_code',
        filters={'cast_id': f'eq.{cast_id}'},
        limit=page_size,
        offset=offset
    )
    
    if not junctions:
        return await _paginate([], 0, page, page_size)
    
    # Get videos by codes
    codes = [j['video_code'] for j in junctions]
    codes_filter = ','.join(f'"{c}"' for c in codes)
    
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'code': f'in.({codes_filter})'},
        order='release_date.desc'
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_videos_by_studio(studio: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos from a studio."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'studio': f'eq.{studio}'},
        order='release_date.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_videos_by_series(series: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos from a series."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'series': f'eq.{series}'},
        order='release_date.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


# ============================================
# Homepage Categories
# ============================================

async def get_trending_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get trending videos based on views and recency."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Trending = recent + high views
    # Get videos from last 30 days sorted by views
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'scraped_at': f'gte.{thirty_days_ago}'},
        order='views.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_popular_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get most popular videos by view count."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='views.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_new_releases(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get new releases within the last 90 days."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    ninety_days_ago = (datetime.utcnow() - timedelta(days=90)).isoformat()
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'release_date': f'gte.{ninety_days_ago}'},
        order='release_date.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_featured_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get featured videos - high quality content with thumbnails and descriptions."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Featured = has thumbnail, has description, sorted by views
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={
            'thumbnail_url': 'neq.',
            'description': 'neq.'
        },
        order='views.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_top_rated_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get top-rated videos."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Get videos with ratings - for simplicity, just get popular videos
    # Real top-rated would need a join with video_ratings table
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='views.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def get_classics(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get classic videos (older than 1 year)."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    one_year_ago = (datetime.utcnow() - timedelta(days=365)).isoformat()
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'release_date': f'lte.{one_year_ago}'},
        order='views.desc',
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


# ============================================
# Views and Ratings
# ============================================

async def increment_views(code: str) -> bool:
    """Increment view count for a video."""
    client = get_supabase_rest()
    
    # Get current views
    video = await client.get('videos', select='views', filters={'code': f'eq.{code}'}, single=True)
    if not video:
        return False
    
    current_views = video.get('views') or 0
    
    # Update views
    result = await client.update(
        'videos',
        {'views': current_views + 1},
        filters={'code': f'eq.{code}'},
        use_admin=True
    )
    
    return result is not None


async def get_video_rating(code: str) -> dict:
    """Get rating statistics for a video."""
    client = get_supabase_rest()
    
    ratings = await client.get(
        'video_ratings',
        select='rating',
        filters={'video_code': f'eq.{code}'}
    )
    
    if not ratings:
        return {
            "average": 0,
            "count": 0,
            "distribution": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        }
    
    total = sum(r['rating'] for r in ratings)
    count = len(ratings)
    distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for r in ratings:
        distribution[r['rating']] += 1
    
    return {
        "average": round(total / count, 1),
        "count": count,
        "distribution": distribution
    }


async def get_user_rating(code: str, user_id: str) -> Optional[int]:
    """Get a user's rating for a video."""
    client = get_supabase_rest()
    
    rating = await client.get(
        'video_ratings',
        select='rating',
        filters={'video_code': f'eq.{code}', 'user_id': f'eq.{user_id}'},
        single=True
    )
    
    return rating['rating'] if rating else None


async def set_video_rating(code: str, user_id: str, rating: int) -> dict:
    """Set or update a user's rating for a video."""
    if rating < 1 or rating > 5:
        raise ValueError("Rating must be between 1 and 5")
    
    client = get_supabase_rest()
    
    # Check if video exists
    video = await client.get('videos', select='code', filters={'code': f'eq.{code}'}, single=True)
    if not video:
        raise ValueError("Video not found")
    
    # Try upsert
    result = await client.insert(
        'video_ratings',
        {
            'video_code': code,
            'user_id': user_id,
            'rating': rating,
            'updated_at': datetime.utcnow().isoformat()
        },
        upsert=True,
        use_admin=True
    )
    
    # Return updated stats
    stats = await get_video_rating(code)
    stats["user_rating"] = rating
    return stats


async def delete_video_rating(code: str, user_id: str) -> bool:
    """Delete a user's rating for a video."""
    client = get_supabase_rest()
    
    return await client.delete(
        'video_ratings',
        filters={'video_code': f'eq.{code}', 'user_id': f'eq.{user_id}'},
        use_admin=True
    )


# ============================================
# Bookmarks
# ============================================

async def is_bookmarked(code: str, user_id: str) -> bool:
    """Check if a video is bookmarked by user."""
    client = get_supabase_rest()
    
    bookmark = await client.get(
        'video_bookmarks',
        select='id',
        filters={'video_code': f'eq.{code}', 'user_id': f'eq.{user_id}'},
        single=True
    )
    
    return bookmark is not None


async def add_bookmark(code: str, user_id: str) -> bool:
    """Add a bookmark for a video."""
    client = get_supabase_rest()
    
    # Check if video exists
    video = await client.get('videos', select='code', filters={'code': f'eq.{code}'}, single=True)
    if not video:
        raise ValueError("Video not found")
    
    # Check if already bookmarked
    if await is_bookmarked(code, user_id):
        return False
    
    result = await client.insert(
        'video_bookmarks',
        {
            'video_code': code,
            'user_id': user_id,
            'created_at': datetime.utcnow().isoformat()
        },
        use_admin=True
    )
    
    return result is not None


async def remove_bookmark(code: str, user_id: str) -> bool:
    """Remove a bookmark for a video."""
    client = get_supabase_rest()
    
    return await client.delete(
        'video_bookmarks',
        filters={'video_code': f'eq.{code}', 'user_id': f'eq.{user_id}'},
        use_admin=True
    )


async def get_bookmark_count(user_id: str) -> int:
    """Get user's total bookmark count."""
    client = get_supabase_rest()
    return await client.count('video_bookmarks', filters={'user_id': f'eq.{user_id}'})


async def get_user_bookmarks(user_id: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get user's bookmarked videos."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Get bookmark video codes
    bookmarks, total = await client.get_with_count(
        'video_bookmarks',
        select='video_code',
        filters={'user_id': f'eq.{user_id}'},
        order='created_at.desc',
        limit=page_size,
        offset=offset
    )
    
    if not bookmarks:
        return await _paginate([], 0, page, page_size)
    
    codes = [b['video_code'] for b in bookmarks]
    codes_filter = ','.join(f'"{c}"' for c in codes)
    
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'code': f'in.({codes_filter})'}
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


# ============================================
# Watch History
# ============================================

async def record_watch(code: str, user_id: str, duration: int = 0, completed: bool = False) -> bool:
    """Record a watch event."""
    client = get_supabase_rest()
    
    # Check if video exists
    video = await client.get('videos', select='code', filters={'code': f'eq.{code}'}, single=True)
    if not video:
        return False
    
    result = await client.insert(
        'watch_history',
        {
            'video_code': code,
            'user_id': user_id,
            'duration_watched': duration,
            'completed': completed,
            'watched_at': datetime.utcnow().isoformat()
        },
        upsert=True,
        use_admin=True
    )
    
    return result is not None


async def get_watch_history(user_id: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get user's watch history."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    history, total = await client.get_with_count(
        'watch_history',
        select='video_code',
        filters={'user_id': f'eq.{user_id}'},
        order='watched_at.desc',
        limit=page_size,
        offset=offset
    )
    
    if not history:
        return await _paginate([], 0, page, page_size)
    
    codes = [h['video_code'] for h in history]
    codes_filter = ','.join(f'"{c}"' for c in codes)
    
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'code': f'in.({codes_filter})'}
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


async def clear_watch_history(user_id: str) -> dict:
    """Clear user's watch history."""
    client = get_supabase_rest()
    
    success = await client.delete(
        'watch_history',
        filters={'user_id': f'eq.{user_id}'},
        use_admin=True
    )
    
    return {"success": success}


async def merge_watch_history(from_user_id: str, to_user_id: str) -> dict:
    """Merge anonymous watch history into logged-in user account."""
    client = get_supabase_rest()
    
    # Get anonymous user's history
    history = await client.get(
        'watch_history',
        select='video_code,duration_watched,completed,watched_at',
        filters={'user_id': f'eq.{from_user_id}'}
    )
    
    merged = 0
    for h in history:
        result = await client.insert(
            'watch_history',
            {
                'video_code': h['video_code'],
                'user_id': to_user_id,
                'duration_watched': h['duration_watched'],
                'completed': h['completed'],
                'watched_at': h['watched_at']
            },
            upsert=True,
            use_admin=True
        )
        if result:
            merged += 1
    
    # Delete anonymous history
    await client.delete('watch_history', filters={'user_id': f'eq.{from_user_id}'}, use_admin=True)
    
    return {"merged": merged}


# ============================================
# Categories, Studios, Series helpers
# ============================================

async def get_all_categories() -> List[dict]:
    """Get all categories with video counts."""
    client = get_supabase_rest()
    
    categories = await client.get('categories', select='id,name')
    
    result = []
    for cat in categories:
        count = await client.count('video_categories', filters={'category_id': f'eq.{cat["id"]}'})
        result.append({'name': cat['name'], 'count': count})
    
    result.sort(key=lambda x: x['count'], reverse=True)
    return result


async def get_all_studios() -> List[dict]:
    """Get all studios with video counts."""
    client = get_supabase_rest()
    
    # This is trickier with REST API - get distinct studios and count
    # For now, get videos and aggregate locally
    videos = await client.get('videos', select='studio')
    
    studio_counts = {}
    for v in videos:
        studio = v.get('studio')
        if studio:
            studio_counts[studio] = studio_counts.get(studio, 0) + 1
    
    result = [{'name': name, 'count': count} for name, count in studio_counts.items()]
    result.sort(key=lambda x: x['count'], reverse=True)
    return result


async def get_all_cast() -> List[dict]:
    """Get all cast members with video counts."""
    client = get_supabase_rest()
    
    cast_members = await client.get('cast_members', select='id,name')
    
    result = []
    for cm in cast_members:
        count = await client.count('video_cast', filters={'cast_id': f'eq.{cm["id"]}'})
        if count > 0:
            result.append({'name': cm['name'], 'count': count})
    
    result.sort(key=lambda x: x['count'], reverse=True)
    return result[:100]  # Limit to top 100


async def get_all_series() -> List[dict]:
    """Get all series with video counts."""
    client = get_supabase_rest()
    
    videos = await client.get('videos', select='series')
    
    series_counts = {}
    for v in videos:
        series = v.get('series')
        if series:
            series_counts[series] = series_counts.get(series, 0) + 1
    
    result = [{'name': name, 'count': count} for name, count in series_counts.items()]
    result.sort(key=lambda x: x['count'], reverse=True)
    return result


async def get_search_suggestions(query: str, limit: int = 10) -> dict:
    """Get search suggestions based on partial query."""
    if not query or len(query) < 2:
        return {"suggestions": []}
    
    client = get_supabase_rest()
    suggestions = []
    
    # Video code/title suggestions
    videos = await client.get(
        'videos',
        select='code,title',
        filters={'or': f'(code.ilike.*{query}*,title.ilike.*{query}*)'},
        limit=5
    )
    
    for v in videos:
        suggestions.append({
            "type": "video",
            "value": v['code'],
            "label": f"{v['code']} - {v['title'][:50]}" if len(v['title']) > 50 else f"{v['code']} - {v['title']}",
            "priority": 1
        })
    
    # Cast suggestions
    cast = await client.get(
        'cast_members',
        select='name',
        filters={'name': f'ilike.*{query}*'},
        limit=3
    )
    
    for c in cast:
        suggestions.append({
            "type": "cast",
            "value": c['name'],
            "label": c['name'],
            "priority": 2
        })
    
    suggestions.sort(key=lambda x: x['priority'])
    return {"suggestions": suggestions[:limit]}


async def get_search_facets(query: str = None) -> dict:
    """Get available filter facets for search refinement."""
    # Simplified facets - just return top categories, studios, cast
    client = get_supabase_rest()
    
    categories = await get_all_categories()
    studios = await get_all_studios()
    
    return {
        "categories": categories[:20],
        "studios": studios[:20],
        "cast": [],
        "years": []
    }


async def advanced_search(
    query: str = None,
    category: str = None,
    studio: str = None,
    cast_name: str = None,
    series: str = None,
    date_from: str = None,
    date_to: str = None,
    min_rating: float = None,
    sort_by: str = "relevance",
    sort_order: str = "desc",
    page: int = 1,
    page_size: int = 20
) -> PaginatedResponse:
    """Advanced search with multiple filters."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    filters = {}
    
    # Build filters
    if studio:
        filters['studio'] = f'eq.{studio}'
    if series:
        filters['series'] = f'eq.{series}'
    if date_from:
        filters['release_date'] = f'gte.{date_from}'
    if date_to:
        if 'release_date' in filters:
            # Can't combine gte and lte in same key
            pass
        else:
            filters['release_date'] = f'lte.{date_to}'
    
    if query:
        filters['or'] = f'(code.ilike.*{query}*,title.ilike.*{query}*,description.ilike.*{query}*)'
    
    # Determine order
    order_map = {
        'date': 'release_date',
        'views': 'views',
        'title': 'title',
        'relevance': 'release_date'
    }
    order_col = order_map.get(sort_by, 'release_date')
    order = f'{order_col}.{sort_order}'
    
    videos, total = await client.get_with_count(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters=filters if filters else None,
        order=order,
        limit=page_size,
        offset=offset
    )
    
    items = [await _video_to_list_item(v) for v in videos]
    return await _paginate(items, total, page, page_size)


# ============================================
# Personalized Recommendations (simplified)
# ============================================

async def get_personalized_recommendations(user_id: str, page: int = 1, page_size: int = 12) -> PaginatedResponse:
    """Get personalized 'For You' recommendations - simplified version."""
    # For REST API, just return popular videos as recommendations
    return await get_popular_videos(page, page_size)
