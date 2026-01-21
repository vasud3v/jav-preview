"""
Video service using Supabase REST API.
Replaces SQLAlchemy-based video_service.py for Railway deployment.
"""
import asyncio
import math
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from app.core.supabase_rest_client import get_supabase_rest
from app.schemas import VideoListItem, VideoResponse, PaginatedResponse, HomeFeedResponse


# ============================================
# Feed Configuration Constants
# ============================================

# Section sizes
FEED_SECTION_SIZE = 12

# Candidate batch sizes (larger to account for duplicate filtering)
FEATURED_BATCH_SIZE = 50
TRENDING_BATCH_SIZE = 60
POPULAR_BATCH_SIZE = 60
TOP_RATED_BATCH_SIZE = 60
NEW_RELEASES_BATCH_SIZE = 60
CLASSICS_BATCH_SIZE = 60
MOST_LIKED_BATCH_SIZE = 60

# Time windows
TRENDING_WINDOW_DAYS = 30
CLASSICS_AGE_YEARS = 1
MOST_LIKED_WINDOW_DAYS = 30

# Quality thresholds
MIN_RATINGS_FOR_TOP_RATED = 1
MIN_LIKES_FOR_FEATURED_BOOST = 5

# Scoring weights for personalization
WEIGHT_STUDIO = 30
WEIGHT_SERIES = 25
WEIGHT_CATEGORY = 25
WEIGHT_CAST = 20

# Like algorithm weights
LIKE_WEIGHT_IN_TRENDING = 0.3  # 30% weight for likes in trending score
LIKE_RATIO_BOOST = 2.0  # Multiplier for high like-to-view ratio


async def _get_ratings_for_videos(video_codes: list) -> dict:
    """Get rating statistics for multiple videos efficiently."""
    if not video_codes:
        return {}
    
    client = get_supabase_rest()
    
    # Fetch all ratings for these videos
    codes_filter = ','.join(f'"{code}"' for code in video_codes)
    ratings_data = await client.get(
        'video_ratings',
        select='video_code,rating',
        filters={'video_code': f'in.({codes_filter})'}
    )
    
    if not ratings_data:
        return {}
    
    # Calculate stats per video
    from collections import defaultdict
    rating_stats = defaultdict(lambda: {'sum': 0, 'count': 0})
    
    for rating in ratings_data:
        code = rating.get('video_code')
        rating_val = rating.get('rating', 0)
        rating_stats[code]['sum'] += rating_val
        rating_stats[code]['count'] += 1
    
    # Convert to average
    result = {}
    for code, stats in rating_stats.items():
        if stats['count'] > 0:
            result[code] = {
                'average': round(stats['sum'] / stats['count'], 1),
                'count': stats['count']
            }
    
    return result


async def _get_likes_for_videos(video_codes: list) -> dict:
    """Get like counts for multiple videos efficiently."""
    if not video_codes:
        return {}
    
    try:
        client = get_supabase_rest()
        
        # Fetch all likes for these videos
        codes_filter = ','.join(f'"{code}"' for code in video_codes)
        likes_data = await client.get(
            'video_likes',
            select='video_code',
            filters={'video_code': f'in.({codes_filter})'}
        )
        
        if not likes_data:
            return {}
        
        # Count likes per video
        from collections import defaultdict
        like_counts = defaultdict(int)
        
        for like in likes_data:
            code = like.get('video_code')
            if code:
                like_counts[code] += 1
        
        return dict(like_counts)
    except Exception as e:
        print(f"Error fetching likes for videos: {e}")
        return {}


async def _videos_to_list_items(videos: list) -> list:
    """Convert list of videos to list items with ratings."""
    if not videos:
        return []
    
    video_codes = [v['code'] for v in videos]
    ratings = await _get_ratings_for_videos(video_codes)
    
    return [await _video_to_list_item(v, ratings.get(v['code'])) for v in videos]


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
        "rating_avg": 0,
        "rating_count": 0,
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


async def _get_categories_for_videos(client, video_codes: List[str]) -> Dict[str, List[str]]:
    """Get categories for multiple videos efficiently."""
    if not video_codes:
        return {}

    codes_filter = ','.join(f'"{code}"' for code in video_codes)
    data = await client.get(
        'video_categories',
        select='video_code,categories(name)',
        filters={'video_code': f'in.({codes_filter})'}
    )

    result = {}
    if data:
        for r in data:
            if r.get('categories'):
                video_code = r['video_code']
                if video_code not in result:
                    result[video_code] = []
                result[video_code].append(r['categories']['name'])
    return result


async def _get_cast_for_videos(client, video_codes: List[str]) -> Dict[str, List[str]]:
    """Get cast for multiple videos efficiently."""
    if not video_codes:
        return {}

    codes_filter = ','.join(f'"{code}"' for code in video_codes)
    data = await client.get(
        'video_cast',
        select='video_code,cast_members(name)',
        filters={'video_code': f'in.({codes_filter})'}
    )

    result = {}
    if data:
        for r in data:
            if r.get('cast_members'):
                video_code = r['video_code']
                if video_code not in result:
                    result[video_code] = []
                result[video_code].append(r['cast_members']['name'])
    return result


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
    
    # Fetch ratings for these videos
    video_codes = [v['code'] for v in videos]
    ratings = await _get_ratings_for_videos(video_codes)
    
    items = [await _video_to_list_item(v, ratings.get(v['code'])) for v in videos]
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
    
    # Fetch ratings for these videos
    video_codes = [v['code'] for v in videos]
    ratings = await _get_ratings_for_videos(video_codes)
    
    items = [await _video_to_list_item(v, ratings.get(v['code'])) for v in videos]
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
    
    items = await _videos_to_list_items(videos)
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
    
    items = await _videos_to_list_items(videos)
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
    
    items = await _videos_to_list_items(videos)
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
    
    items = await _videos_to_list_items(videos)
    return await _paginate(items, total, page, page_size)


# ============================================
# Homepage Categories
# ============================================

async def get_trending_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get trending videos - most recently scraped content."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Trending = sorted by scraped_at (most recently added to our database)
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='scraped_at.desc',
        limit=page_size,
        offset=offset  # Start from beginning for trending
    )
    
    if not videos:
        return await _paginate([], 0, page, page_size)

    items = await _videos_to_list_items(videos)
    return await _paginate(items, 10000, page, page_size)


async def get_popular_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get popular videos - offset by 100 to differ from trending."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Popular = skip first 100 to show different videos than trending
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='scraped_at.desc',
        limit=page_size,
        offset=offset + 100  # Skip first 100 videos
    )
    
    if not videos:
        videos = await client.get(
            'videos',
            select='code,title,thumbnail_url,duration,release_date,studio,views',
            order='scraped_at.desc',
            limit=page_size,
            offset=offset
        )
    
    if not videos:
        return await _paginate([], 0, page, page_size)

    items = await _videos_to_list_items(videos)
    return await _paginate(items, 10000, page, page_size)


async def get_new_releases(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get videos with the most recent release dates."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # New releases = sorted by release_date descending (newest first)
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='release_date.desc',
        limit=page_size,
        offset=offset
    )
    
    if not videos:
        return await _paginate([], 0, page, page_size)
    
    items = await _videos_to_list_items(videos)
    return await _paginate(items, 10000, page, page_size)


async def get_featured_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get featured videos - offset by 200 to differ from other sections."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Featured = skip first 200 to show completely different videos
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'thumbnail_url': 'neq.'},
        order='scraped_at.desc',
        limit=page_size,
        offset=offset + 200  # Skip first 200 videos
    )
    
    if not videos:
        videos = await client.get(
            'videos',
            select='code,title,thumbnail_url,duration,release_date,studio,views',
            order='scraped_at.desc',
            limit=page_size,
            offset=offset
        )
    
    if not videos:
        return await _paginate([], 0, page, page_size)
    
    items = await _videos_to_list_items(videos)
    return await _paginate(items, 10000, page, page_size)


async def get_top_rated_videos(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get top rated videos based on actual ratings."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    try:
        # Get all ratings
        ratings_data = await client.get(
            'video_ratings',
            select='video_code,rating'
        )
        
        if ratings_data and len(ratings_data) > 0:
            # Calculate average rating per video
            from collections import defaultdict
            rating_stats = defaultdict(lambda: {'sum': 0, 'count': 0})
            
            for rating in ratings_data:
                code = rating.get('video_code')
                rating_val = rating.get('rating', 0)
                rating_stats[code]['sum'] += rating_val
                rating_stats[code]['count'] += 1
            
            # Get videos with best ratings (min threshold)
            top_codes = sorted(
                [(code, stats['sum'] / stats['count'], stats['count']) 
                 for code, stats in rating_stats.items() if stats['count'] >= MIN_RATINGS_FOR_TOP_RATED],
                key=lambda x: (x[1], x[2]),  # Sort by avg rating, then count
                reverse=True
            )
            
            # Apply pagination
            paginated_codes = top_codes[offset:offset + page_size]
            
            if paginated_codes:
                # Fetch video details
                codes_list = [code for code, _, _ in paginated_codes]
                codes_filter = ','.join(f'"{c}"' for c in codes_list)
                
                videos = await client.get(
                    'videos',
                    select='code,title,thumbnail_url,duration,release_date,studio,views',
                    filters={'code': f'in.({codes_filter})'}
                )
                
                # Sort videos to match the rating order
                video_dict = {v['code']: v for v in videos}
                sorted_videos = [video_dict[code] for code in codes_list if code in video_dict]
                
                items = await _videos_to_list_items(sorted_videos)
                return await _paginate(items, len(top_codes), page, page_size)
        
        # Fallback: no ratings found
        print("No rated videos found, using high-view fallback for Top Rated")
        videos = await client.get(
            'videos',
            select='code,title,thumbnail_url,duration,release_date,studio,views',
            order='views.desc',
            limit=page_size,
            offset=offset
        )
        
        items = await _videos_to_list_items(videos)
        return await _paginate(items, 10000, page, page_size)
        
    except Exception as e:
        print(f"Error fetching top rated: {e}")
        # Fallback to high view count videos
        videos = await client.get(
            'videos',
            select='code,title,thumbnail_url,duration,release_date,studio,views',
            order='views.desc',
            limit=page_size,
            offset=offset
        )
        
        items = await _videos_to_list_items(videos)
        return await _paginate(items, 10000, page, page_size)


async def get_classics(page: int = 1, page_size: int = 10) -> PaginatedResponse:
    """Get classic videos - oldest content by release date."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Classics = oldest release dates first
    videos = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='release_date.asc',
        limit=page_size,
        offset=offset
    )
    
    if not videos:
        return await _paginate([], 0, page, page_size)
    
    items = await _videos_to_list_items(videos)
    return await _paginate(items, 10000, page, page_size)



async def get_home_feed(user_id: str) -> HomeFeedResponse:
    """
    Get a unified home feed with distinct videos for each section.
    Uses smart algorithms to ensure quality, diversity, and relevance.
    """
    client = get_supabase_rest()
    seen_codes = set()
    
    # Helper to process videos and update seen set
    async def process_section(videos: List[dict], limit: int) -> List[VideoListItem]:
        result = []
        section_videos = []
        for v in videos:
            code = v.get('code')
            if code and code not in seen_codes:
                seen_codes.add(code)
                section_videos.append(v)
                if len(section_videos) >= limit:
                    break
        
        # Fetch ratings for this section's videos
        if section_videos:
            video_codes = [v['code'] for v in section_videos]
            ratings = await _get_ratings_for_videos(video_codes)
            result = [await _video_to_list_item(v, ratings.get(v['code'])) for v in section_videos]
        
        return result
    
    # Helper to calculate days since release
    def days_since_release(release_date_str: str) -> int:
        try:
            from datetime import datetime
            release_date = datetime.fromisoformat(release_date_str.replace('Z', '+00:00'))
            return (datetime.now() - release_date).days
        except:
            return 999999
    
    # Helper to score videos with multiple factors
    def calculate_quality_score(video: dict, like_count: int = 0) -> float:
        """Score based on views, likes, recency, and quality indicators"""
        views = video.get('views', 0)
        has_thumbnail = bool(video.get('thumbnail_url'))
        has_cover = bool(video.get('cover_url'))
        
        # Base score from views (logarithmic to prevent dominance)
        import math
        view_score = math.log10(max(views, 1) + 1) * 10
        
        # Quality bonus
        quality_bonus = 0
        if has_thumbnail:
            quality_bonus += 5
        if has_cover:
            quality_bonus += 5
        
        # Like ratio boost (likes per 100 views)
        like_ratio_bonus = 0
        if views > 0 and like_count >= MIN_LIKES_FOR_FEATURED_BOOST:
            like_ratio = (like_count / views) * 100
            # High like ratio (>5%) gets significant boost
            if like_ratio > 5:
                like_ratio_bonus = like_ratio * LIKE_RATIO_BOOST
        
        return view_score + quality_bonus + like_ratio_bonus

    # 1. TOP RATED - Get videos with best ratings (PROCESS FIRST to ensure rated videos appear here)
    # Aggregate ratings from video_ratings table
    top_rated_candidates = []
    try:
        # Get videos with ratings, calculate average (use admin to bypass RLS)
        ratings_data = await client.get(
            'video_ratings',
            select='video_code,rating',
            limit=1000,
            use_admin=True
        )
        
        if ratings_data and len(ratings_data) > 0:
            # Calculate average rating per video
            from collections import defaultdict
            rating_stats = defaultdict(lambda: {'sum': 0, 'count': 0})
            
            for rating in ratings_data:
                code = rating.get('video_code')
                rating_val = rating.get('rating', 0)
                rating_stats[code]['sum'] += rating_val
                rating_stats[code]['count'] += 1
            
            # Get videos with best ratings (min threshold)
            top_codes = sorted(
                [(code, stats['sum'] / stats['count'], stats['count']) 
                 for code, stats in rating_stats.items() if stats['count'] >= MIN_RATINGS_FOR_TOP_RATED],
                key=lambda x: (x[1], x[2]),  # Sort by avg rating, then count
                reverse=True
            )[:TOP_RATED_BATCH_SIZE]
            
            # Fetch video details for top rated
            if top_codes:
                codes_list = [code for code, _, _ in top_codes]
                for code in codes_list[:TOP_RATED_BATCH_SIZE]:
                    video_data = await client.get(
                        'videos',
                        select='code,title,thumbnail_url,duration,release_date,studio,views',
                        filters={'code': f'eq.{code}'},
                        limit=1
                    )
                    if video_data:
                        top_rated_candidates.extend(video_data)
        
        # If no ratings or not enough rated videos, use fallback
        if not top_rated_candidates:
            top_rated_candidates = await client.get(
                'videos',
                select='code,title,thumbnail_url,duration,release_date,studio,views',
                order='views.desc',
                limit=TOP_RATED_BATCH_SIZE,
                offset=150  # Different offset from Popular to show different videos
            )
    except Exception as e:
        print(f"Error fetching top rated: {e}")
        # Fallback to high view count videos
        top_rated_candidates = await client.get(
            'videos',
            select='code,title,thumbnail_url,duration,release_date,studio,views',
            order='views.desc',
            limit=TOP_RATED_BATCH_SIZE,
            offset=150
        )
    
    top_rated = await process_section(top_rated_candidates or [], FEED_SECTION_SIZE)

    # 2. FEATURED - High quality recent content with good engagement
    # Fetch videos with both thumbnail and cover (quality indicator)
    featured_candidates = await client.get(
        'videos',
        select='code,title,thumbnail_url,cover_url,duration,release_date,studio,views',
        filters={'thumbnail_url': 'neq.', 'cover_url': 'neq.'},
        order='scraped_at.desc',
        limit=FEATURED_BATCH_SIZE
    )
    
    # Get like counts for featured candidates
    if featured_candidates:
        featured_codes = [v['code'] for v in featured_candidates]
        like_counts = await _get_likes_for_videos(featured_codes)
        
        # Score and sort by quality (including like ratio)
        for video in featured_candidates:
            like_count = like_counts.get(video['code'], 0)
            video['_score'] = calculate_quality_score(video, like_count)
        featured_candidates.sort(key=lambda x: x['_score'], reverse=True)
    
    featured = await process_section(featured_candidates or [], FEED_SECTION_SIZE)
    
    # 3. TRENDING - Recent content with growing engagement
    # Prioritize videos from last 30 days with good view velocity AND like velocity
    from datetime import datetime, timedelta
    thirty_days_ago = (datetime.now() - timedelta(days=TRENDING_WINDOW_DAYS)).isoformat()
    
    trending_candidates = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views,scraped_at',
        filters={'scraped_at': f'gte.{thirty_days_ago}'},
        order='views.desc',
        limit=TRENDING_BATCH_SIZE
    )
    
    # Get like counts for trending candidates
    if trending_candidates:
        trending_codes = [v['code'] for v in trending_candidates]
        like_counts = await _get_likes_for_videos(trending_codes)
        
        # Calculate trending score (views + likes velocity / days since scraped)
        for video in trending_candidates:
            days_old = days_since_release(video.get('scraped_at', ''))
            views = video.get('views', 0)
            likes = like_counts.get(video['code'], 0)
            
            # Velocity score: (views + weighted likes) per day, with recency boost
            view_velocity = views / max(days_old, 1)
            like_velocity = likes / max(days_old, 1)
            
            # Combine view and like velocity (likes weighted at 30%)
            combined_velocity = view_velocity + (like_velocity * LIKE_WEIGHT_IN_TRENDING * 100)
            
            # Add recency boost
            recency_boost = 1 + (TRENDING_WINDOW_DAYS - min(days_old, TRENDING_WINDOW_DAYS)) / TRENDING_WINDOW_DAYS
            video['_score'] = combined_velocity * recency_boost
            
        trending_candidates.sort(key=lambda x: x['_score'], reverse=True)
    
    trending = await process_section(trending_candidates or [], FEED_SECTION_SIZE)
    
    # 4. POPULAR - All-time most viewed content
    popular_candidates = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='views.desc',
        limit=POPULAR_BATCH_SIZE
    )
    popular = await process_section(popular_candidates or [], FEED_SECTION_SIZE)
    
    # 5. NEW RELEASES - Recently released content
    new_releases_candidates = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        order='release_date.desc',
        limit=NEW_RELEASES_BATCH_SIZE
    )
    new_releases = await process_section(new_releases_candidates or [], FEED_SECTION_SIZE)
    
    # 6. CLASSICS - Older content (>1 year) with proven quality
    one_year_ago = (datetime.now() - timedelta(days=365 * CLASSICS_AGE_YEARS)).isoformat()
    
    classics_candidates = await client.get(
        'videos',
        select='code,title,thumbnail_url,duration,release_date,studio,views',
        filters={'release_date': f'lt.{one_year_ago}'},
        order='views.desc',
        limit=CLASSICS_BATCH_SIZE
    )
    classics = await process_section(classics_candidates or [], FEED_SECTION_SIZE)

    return HomeFeedResponse(
        featured=featured,
        trending=trending,
        popular=popular,
        top_rated=top_rated,
        new_releases=new_releases,
        classics=classics
    )


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
    
    # Use admin to bypass RLS
    ratings = await client.get(
        'video_ratings',
        select='rating',
        filters={'video_code': f'eq.{code}'},
        use_admin=True
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
        single=True,
        use_admin=True
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
    
    items = await _videos_to_list_items(videos)
    return await _paginate(items, total, page, page_size)


# ============================================
# Watch History
# ============================================

async def record_watch(code: str, user_id: str, duration: int = 0, completed: bool = False) -> bool:
    """Record a watch event."""
    client = get_supabase_rest()
    
    # Normalize code to uppercase
    code = code.upper()
    
    # Try to insert watch history (video FK constraint will fail if video doesn't exist)
    try:
        result = await client.insert(
            'watch_history',
            {
                'video_code': code,
                'user_id': user_id,
                'watch_duration': duration,
                'completed': completed,
                'watched_at': datetime.utcnow().isoformat()
            },
            upsert=True,
            use_admin=True
        )
        return result is not None
    except Exception as e:
        print(f"record_watch error: {e}")
        return True  # Don't fail the request even if tracking fails


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
    
    items = await _videos_to_list_items(videos)
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
        select='video_code,watch_duration,completed,watched_at',
        filters={'user_id': f'eq.{from_user_id}'}
    )
    
    merged = 0
    for h in history:
        result = await client.insert(
            'watch_history',
            {
                'video_code': h['video_code'],
                'user_id': to_user_id,
                'watch_duration': h['watch_duration'],
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
    
    # Try to use Resource Embedding for efficient counting in a single query
    # Supabase/PostgREST allows embedding with count
    try:
        categories = await client.get(
            'categories',
            select='id,name,video_categories(count)'
        )

        if categories:
            result = []
            for cat in categories:
                # Extract count from the nested list
                # PostgREST returns [{'count': N}] or []
                vc_data = cat.get('video_categories', [])
                count = vc_data[0]['count'] if vc_data and isinstance(vc_data, list) and 'count' in vc_data[0] else 0
                result.append({'name': cat['name'], 'video_count': count})

            result.sort(key=lambda x: x['video_count'], reverse=True)
            return result

    except Exception as e:
        print(f"Resource embedding failed, falling back to parallel counts: {e}")

    # Fallback to parallel queries if embedding fails (e.g. missing FK)

    # Get all categories
    categories = await client.get('categories', select='id,name')
    if not categories:
        return []

    # Limit concurrency to avoid overwhelming the database/network
    semaphore = asyncio.Semaphore(10)
    
    async def get_category_count(category):
        async with semaphore:
            count = await client.count(
                'video_categories',
                filters={'category_id': f"eq.{category['id']}"}
            )
            return {'name': category['name'], 'video_count': count}
    
    # Execute counts in parallel
    tasks = [get_category_count(cat) for cat in categories]
    result = await asyncio.gather(*tasks)
    
    result.sort(key=lambda x: x['video_count'], reverse=True)
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
    
    result = [{'name': name, 'video_count': count} for name, count in studio_counts.items()]
    result.sort(key=lambda x: x['video_count'], reverse=True)
    return result


async def get_all_cast() -> List[dict]:
    """Get all cast members with video counts."""
    client = get_supabase_rest()
    
    # Get all cast members with video counts using resource embedding
    # This avoids fetching the entire video_cast table
    cast_members = await client.get('cast_members', select='id,name,video_cast(count)')
    if not cast_members:
        return []
    
    # Build result
    result = []
    for cm in cast_members:
        # Extract count from nested structure: "video_cast": [{"count": 5}]
        video_count = 0
        vc = cm.get('video_cast')
        if vc and isinstance(vc, list) and len(vc) > 0:
            video_count = vc[0].get('count', 0)

        if video_count > 0:
            result.append({'name': cm['name'], 'video_count': video_count})
    
    result.sort(key=lambda x: x['video_count'], reverse=True)
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
    
    result = [{'name': name, 'video_count': count} for name, count in series_counts.items()]
    result.sort(key=lambda x: x['video_count'], reverse=True)
    return result


async def get_cast_with_images(limit: int = 100) -> List[dict]:
    """
    Get featured cast members with their images.
    Uses a mix of popularity and variety to show different cast each time.
    """
    client = get_supabase_rest()
    
    # Strategy: Get top cast by video count, then add some variety
    # This ensures we show popular cast but with some rotation
    
    # Get cast members with video counts using aggregation
    # First, get all cast IDs with their video counts
    video_cast_data = await client.get('video_cast', select='cast_id')
    
    if not video_cast_data:
        return []
    
    # Count videos per cast
    from collections import Counter
    cast_counts = Counter(vc['cast_id'] for vc in video_cast_data if vc.get('cast_id'))
    
    # Get top cast members (fetch more than needed for variety)
    top_cast_ids = [cast_id for cast_id, _ in cast_counts.most_common(limit * 3)]
    
    if not top_cast_ids:
        return []
    
    # Fetch cast member details
    # Split into chunks to avoid URL length issues
    chunk_size = 50
    cast_members = []
    
    for i in range(0, len(top_cast_ids), chunk_size):
        chunk = top_cast_ids[i:i + chunk_size]
        chunk_str = ','.join(str(cid) for cid in chunk)
        
        members = await client.get(
            'cast_members',
            select='id,name',
            filters={'id': f'in.({chunk_str})'}
        )
        if members:
            cast_members.extend(members)
    
    if not cast_members:
        return []
    
    # Get videos with cast_images to find profile pictures
    # Only fetch videos that have cast_images
    videos = await client.get(
        'videos',
        select='cast_images',
        filters={'cast_images': 'not.is.null'},
        limit=500  # Limit to recent videos for performance
    )
    
    # Build a map of cast name -> image URL
    cast_images = {}
    for v in videos or []:
        video_cast_images = v.get('cast_images') or {}
        if isinstance(video_cast_images, dict):
            for name, url in video_cast_images.items():
                if name and url and name not in cast_images:
                    cast_images[name] = url
    
    # Build result with images and counts
    result = []
    for cm in cast_members:
        cast_id = cm['id']
        video_count = cast_counts.get(cast_id, 0)
        
        if video_count > 0:
            image_url = cast_images.get(cm['name'])
            
            # Only include cast with images for featured section
            if image_url:
                result.append({
                    'name': cm['name'],
                    'video_count': video_count,
                    'image_url': image_url
                })
    
    # Sort by video count (popularity)
    result.sort(key=lambda x: x['video_count'], reverse=True)
    
    # Add some variety: take top 70% by popularity, then shuffle the rest
    if len(result) > limit:
        import random
        
        # Take top performers (70% of limit)
        top_count = int(limit * 0.7)
        top_cast = result[:top_count]
        
        # Randomly select from the rest (30% of limit)
        remaining_count = limit - top_count
        remaining_pool = result[top_count:limit * 2]  # Pool from next batch
        
        if remaining_pool:
            random_cast = random.sample(remaining_pool, min(remaining_count, len(remaining_pool)))
            result = top_cast + random_cast
        else:
            result = result[:limit]
    
    return result[:limit]


async def get_all_cast_with_images() -> List[dict]:
    """
    Get ALL cast members (with or without images, with or without videos).
    Shows all 1000 cast members from database.
    """
    client = get_supabase_rest()
    
    # Get all cast IDs with their video counts
    video_cast_data = await client.get('video_cast', select='cast_id')
    
    # Count videos per cast
    from collections import Counter
    cast_counts = Counter()
    if video_cast_data:
        cast_counts = Counter(vc['cast_id'] for vc in video_cast_data if vc.get('cast_id'))
    
    # Get ALL cast members from database (pagination handles limits)
    all_cast_members = await client.get('cast_members', select='id,name')
    
    if not all_cast_members:
        return []
    
    # Get ALL videos with cast_images
    videos = await client.get(
        'videos',
        select='cast_images',
        filters={'cast_images': 'not.is.null'},
        limit=1000
    )
    
    # Build a map of cast name -> image URL and count videos per cast name
    cast_images = {}
    cast_name_video_counts = Counter()
    
    for v in videos or []:
        video_cast_images = v.get('cast_images') or {}
        if isinstance(video_cast_images, dict):
            for name, url in video_cast_images.items():
                if name and url:
                    if name not in cast_images:
                        cast_images[name] = url
                    cast_name_video_counts[name] += 1
    
    # Build result: ALL cast members from database (even with 0 videos)
    result = []
    seen_names = set()
    
    # Add ALL cast members from the database
    for cm in all_cast_members:
        cast_id = cm['id']
        name = cm['name']
        video_count = cast_counts.get(cast_id, 0)  # Will be 0 if no videos
        image_url = cast_images.get(name)  # Will be None if no image
        
        result.append({
            'name': name,
            'video_count': video_count,  # Can be 0
            'image_url': image_url  # Can be None
        })
        seen_names.add(name)
    
    # Also add cast from images who are NOT in cast_members table
    for name, image_url in cast_images.items():
        if name not in seen_names:
            video_count = cast_name_video_counts.get(name, 0)
            result.append({
                'name': name,
                'video_count': video_count,
                'image_url': image_url
            })
    
    # Sort by video count (popularity), then by name
    result.sort(key=lambda x: (-x['video_count'], x['name']))
    
    return result  # Return ALL cast (1000+)


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


async def _get_search_results_codes(query: str, limit: int = 500) -> List[dict]:
    """Get video codes and details for search results to build facets."""
    client = get_supabase_rest()

    # Sanitize query to prevent filter syntax errors
    # Remove characters that might break PostgREST syntax if not properly escaped
    safe_query = query.replace('(', ' ').replace(')', ' ').replace(',', ' ')
    search_term = f'*{safe_query.strip()}*'

    # Fetch videos matching query (larger limit for facets)
    videos = await client.get(
        'videos',
        select='code,studio,release_date',
        filters={'or': f'(code.ilike.{search_term},title.ilike.{search_term},description.ilike.{search_term})'},
        order='views.desc',
        limit=limit
    )
    return videos or []


async def get_search_facets(query: str = None) -> dict:
    """Get available filter facets for search refinement."""
    client = get_supabase_rest()
    
    if not query:
        # Default behavior: global top lists
        categories = await get_all_categories()
        studios = await get_all_studios()
        cast = await get_all_cast()
        return {
            "categories": categories[:20],
            "studios": studios[:20],
            "cast": cast[:20],
            "years": []
        }

    # 1. Get matching videos (limit to 500 to be responsive)
    videos = await _get_search_results_codes(query, limit=500)

    if not videos:
        return {
            "categories": [],
            "studios": [],
            "cast": [],
            "years": []
        }

    video_codes = [v['code'] for v in videos]

    # 2. Aggregate Studios (from video objects directly)
    studio_counts = {}
    year_counts = {}

    for v in videos:
        # Studio
        studio = v.get('studio')
        if studio:
            studio_counts[studio] = studio_counts.get(studio, 0) + 1

        # Year
        release_date = v.get('release_date')
        if release_date:
            try:
                # Handle ISO format or direct string
                if hasattr(release_date, 'year'):
                    year = str(release_date.year)
                else:
                    year = release_date[:4]
                year_counts[year] = year_counts.get(year, 0) + 1
            except:
                pass

    studios_result = [{'name': k, 'video_count': v} for k, v in studio_counts.items()]
    studios_result.sort(key=lambda x: x['video_count'], reverse=True)

    years_result = [{'name': k, 'video_count': v} for k, v in year_counts.items()]
    years_result.sort(key=lambda x: x['name'], reverse=True)

    # 3. Aggregate Categories (needs join)
    # Fetch video_categories for these videos
    # Split into chunks if too many codes
    category_counts = {}
    chunk_size = 50

    for i in range(0, len(video_codes), chunk_size):
        chunk = video_codes[i:i+chunk_size]
        chunk_filter = ','.join(f'"{c}"' for c in chunk)

        # Get category names directly via join
        vc_data = await client.get(
            'video_categories',
            select='categories(name)',
            filters={'video_code': f'in.({chunk_filter})'}
        )

        if vc_data:
            for item in vc_data:
                cat = item.get('categories')
                if cat and cat.get('name'):
                    name = cat.get('name')
                    category_counts[name] = category_counts.get(name, 0) + 1

    categories_result = [{'name': k, 'video_count': v} for k, v in category_counts.items()]
    categories_result.sort(key=lambda x: x['video_count'], reverse=True)

    # 4. Aggregate Cast (needs join)
    cast_counts = {}

    for i in range(0, len(video_codes), chunk_size):
        chunk = video_codes[i:i+chunk_size]
        chunk_filter = ','.join(f'"{c}"' for c in chunk)

        # Get cast names directly via join
        vc_data = await client.get(
            'video_cast',
            select='cast_members(name)',
            filters={'video_code': f'in.({chunk_filter})'}
        )

        if vc_data:
            for item in vc_data:
                member = item.get('cast_members')
                if member and member.get('name'):
                    name = member.get('name')
                    cast_counts[name] = cast_counts.get(name, 0) + 1

    cast_result = [{'name': k, 'video_count': v} for k, v in cast_counts.items()]
    cast_result.sort(key=lambda x: x['video_count'], reverse=True)
    
    return {
        "categories": categories_result[:20],
        "studios": studios_result[:20],
        "cast": cast_result[:20],
        "years": years_result[:20]
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
    
    items = await _videos_to_list_items(videos)
    return await _paginate(items, total, page, page_size)


# ============================================
# Personalized Recommendations (simplified)
# ============================================

async def get_personalized_recommendations(user_id: str, page: int = 1, page_size: int = 12) -> PaginatedResponse:
    """
    Get personalized 'For You' recommendations based on:
    - Watch history (what they've watched)
    - Ratings (what they liked)
    - Bookmarks (what they saved)
    - Similar users' preferences (collaborative filtering)
    """
    client = get_supabase_rest()
    offset = (page - 1) * page_size
    
    try:
        # 1. Get user's watch history
        watch_history = await client.get(
            'watch_history',
            select='video_code,watch_duration,completed',
            filters={'user_id': f'eq.{user_id}'},
            order='watched_at.desc',
            limit=50
        )
        
        # 2. Get user's ratings
        user_ratings = await client.get(
            'video_ratings',
            select='video_code,rating',
            filters={'user_id': f'eq.{user_id}'},
            limit=50
        )
        
        # 3. Get user's bookmarks
        bookmarks = await client.get(
            'video_bookmarks',
            select='video_code',
            filters={'user_id': f'eq.{user_id}'},
            limit=50
        )
        
        # 4. Get user's liked videos
        liked_videos = await client.get(
            'video_likes',
            select='video_code',
            filters={'user_id': f'eq.{user_id}'},
            order='created_at.desc',
            limit=50
        )
        
        # Extract codes and preferences
        watched_codes = {h['video_code'] for h in (watch_history or [])}
        liked_codes = {r['video_code'] for r in (user_ratings or []) if r.get('rating', 0) >= 4}
        bookmarked_codes = {b['video_code'] for b in (bookmarks or [])}
        heart_liked_codes = {l['video_code'] for l in (liked_videos or [])}
        
        # Combine all interacted videos (heart likes have higher weight)
        interacted_codes = watched_codes | liked_codes | bookmarked_codes | heart_liked_codes
        
        if not interacted_codes:
            # New user - return trending content
            return await get_trending_videos(page, page_size)
        
        # 4. Get details of interacted videos to find patterns
        interacted_videos = []
        for code in list(interacted_codes)[:20]:  # Limit to avoid too many queries
            video = await client.get(
                'videos',
                select='code,studio,series',
                filters={'code': f'eq.{code}'},
                limit=1
            )
            if video:
                interacted_videos.extend(video)
        
        # Extract preferences
        preferred_studios = {}
        preferred_series = {}
        
        for video in interacted_videos:
            studio = video.get('studio')
            series = video.get('series')
            
            if studio:
                preferred_studios[studio] = preferred_studios.get(studio, 0) + 1
            if series:
                preferred_series[series] = preferred_series.get(series, 0) + 1
        
        # Get top preferences
        top_studios = sorted(preferred_studios.items(), key=lambda x: x[1], reverse=True)[:3]
        top_series = sorted(preferred_series.items(), key=lambda x: x[1], reverse=True)[:2]
        
        # 5. Get categories from watched videos
        preferred_categories = {}
        interacted_codes_list = list(interacted_codes)[:15]

        # Batch fetch categories
        all_categories = await _get_categories_for_videos(client, interacted_codes_list)
        for code in interacted_codes_list:
            categories = all_categories.get(code, [])
            for cat_name in categories:
                preferred_categories[cat_name] = preferred_categories.get(cat_name, 0) + 1
        
        top_categories = sorted(preferred_categories.items(), key=lambda x: x[1], reverse=True)[:3]
        
        # 6. Get cast from watched videos
        preferred_cast = {}

        # Batch fetch cast
        all_cast = await _get_cast_for_videos(client, interacted_codes_list)
        for code in interacted_codes_list:
            cast_members = all_cast.get(code, [])
            for cast_name in cast_members:
                preferred_cast[cast_name] = preferred_cast.get(cast_name, 0) + 1
        
        top_cast = sorted(preferred_cast.items(), key=lambda x: x[1], reverse=True)[:3]
        
        # 7. Build recommendation candidates from multiple sources
        candidates = []
        seen_codes = set(interacted_codes)  # Don't recommend already watched
        
        # Strategy 1: Same studios
        for studio, _ in top_studios:
            studio_videos = await client.get(
                'videos',
                select='code,title,thumbnail_url,duration,release_date,studio,views',
                filters={'studio': f'eq.{studio}'},
                order='views.desc',
                limit=20
            )
            if studio_videos:
                for v in studio_videos:
                    if v['code'] not in seen_codes:
                        v['_score'] = WEIGHT_STUDIO
                        candidates.append(v)
                        seen_codes.add(v['code'])
        
        # Strategy 2: Same series
        for series, _ in top_series:
            series_videos = await client.get(
                'videos',
                select='code,title,thumbnail_url,duration,release_date,studio,views',
                filters={'series': f'eq.{series}'},
                order='release_date.desc',
                limit=15
            )
            if series_videos:
                for v in series_videos:
                    if v['code'] not in seen_codes:
                        v['_score'] = WEIGHT_SERIES
                        candidates.append(v)
                        seen_codes.add(v['code'])
        
        # Strategy 3: Same categories
        for category, _ in top_categories:
            # Get category ID
            cat_data = await client.get(
                'categories',
                filters={'name': f'eq.{category}'},
                limit=1
            )
            if cat_data:
                cat_id = cat_data[0]['id']
                # Get videos in this category
                cat_videos = await client.get(
                    'video_categories',
                    select='video_code',
                    filters={'category_id': f'eq.{cat_id}'},
                    limit=20
                )
                if cat_videos:
                    for cv in cat_videos[:15]:
                        code = cv['video_code']
                        if code not in seen_codes:
                            video = await client.get(
                                'videos',
                                select='code,title,thumbnail_url,duration,release_date,studio,views',
                                filters={'code': f'eq.{code}'},
                                limit=1
                            )
                            if video:
                                video[0]['_score'] = WEIGHT_CATEGORY
                                candidates.append(video[0])
                                seen_codes.add(code)
        
        # Strategy 4: Same cast
        for cast_name, _ in top_cast:
            # Get cast ID
            cast_data = await client.get(
                'cast_members',
                filters={'name': f'eq.{cast_name}'},
                limit=1
            )
            if cast_data:
                cast_id = cast_data[0]['id']
                # Get videos with this cast
                cast_videos = await client.get(
                    'video_cast',
                    select='video_code',
                    filters={'cast_id': f'eq.{cast_id}'},
                    limit=15
                )
                if cast_videos:
                    for cv in cast_videos[:10]:
                        code = cv['video_code']
                        if code not in seen_codes:
                            video = await client.get(
                                'videos',
                                select='code,title,thumbnail_url,duration,release_date,studio,views',
                                filters={'code': f'eq.{code}'},
                                limit=1
                            )
                            if video:
                                video[0]['_score'] = WEIGHT_CAST
                                candidates.append(video[0])
                                seen_codes.add(code)
        
        # 8. Score and rank candidates
        import math
        for video in candidates:
            base_score = video.get('_score', 0)
            views = video.get('views', 0)
            
            # Add view popularity bonus (logarithmic)
            view_bonus = math.log10(max(views, 1) + 1) * 2
            
            # Recency bonus (newer content gets slight boost)
            try:
                from datetime import datetime
                release_date = video.get('release_date', '')
                if release_date:
                    days_old = (datetime.now() - datetime.fromisoformat(release_date.replace('Z', '+00:00'))).days
                    recency_bonus = max(0, (365 - min(days_old, 365)) / 365) * 5
                else:
                    recency_bonus = 0
            except:
                recency_bonus = 0
            
            video['_final_score'] = base_score + view_bonus + recency_bonus
        
        # Sort by final score
        candidates.sort(key=lambda x: x.get('_final_score', 0), reverse=True)
        
        # 9. Paginate results
        start_idx = offset
        end_idx = offset + page_size
        page_candidates = candidates[start_idx:end_idx]
        
        if not page_candidates:
            # Fallback to trending if not enough recommendations
            return await get_trending_videos(page, page_size)
        
        items = [await _video_to_list_item(v) for v in page_candidates]
        total = len(candidates)
        
        return await _paginate(items, total, page, page_size)
        
    except Exception as e:
        print(f"Error in personalized recommendations: {e}")
        # Fallback to trending content
        return await get_trending_videos(page, page_size)

