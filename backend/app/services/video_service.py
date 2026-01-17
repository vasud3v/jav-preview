"""Video service - business logic for video operations."""
import math
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from sqlalchemy import func, desc, asc, case, and_, or_
from sqlalchemy.orm import Session

from app.models import Video, Category, CastMember, VideoRating, VideoBookmark, WatchHistory
from scraper.db_models import video_categories, video_cast
from app.schemas import VideoListItem, VideoResponse, PaginatedResponse


def _video_to_list_item(video: Video, rating_info: dict = None) -> dict:
    """Convert video to list item format."""
    result = {
        "code": video.code,
        "title": video.title,
        "thumbnail_url": video.thumbnail_url or "",
        "duration": video.duration or "",
        "release_date": video.release_date.isoformat() if video.release_date else "",
        "studio": video.studio or "",
        "views": video.views or 0,
    }
    if rating_info:
        result["rating_avg"] = rating_info.get("average", 0)
        result["rating_count"] = rating_info.get("count", 0)
    return result


def _video_to_response(video: Video) -> dict:
    """Convert video to full response format."""
    return {
        "code": video.code,
        "title": video.title,
        "content_id": video.content_id or "",
        "duration": video.duration or "",
        "release_date": video.release_date.isoformat() if video.release_date else "",
        "thumbnail_url": video.thumbnail_url or "",
        "cover_url": video.cover_url or "",
        "studio": video.studio or "",
        "series": video.series or "",
        "description": video.description or "",
        "embed_urls": video.embed_urls,
        "gallery_images": video.gallery_images,
        "categories": [c.name for c in video.categories],
        "cast": [c.name for c in video.cast],
        "cast_images": video.cast_images,
        "scraped_at": video.scraped_at.isoformat() if video.scraped_at else "",
        "source_url": video.source_url or "",
        "views": video.views or 0,
    }


def _paginate(db: Session, videos: List[Video], total: int, page: int, page_size: int) -> PaginatedResponse:
    """Create paginated response with ratings."""
    total_pages = math.ceil(total / page_size) if total > 0 else 1
    
    # Get ratings for all videos in batch
    video_codes = [v.code for v in videos]
    ratings_map = {}
    
    if video_codes:
        ratings_query = db.query(
            VideoRating.video_code,
            func.avg(VideoRating.rating).label('avg'),
            func.count(VideoRating.id).label('count')
        ).filter(VideoRating.video_code.in_(video_codes)).group_by(VideoRating.video_code).all()
        
        ratings_map = {r.video_code: {"average": round(float(r.avg), 1), "count": r.count} for r in ratings_query}
    
    items = []
    for v in videos:
        rating_info = ratings_map.get(v.code, {"average": 0, "count": 0})
        items.append(VideoListItem(**_video_to_list_item(v, rating_info)))
    
    return PaginatedResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


def get_video(db: Session, code: str) -> Optional[VideoResponse]:
    """Get single video by code."""
    video = db.query(Video).filter(Video.code == code).first()
    if not video:
        return None
    return VideoResponse(**_video_to_response(video))


def get_random_video_code(db: Session, exclude: List[str] = None) -> Optional[str]:
    """Get a random video code, excluding specified codes."""
    from sqlalchemy.sql.expression import func
    query = db.query(Video.code)
    
    if exclude:
        query = query.filter(~Video.code.in_(exclude))
    
    video = query.order_by(func.random()).first()
    return video.code if video else None


def get_videos(
    db: Session,
    page: int = 1,
    page_size: int = 20,
    sort_by: str = "release_date",
    sort_order: str = "desc"
) -> PaginatedResponse:
    """Get paginated list of videos."""
    query = db.query(Video)
    total = query.count()
    
    sort_column = getattr(Video, sort_by, Video.release_date)
    query = query.order_by(desc(sort_column) if sort_order == "desc" else sort_column)
    
    offset = (page - 1) * page_size
    videos = query.offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def search_videos(db: Session, query: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Search videos by title, code, or description."""
    search_term = f"%{query}%"
    q = db.query(Video).filter(
        (Video.title.ilike(search_term)) |
        (Video.code.ilike(search_term)) |
        (Video.description.ilike(search_term))
    )
    total = q.count()
    
    offset = (page - 1) * page_size
    videos = q.order_by(desc(Video.release_date)).offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def advanced_search(
    db: Session,
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
) -> dict:
    """
    Advanced search with multiple filters, relevance scoring, and sorting.
    
    Features:
    - Multi-field text search with relevance scoring
    - Filter by category, studio, cast, series
    - Date range filtering
    - Minimum rating filter
    - Multiple sort options (relevance, date, rating, views)
    - Returns facets for filter refinement
    """
    from datetime import datetime
    
    # Start with base query
    base_query = db.query(Video)
    
    # Get rating subquery for filtering and sorting
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count')
    ).group_by(VideoRating.video_code).subquery()
    
    # Join with ratings
    base_query = base_query.outerjoin(rating_subq, Video.code == rating_subq.c.video_code)
    
    # Apply filters
    filters = []
    
    # Text search with relevance scoring
    relevance_scores = []
    if query and query.strip():
        query_lower = query.lower().strip()
        search_term = f"%{query_lower}%"
        
        # Check for exact code match first
        exact_code_match = Video.code.ilike(query_lower)
        
        # Title match (highest weight)
        title_match = Video.title.ilike(search_term)
        
        # Code partial match
        code_match = Video.code.ilike(search_term)
        
        # Description match (lower weight)
        desc_match = Video.description.ilike(search_term)
        
        # Studio match
        studio_match = Video.studio.ilike(search_term)
        
        # Series match
        series_match = Video.series.ilike(search_term)
        
        filters.append(or_(
            exact_code_match,
            title_match,
            code_match,
            desc_match,
            studio_match,
            series_match
        ))
        
        # Build relevance score for sorting
        relevance_scores = [
            case((exact_code_match, 100), else_=0),  # Exact code match
            case((Video.code.ilike(f"{query_lower}%"), 80), else_=0),  # Code starts with
            case((Video.title.ilike(f"{query_lower}%"), 70), else_=0),  # Title starts with
            case((title_match, 50), else_=0),  # Title contains
            case((code_match, 40), else_=0),  # Code contains
            case((studio_match, 30), else_=0),  # Studio match
            case((series_match, 25), else_=0),  # Series match
            case((desc_match, 10), else_=0),  # Description contains
        ]
    
    # Category filter
    if category:
        base_query = base_query.join(Video.categories).filter(Category.name == category)
    
    # Studio filter
    if studio:
        filters.append(Video.studio == studio)
    
    # Cast filter
    if cast_name:
        base_query = base_query.join(Video.cast).filter(CastMember.name == cast_name)
    
    # Series filter
    if series:
        filters.append(Video.series == series)
    
    # Date range filter
    if date_from:
        try:
            from_date = datetime.fromisoformat(date_from)
            filters.append(Video.release_date >= from_date)
        except ValueError:
            pass
    
    if date_to:
        try:
            to_date = datetime.fromisoformat(date_to)
            filters.append(Video.release_date <= to_date)
        except ValueError:
            pass
    
    # Minimum rating filter
    if min_rating is not None and min_rating > 0:
        filters.append(rating_subq.c.rating_avg >= min_rating)
    
    # Apply all filters
    if filters:
        base_query = base_query.filter(and_(*filters))
    
    # Get total count before pagination
    total = base_query.count()
    
    # Apply sorting
    if sort_by == "relevance" and relevance_scores:
        # Sum all relevance scores
        total_relevance = sum(relevance_scores)
        base_query = base_query.order_by(desc(total_relevance), desc(Video.release_date))
    elif sort_by == "date":
        if sort_order == "asc":
            base_query = base_query.order_by(asc(Video.release_date))
        else:
            base_query = base_query.order_by(desc(Video.release_date))
    elif sort_by == "rating":
        if sort_order == "asc":
            base_query = base_query.order_by(asc(func.coalesce(rating_subq.c.rating_avg, 0)))
        else:
            base_query = base_query.order_by(desc(func.coalesce(rating_subq.c.rating_avg, 0)))
    elif sort_by == "views":
        if sort_order == "asc":
            base_query = base_query.order_by(asc(func.coalesce(Video.views, 0)))
        else:
            base_query = base_query.order_by(desc(func.coalesce(Video.views, 0)))
    elif sort_by == "title":
        if sort_order == "asc":
            base_query = base_query.order_by(asc(Video.title))
        else:
            base_query = base_query.order_by(desc(Video.title))
    else:
        base_query = base_query.order_by(desc(Video.release_date))
    
    # Pagination
    offset = (page - 1) * page_size
    videos = base_query.offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def get_search_suggestions(db: Session, query: str, limit: int = 10) -> dict:
    """
    Get search suggestions based on partial query.
    Returns suggestions from videos, cast, studios, and categories.
    """
    if not query or len(query) < 2:
        return {"suggestions": []}
    
    search_term = f"%{query}%"
    starts_with = f"{query}%"
    suggestions = []
    
    # Video code suggestions (exact prefix match prioritized)
    code_results = db.query(Video.code, Video.title).filter(
        or_(
            Video.code.ilike(starts_with),
            Video.code.ilike(search_term)
        )
    ).limit(5).all()
    
    for code, title in code_results:
        suggestions.append({
            "type": "video",
            "value": code,
            "label": f"{code} - {title[:50]}..." if len(title) > 50 else f"{code} - {title}",
            "priority": 1 if code.lower().startswith(query.lower()) else 2
        })
    
    # Title suggestions
    title_results = db.query(Video.code, Video.title).filter(
        Video.title.ilike(search_term)
    ).limit(5).all()
    
    for code, title in title_results:
        if not any(s["value"] == code for s in suggestions):
            suggestions.append({
                "type": "video",
                "value": code,
                "label": title[:60] + "..." if len(title) > 60 else title,
                "priority": 3
            })
    
    # Cast suggestions
    cast_results = db.query(CastMember.name).filter(
        CastMember.name.ilike(search_term)
    ).limit(5).all()
    
    for (name,) in cast_results:
        suggestions.append({
            "type": "cast",
            "value": name,
            "label": name,
            "priority": 1 if name.lower().startswith(query.lower()) else 2
        })
    
    # Studio suggestions
    studio_results = db.query(Video.studio).filter(
        Video.studio.ilike(search_term),
        Video.studio.isnot(None),
        Video.studio != ''
    ).distinct().limit(5).all()
    
    for (studio,) in studio_results:
        suggestions.append({
            "type": "studio",
            "value": studio,
            "label": studio,
            "priority": 1 if studio.lower().startswith(query.lower()) else 2
        })
    
    # Category suggestions
    category_results = db.query(Category.name).filter(
        Category.name.ilike(search_term)
    ).limit(5).all()
    
    for (name,) in category_results:
        suggestions.append({
            "type": "category",
            "value": name,
            "label": name,
            "priority": 1 if name.lower().startswith(query.lower()) else 2
        })
    
    # Series suggestions
    series_results = db.query(Video.series).filter(
        Video.series.ilike(search_term),
        Video.series.isnot(None),
        Video.series != ''
    ).distinct().limit(5).all()
    
    for (series_name,) in series_results:
        suggestions.append({
            "type": "series",
            "value": series_name,
            "label": series_name,
            "priority": 1 if series_name.lower().startswith(query.lower()) else 2
        })
    
    # Sort by priority and limit
    suggestions.sort(key=lambda x: x["priority"])
    
    return {"suggestions": suggestions[:limit]}


def get_search_facets(db: Session, query: str = None) -> dict:
    """
    Get available filter facets for search refinement.
    Returns counts for categories, studios, cast, and date ranges.
    """
    base_query = db.query(Video)
    
    # Apply text search if query provided
    if query and query.strip():
        search_term = f"%{query}%"
        base_query = base_query.filter(
            or_(
                Video.title.ilike(search_term),
                Video.code.ilike(search_term),
                Video.description.ilike(search_term),
                Video.studio.ilike(search_term),
                Video.series.ilike(search_term)
            )
        )
    
    video_codes = [v.code for v in base_query.all()]
    
    if not video_codes:
        return {
            "categories": [],
            "studios": [],
            "cast": [],
            "years": []
        }
    
    # Category facets
    category_counts = db.query(
        Category.name,
        func.count(video_categories.c.video_code).label('count')
    ).join(video_categories).filter(
        video_categories.c.video_code.in_(video_codes)
    ).group_by(Category.name).order_by(desc('count')).limit(20).all()
    
    # Studio facets
    studio_counts = db.query(
        Video.studio,
        func.count(Video.code).label('count')
    ).filter(
        Video.code.in_(video_codes),
        Video.studio.isnot(None),
        Video.studio != ''
    ).group_by(Video.studio).order_by(desc('count')).limit(20).all()
    
    # Cast facets
    cast_counts = db.query(
        CastMember.name,
        func.count(video_cast.c.video_code).label('count')
    ).join(video_cast).filter(
        video_cast.c.video_code.in_(video_codes)
    ).group_by(CastMember.name).order_by(desc('count')).limit(20).all()
    
    # Year facets
    year_counts = db.query(
        func.strftime('%Y', Video.release_date).label('year'),
        func.count(Video.code).label('count')
    ).filter(
        Video.code.in_(video_codes),
        Video.release_date.isnot(None)
    ).group_by('year').order_by(desc('year')).limit(10).all()
    
    return {
        "categories": [{"name": name, "count": count} for name, count in category_counts],
        "studios": [{"name": name, "count": count} for name, count in studio_counts],
        "cast": [{"name": name, "count": count} for name, count in cast_counts],
        "years": [{"year": year, "count": count} for year, count in year_counts if year]
    }


def get_videos_by_category(db: Session, category: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos in a category."""
    q = db.query(Video).join(Video.categories).filter(Category.name == category)
    total = q.count()
    
    offset = (page - 1) * page_size
    videos = q.order_by(desc(Video.release_date)).offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def get_videos_by_cast(db: Session, cast_name: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos featuring a cast member."""
    q = db.query(Video).join(Video.cast).filter(CastMember.name == cast_name)
    total = q.count()
    
    offset = (page - 1) * page_size
    videos = q.order_by(desc(Video.release_date)).offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def get_videos_by_studio(db: Session, studio: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos from a studio."""
    q = db.query(Video).filter(Video.studio == studio)
    total = q.count()
    
    offset = (page - 1) * page_size
    videos = q.order_by(desc(Video.release_date)).offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def get_videos_by_series(db: Session, series: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get videos from a series."""
    q = db.query(Video).filter(Video.series == series)
    total = q.count()
    
    offset = (page - 1) * page_size
    videos = q.order_by(desc(Video.release_date)).offset(offset).limit(page_size).all()
    
    return _paginate(db, videos, total, page, page_size)


def get_all_series(db: Session) -> list:
    """Get all series with video counts."""
    from sqlalchemy import func
    
    results = db.query(
        Video.series,
        func.count(Video.code).label('count')
    ).filter(
        Video.series.isnot(None),
        Video.series != ''
    ).group_by(Video.series).order_by(desc('count')).all()
    
    return [{"name": r.series, "count": r.count} for r in results]


def increment_views(db: Session, code: str) -> bool:
    """Increment view count for a video."""
    video = db.query(Video).filter(Video.code == code).first()
    if not video:
        return False
    
    video.views = (video.views or 0) + 1
    db.commit()
    return True


def get_video_rating(db: Session, code: str) -> dict:
    """Get rating statistics for a video."""
    ratings = db.query(VideoRating).filter(VideoRating.video_code == code).all()
    
    if not ratings:
        return {
            "average": 0,
            "count": 0,
            "distribution": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        }
    
    total = sum(r.rating for r in ratings)
    count = len(ratings)
    distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for r in ratings:
        distribution[r.rating] += 1
    
    return {
        "average": round(total / count, 1),
        "count": count,
        "distribution": distribution
    }


def get_user_rating(db: Session, code: str, user_id: str) -> Optional[int]:
    """Get a user's rating for a video."""
    rating = db.query(VideoRating).filter(
        VideoRating.video_code == code,
        VideoRating.user_id == user_id
    ).first()
    return rating.rating if rating else None


def set_video_rating(db: Session, code: str, user_id: str, rating: int) -> dict:
    """Set or update a user's rating for a video."""
    if rating < 1 or rating > 5:
        raise ValueError("Rating must be between 1 and 5")
    
    # Check if video exists
    video = db.query(Video).filter(Video.code == code).first()
    if not video:
        raise ValueError("Video not found")
    
    # Check for existing rating
    existing = db.query(VideoRating).filter(
        VideoRating.video_code == code,
        VideoRating.user_id == user_id
    ).first()
    
    if existing:
        existing.rating = rating
        existing.updated_at = datetime.utcnow()
    else:
        new_rating = VideoRating(
            video_code=code,
            user_id=user_id,
            rating=rating
        )
        db.add(new_rating)
    
    db.commit()
    
    # Return updated stats with user rating
    stats = get_video_rating(db, code)
    stats["user_rating"] = rating
    return stats


def delete_video_rating(db: Session, code: str, user_id: str) -> bool:
    """Delete a user's rating for a video."""
    rating = db.query(VideoRating).filter(
        VideoRating.video_code == code,
        VideoRating.user_id == user_id
    ).first()
    
    if rating:
        db.delete(rating)
        db.commit()
        return True
    return False


# ============================================
# Homepage Category Algorithms (Advanced)
# ============================================

def _exponential_decay(days: float, half_life: float = 7.0) -> float:
    """
    Calculate exponential decay factor.
    Returns value between 0 and 1, where 1 is most recent.
    Half-life determines how quickly the decay occurs.
    """
    return math.exp(-0.693 * days / half_life)


def _wilson_score(positive: int, total: int, confidence: float = 0.95) -> float:
    """
    Wilson score confidence interval for ranking.
    Better than simple average for items with few ratings.
    Returns lower bound of confidence interval.
    """
    if total == 0:
        return 0.0
    
    z = 1.96 if confidence == 0.95 else 1.645  # z-score for confidence level
    p = positive / total
    
    denominator = 1 + z * z / total
    centre_adjusted_probability = p + z * z / (2 * total)
    adjusted_standard_deviation = math.sqrt((p * (1 - p) + z * z / (4 * total)) / total)
    
    lower_bound = (centre_adjusted_probability - z * adjusted_standard_deviation) / denominator
    return max(0.0, lower_bound)


def _bayesian_average(rating_avg: float, rating_count: int, global_avg: float = 3.0, min_votes: int = 5) -> float:
    """
    Bayesian average (IMDB-style weighted rating).
    Pulls ratings toward global average for items with few votes.
    
    Formula: (v / (v + m)) * R + (m / (v + m)) * C
    Where: v = votes, m = minimum votes, R = item average, C = global average
    """
    return (rating_count / (rating_count + min_votes)) * rating_avg + (min_votes / (rating_count + min_votes)) * global_avg


def _get_rating_stats(db: Session) -> dict:
    """Get global rating statistics for normalization."""
    # Get global average rating
    global_avg_result = db.query(
        func.avg(VideoRating.rating).label('global_avg')
    ).first()
    
    global_avg = float(global_avg_result.global_avg) if global_avg_result and global_avg_result.global_avg else 3.0
    
    # Get max rating count per video (separate query to avoid nested aggregates)
    max_count_result = db.query(
        func.count(VideoRating.id).label('cnt')
    ).group_by(VideoRating.video_code).order_by(desc('cnt')).first()
    
    max_rating_count = max_count_result.cnt if max_count_result else 1
    
    return {
        'global_avg': global_avg,
        'max_rating_count': max_rating_count
    }


def _get_video_metrics(db: Session) -> dict:
    """Get global video metrics for normalization."""
    max_views = db.query(func.max(Video.views)).scalar() or 1
    total_videos = db.query(func.count(Video.code)).scalar() or 1
    
    # Get percentile values for better normalization
    views_list = [v.views or 0 for v in db.query(Video.views).all()]
    views_list.sort()
    
    p90_views = views_list[int(len(views_list) * 0.9)] if views_list else 1
    median_views = views_list[int(len(views_list) * 0.5)] if views_list else 1
    
    return {
        'max_views': max_views,
        'p90_views': max(p90_views, 1),
        'median_views': max(median_views, 1),
        'total_videos': total_videos
    }


def get_trending_videos(
    db: Session,
    page: int = 1,
    page_size: int = 10,
    days_window: int = 30
) -> PaginatedResponse:
    """
    Advanced trending algorithm using exponential time decay and engagement velocity.
    
    Trending Score = (views * velocity_multiplier * time_decay) + rating_boost + freshness_bonus
    
    Components:
    - Exponential time decay (half-life = 7 days)
    - Velocity multiplier based on views per day
    - Rating boost for highly-rated content
    - Freshness bonus for very new content
    """
    now = datetime.utcnow()
    metrics = _get_video_metrics(db)
    rating_stats = _get_rating_stats(db)
    
    # Get all videos with ratings in one query
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count')
    ).group_by(VideoRating.video_code).subquery()
    
    videos_with_ratings = db.query(
        Video,
        rating_subq.c.rating_avg,
        rating_subq.c.rating_count
    ).outerjoin(rating_subq, Video.code == rating_subq.c.video_code).all()
    
    scored_videos = []
    for video, rating_avg, rating_count in videos_with_ratings:
        views = video.views or 0
        scraped_at = video.scraped_at or now
        
        # Calculate time factors
        hours_since = max(1, (now - scraped_at).total_seconds() / 3600)
        days_since = hours_since / 24
        
        # Exponential time decay (half-life = 7 days)
        time_decay = _exponential_decay(days_since, half_life=7.0)
        
        # Velocity: views per day (higher = more viral)
        velocity = views / max(1, days_since)
        velocity_multiplier = math.log1p(velocity) / math.log1p(metrics['median_views'])
        
        # Normalize views using log scale (handles viral content better)
        log_views = math.log1p(views)
        max_log_views = math.log1p(metrics['max_views'])
        norm_views = log_views / max_log_views if max_log_views > 0 else 0
        
        # Rating boost (Bayesian average to handle few ratings)
        rating_boost = 0
        if rating_avg and rating_count:
            bayesian_rating = _bayesian_average(
                float(rating_avg), 
                rating_count, 
                rating_stats['global_avg'],
                min_votes=3
            )
            rating_boost = (bayesian_rating / 5.0) * 0.2  # Up to 20% boost
        
        # Freshness bonus for content < 3 days old
        freshness_bonus = 0
        if days_since <= 3:
            freshness_bonus = 0.3 * (1 - days_since / 3)  # Linear decay from 30% to 0%
        elif days_since <= 7:
            freshness_bonus = 0.1 * (1 - (days_since - 3) / 4)  # Smaller bonus 7 days
        
        # Final trending score
        trending_score = (
            norm_views * velocity_multiplier * time_decay * 0.6 +
            rating_boost +
            freshness_bonus
        )
        
        # Ensure minimum score for very new content with no views
        if days_since <= 1 and views == 0:
            trending_score = max(trending_score, 0.1)
        
        scored_videos.append((video, trending_score))
    
    # Sort by trending score descending
    scored_videos.sort(key=lambda x: x[1], reverse=True)
    
    total = len(scored_videos)
    offset = (page - 1) * page_size
    paginated = scored_videos[offset:offset + page_size]
    
    videos_only = [v[0] for v in paginated]
    return _paginate(db, videos_only, total, page, page_size)


def get_popular_videos(
    db: Session,
    page: int = 1,
    page_size: int = 10
) -> PaginatedResponse:
    """
    Advanced popularity algorithm using weighted engagement score.
    
    Popularity Score = log(views + 1) * engagement_multiplier * quality_factor
    
    Components:
    - Logarithmic view scaling (prevents viral outliers from dominating)
    - Engagement multiplier based on rating participation
    - Quality factor using Bayesian rating average
    - Bookmark count consideration
    """
    metrics = _get_video_metrics(db)
    rating_stats = _get_rating_stats(db)
    
    # Get ratings and bookmark counts
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count')
    ).group_by(VideoRating.video_code).subquery()
    
    bookmark_subq = db.query(
        VideoBookmark.video_code,
        func.count(VideoBookmark.id).label('bookmark_count')
    ).group_by(VideoBookmark.video_code).subquery()
    
    videos_data = db.query(
        Video,
        rating_subq.c.rating_avg,
        rating_subq.c.rating_count,
        bookmark_subq.c.bookmark_count
    ).outerjoin(
        rating_subq, Video.code == rating_subq.c.video_code
    ).outerjoin(
        bookmark_subq, Video.code == bookmark_subq.c.video_code
    ).all()
    
    scored_videos = []
    max_bookmarks = max((b or 0 for _, _, _, b in videos_data), default=1) or 1
    
    for video, rating_avg, rating_count, bookmark_count in videos_data:
        views = video.views or 0
        rating_count = rating_count or 0
        bookmark_count = bookmark_count or 0
        
        # Logarithmic view score (handles viral content better)
        log_views = math.log1p(views)
        max_log_views = math.log1p(metrics['max_views'])
        view_score = log_views / max_log_views if max_log_views > 0 else 0
        
        # Engagement multiplier (more ratings = more engaged audience)
        engagement_rate = rating_count / max(1, views) if views > 0 else 0
        engagement_multiplier = 1 + min(0.5, engagement_rate * 100)  # Up to 50% boost
        
        # Quality factor using Bayesian average
        quality_factor = 1.0
        if rating_avg and rating_count > 0:
            bayesian_rating = _bayesian_average(
                float(rating_avg),
                rating_count,
                rating_stats['global_avg'],
                min_votes=5
            )
            quality_factor = 0.7 + (bayesian_rating / 5.0) * 0.6  # Range: 0.7 to 1.3
        
        # Bookmark bonus (indicates save-worthy content)
        bookmark_score = math.log1p(bookmark_count) / math.log1p(max_bookmarks) if max_bookmarks > 1 else 0
        
        # Final popularity score
        popularity_score = (
            view_score * 0.5 +
            view_score * engagement_multiplier * quality_factor * 0.3 +
            bookmark_score * 0.2
        )
        
        scored_videos.append((video, popularity_score))
    
    # Sort by popularity score descending
    scored_videos.sort(key=lambda x: x[1], reverse=True)
    
    total = len(scored_videos)
    offset = (page - 1) * page_size
    paginated = scored_videos[offset:offset + page_size]
    
    videos_only = [v[0] for v in paginated]
    return _paginate(db, videos_only, total, page, page_size)


def get_top_rated_videos(
    db: Session,
    page: int = 1,
    page_size: int = 10,
    min_ratings: int = 1
) -> PaginatedResponse:
    """
    Advanced top-rated algorithm using Wilson score confidence interval.
    
    Uses Wilson score lower bound instead of simple average.
    This properly handles items with few ratings by being more conservative.
    
    Also applies Bayesian averaging to pull scores toward global mean
    for items with very few ratings.
    """
    rating_stats = _get_rating_stats(db)
    
    # Get all videos with their ratings
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count'),
        func.sum(case((VideoRating.rating >= 4, 1), else_=0)).label('positive_ratings')
    ).group_by(VideoRating.video_code).having(
        func.count(VideoRating.id) >= min_ratings
    ).subquery()
    
    videos_data = db.query(
        Video,
        rating_subq.c.rating_avg,
        rating_subq.c.rating_count,
        rating_subq.c.positive_ratings
    ).join(rating_subq, Video.code == rating_subq.c.video_code).all()
    
    scored_videos = []
    for video, rating_avg, rating_count, positive_ratings in videos_data:
        rating_avg = float(rating_avg) if rating_avg else 0
        rating_count = rating_count or 0
        positive_ratings = positive_ratings or 0
        
        # Wilson score for positive rating ratio (ratings >= 4 are "positive")
        wilson = _wilson_score(positive_ratings, rating_count, confidence=0.95)
        
        # Bayesian average rating
        bayesian_rating = _bayesian_average(
            rating_avg,
            rating_count,
            rating_stats['global_avg'],
            min_votes=5
        )
        
        # Confidence factor (more ratings = more confident)
        confidence_factor = min(1.0, rating_count / 20)  # Max confidence at 20 ratings
        
        # Combined score: weighted blend of Wilson score and Bayesian rating
        # Wilson handles the "is it good?" question
        # Bayesian handles the "how good?" question
        combined_score = (
            wilson * 0.4 +  # 40% weight on Wilson score
            (bayesian_rating / 5.0) * 0.4 +  # 40% weight on Bayesian rating
            confidence_factor * 0.2  # 20% weight on confidence
        )
        
        scored_videos.append((video, combined_score, rating_avg, rating_count))
    
    # Sort by combined score descending
    scored_videos.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)
    
    total = len(scored_videos)
    offset = (page - 1) * page_size
    paginated = scored_videos[offset:offset + page_size]
    
    videos_only = [v[0] for v in paginated]
    return _paginate(db, videos_only, total, page, page_size)


def get_featured_videos(
    db: Session,
    page: int = 1,
    page_size: int = 10
) -> PaginatedResponse:
    """
    Advanced featured/editorial picks algorithm.
    
    Selects high-quality content that balances multiple factors:
    - Quality (Bayesian rating)
    - Popularity (views with diminishing returns)
    - Engagement (rating participation rate)
    - Completeness (has thumbnail, description, cast info)
    - Diversity (avoids same studio domination)
    
    Uses a multi-factor scoring system with diversity injection.
    """
    metrics = _get_video_metrics(db)
    rating_stats = _get_rating_stats(db)
    
    # Get comprehensive video data
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count')
    ).group_by(VideoRating.video_code).subquery()
    
    videos_data = db.query(
        Video,
        rating_subq.c.rating_avg,
        rating_subq.c.rating_count
    ).outerjoin(rating_subq, Video.code == rating_subq.c.video_code).all()
    
    scored_videos = []
    for video, rating_avg, rating_count in videos_data:
        views = video.views or 0
        rating_count = rating_count or 0
        
        # Skip videos with no engagement signals
        if views == 0 and rating_count == 0:
            continue
        
        # Quality score (Bayesian rating)
        quality_score = 0.5  # Default for unrated
        if rating_avg and rating_count > 0:
            bayesian_rating = _bayesian_average(
                float(rating_avg),
                rating_count,
                rating_stats['global_avg'],
                min_votes=3
            )
            quality_score = bayesian_rating / 5.0
        
        # Popularity score with diminishing returns (sqrt scaling)
        popularity_score = math.sqrt(views) / math.sqrt(metrics['max_views']) if metrics['max_views'] > 0 else 0
        
        # Engagement score (rating participation)
        engagement_score = 0
        if views > 0:
            engagement_rate = rating_count / views
            engagement_score = min(1.0, engagement_rate * 50)  # Cap at 2% engagement = 1.0
        
        # Completeness score (metadata quality)
        completeness = 0
        if video.thumbnail_url:
            completeness += 0.3
        if video.description and len(video.description) > 50:
            completeness += 0.3
        if video.cast and len(video.cast) > 0:
            completeness += 0.2
        if video.categories and len(video.categories) > 0:
            completeness += 0.2
        
        # Final featured score
        featured_score = (
            quality_score * 0.35 +
            popularity_score * 0.25 +
            engagement_score * 0.20 +
            completeness * 0.20
        )
        
        scored_videos.append((video, featured_score, video.studio))
    
    # Sort by score
    scored_videos.sort(key=lambda x: x[1], reverse=True)
    
    # Apply diversity: limit videos from same studio in top results
    diverse_results = []
    studio_counts = {}
    max_per_studio = 3  # Max 3 videos per studio in results
    
    for video, score, studio in scored_videos:
        studio_key = studio or 'unknown'
        current_count = studio_counts.get(studio_key, 0)
        
        if current_count < max_per_studio:
            diverse_results.append((video, score))
            studio_counts[studio_key] = current_count + 1
        
        if len(diverse_results) >= page_size * 3:  # Get enough for pagination
            break
    
    total = len(diverse_results)
    offset = (page - 1) * page_size
    paginated = diverse_results[offset:offset + page_size]
    
    videos_only = [v[0] for v in paginated]
    return _paginate(db, videos_only, total, page, page_size)


def get_new_releases(
    db: Session,
    page: int = 1,
    page_size: int = 10,
    days_limit: int = 90
) -> PaginatedResponse:
    """
    Advanced new releases algorithm with early engagement signals.
    
    Prioritizes new content but uses early engagement signals to
    surface promising new releases over stale ones.
    
    Score = recency_score * (1 + early_engagement_bonus)
    
    Early engagement bonus rewards new content that's getting
    views and ratings quickly.
    """
    now = datetime.utcnow()
    cutoff_date = now - timedelta(days=days_limit)
    
    # Get ratings for new releases
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count')
    ).group_by(VideoRating.video_code).subquery()
    
    # Filter by release_date or scraped_at within limit
    videos_data = db.query(
        Video,
        rating_subq.c.rating_avg,
        rating_subq.c.rating_count
    ).outerjoin(
        rating_subq, Video.code == rating_subq.c.video_code
    ).filter(
        or_(
            Video.release_date >= cutoff_date,
            and_(Video.release_date.is_(None), Video.scraped_at >= cutoff_date)
        )
    ).all()
    
    scored_videos = []
    for video, rating_avg, rating_count in videos_data:
        # Determine effective date
        effective_date = video.release_date or video.scraped_at or now
        days_since = max(0.1, (now - effective_date).total_seconds() / 86400)
        
        # Recency score (exponential decay, half-life = 14 days for new releases)
        recency_score = _exponential_decay(days_since, half_life=14.0)
        
        # Early engagement bonus
        views = video.views or 0
        rating_count = rating_count or 0
        
        # Views velocity (views per day)
        views_per_day = views / days_since
        velocity_bonus = min(0.3, math.log1p(views_per_day) / 10)  # Up to 30% bonus
        
        # Rating bonus for early ratings
        rating_bonus = 0
        if rating_avg and rating_count > 0:
            # Higher bonus for good ratings on new content
            rating_bonus = (float(rating_avg) / 5.0) * min(0.2, rating_count * 0.05)
        
        # Quality indicator bonus (has good metadata)
        quality_bonus = 0
        if video.thumbnail_url and video.description:
            quality_bonus = 0.1
        
        # Final score
        engagement_bonus = velocity_bonus + rating_bonus + quality_bonus
        new_release_score = recency_score * (1 + engagement_bonus)
        
        scored_videos.append((video, new_release_score, effective_date))
    
    # Sort by score (with date as tiebreaker)
    scored_videos.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
    total = len(scored_videos)
    offset = (page - 1) * page_size
    paginated = scored_videos[offset:offset + page_size]
    
    videos_only = [v[0] for v in paginated]
    return _paginate(db, videos_only, total, page, page_size)


def get_classics(
    db: Session,
    page: int = 1,
    page_size: int = 10,
    min_age_days: int = 365,
    min_ratings: int = 2
) -> PaginatedResponse:
    """
    Advanced classics algorithm for timeless, high-quality content.
    
    Identifies content that has stood the test of time:
    - Old enough to be considered "classic" (> 1 year)
    - Consistently well-rated over time
    - Still getting views (evergreen content)
    
    Classics Score = quality_score * longevity_factor * evergreen_bonus
    
    Uses stricter quality thresholds than other algorithms.
    """
    now = datetime.utcnow()
    cutoff_date = now - timedelta(days=min_age_days)
    metrics = _get_video_metrics(db)
    rating_stats = _get_rating_stats(db)
    
    # Get ratings for old videos
    rating_subq = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('rating_avg'),
        func.count(VideoRating.id).label('rating_count'),
        func.sum(case((VideoRating.rating >= 4, 1), else_=0)).label('positive_ratings')
    ).group_by(VideoRating.video_code).having(
        func.count(VideoRating.id) >= min_ratings
    ).subquery()
    
    # Get old videos with ratings
    videos_data = db.query(
        Video,
        rating_subq.c.rating_avg,
        rating_subq.c.rating_count,
        rating_subq.c.positive_ratings
    ).join(
        rating_subq, Video.code == rating_subq.c.video_code
    ).filter(
        Video.release_date.isnot(None),
        Video.release_date < cutoff_date
    ).all()
    
    scored_videos = []
    for video, rating_avg, rating_count, positive_ratings in videos_data:
        rating_avg = float(rating_avg) if rating_avg else 0
        rating_count = rating_count or 0
        positive_ratings = positive_ratings or 0
        views = video.views or 0
        
        # Quality score using Wilson score (stricter for classics)
        wilson = _wilson_score(positive_ratings, rating_count, confidence=0.95)
        
        # Bayesian rating
        bayesian_rating = _bayesian_average(
            rating_avg,
            rating_count,
            rating_stats['global_avg'],
            min_votes=5  # Higher threshold for classics
        )
        
        # Quality must be above average to be a classic
        if bayesian_rating < rating_stats['global_avg']:
            continue
        
        quality_score = (wilson * 0.5 + bayesian_rating / 5.0 * 0.5)
        
        # Longevity factor (older = more impressive if still rated well)
        age_days = (now - video.release_date).days
        age_years = age_days / 365
        longevity_factor = 1 + math.log1p(age_years) * 0.1  # Slight bonus for older content
        
        # Evergreen bonus (still getting views relative to age)
        views_per_year = views / max(1, age_years)
        median_views_per_year = metrics['median_views']  # Approximate
        evergreen_ratio = views_per_year / max(1, median_views_per_year)
        evergreen_bonus = 1 + min(0.3, math.log1p(evergreen_ratio) * 0.1)
        
        # Rating consistency bonus (high positive ratio)
        positive_ratio = positive_ratings / rating_count if rating_count > 0 else 0
        consistency_bonus = 1 + (positive_ratio - 0.5) * 0.2 if positive_ratio > 0.5 else 1
        
        # Final classics score
        classics_score = quality_score * longevity_factor * evergreen_bonus * consistency_bonus
        
        scored_videos.append((video, classics_score))
    
    # Sort by classics score descending
    scored_videos.sort(key=lambda x: x[1], reverse=True)
    
    total = len(scored_videos)
    offset = (page - 1) * page_size
    paginated = scored_videos[offset:offset + page_size]
    
    videos_only = [v[0] for v in paginated]
    return _paginate(db, videos_only, total, page, page_size)


# ============================================
# Bookmark Functions
# ============================================

def is_bookmarked(db: Session, code: str, user_id: str) -> bool:
    """Check if a video is bookmarked by user."""
    bookmark = db.query(VideoBookmark).filter(
        VideoBookmark.video_code == code,
        VideoBookmark.user_id == user_id
    ).first()
    return bookmark is not None


def add_bookmark(db: Session, code: str, user_id: str) -> bool:
    """Add a bookmark for a video."""
    # Check if video exists
    video = db.query(Video).filter(Video.code == code).first()
    if not video:
        raise ValueError("Video not found")
    
    # Check if already bookmarked
    existing = db.query(VideoBookmark).filter(
        VideoBookmark.video_code == code,
        VideoBookmark.user_id == user_id
    ).first()
    
    if existing:
        return False  # Already bookmarked
    
    bookmark = VideoBookmark(
        video_code=code,
        user_id=user_id
    )
    db.add(bookmark)
    db.commit()
    return True


def remove_bookmark(db: Session, code: str, user_id: str) -> bool:
    """Remove a bookmark for a video."""
    bookmark = db.query(VideoBookmark).filter(
        VideoBookmark.video_code == code,
        VideoBookmark.user_id == user_id
    ).first()
    
    if bookmark:
        db.delete(bookmark)
        db.commit()
        return True
    return False


def get_user_bookmarks(db: Session, user_id: str, page: int = 1, page_size: int = 20) -> PaginatedResponse:
    """Get paginated list of user's bookmarked videos."""
    # Get bookmark video codes for this user, ordered by most recent
    bookmarks_q = db.query(VideoBookmark.video_code).filter(
        VideoBookmark.user_id == user_id
    ).order_by(desc(VideoBookmark.created_at))
    
    total = bookmarks_q.count()
    
    offset = (page - 1) * page_size
    bookmark_codes = [b.video_code for b in bookmarks_q.offset(offset).limit(page_size).all()]
    
    if not bookmark_codes:
        return PaginatedResponse(
            items=[],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=math.ceil(total / page_size) if total > 0 else 1
        )
    
    # Get videos in the same order as bookmarks
    videos = db.query(Video).filter(Video.code.in_(bookmark_codes)).all()
    video_map = {v.code: v for v in videos}
    ordered_videos = [video_map[code] for code in bookmark_codes if code in video_map]
    
    return _paginate(db, ordered_videos, total, page, page_size)


def get_bookmark_count(db: Session, user_id: str) -> int:
    """Get total bookmark count for a user."""
    return db.query(VideoBookmark).filter(VideoBookmark.user_id == user_id).count()

def get_related_videos(
    db: Session,
    code: str,
    user_id: str = None,
    limit: int = 12
) -> List[dict]:
    """
    Advanced recommendation algorithm that combines multiple signals:
    
    1. Content-Based Filtering (40% weight):
       - Same cast members (+4 points each)
       - Same categories (+2 points each)
       - Same studio (+3 points)
       - Same series (+5 points)
    
    2. Collaborative Filtering (30% weight):
       - Videos watched by users who watched this video
       - Videos rated highly by users with similar taste
    
    3. User Preference Profile (20% weight):
       - Based on user's watch history
       - Favorite cast members (most watched)
       - Favorite categories (most watched)
       - Favorite studios (most watched)
    
    4. Popularity & Quality Signals (10% weight):
       - View count (log scaled)
       - Average rating
       - Recency bonus
    
    5. Diversity injection:
       - Limit same studio/series to avoid monotony
       - Mix in some exploration picks
    """
    from collections import Counter
    
    # Get the source video
    source = db.query(Video).filter(Video.code == code).first()
    if not source:
        return []
    
    # Get source video's attributes
    source_cast = set(c.name for c in source.cast) if source.cast else set()
    source_categories = set(c.name for c in source.categories) if source.categories else set()
    source_studio = source.studio
    source_series = source.series
    
    # ========================================
    # Build User Preference Profile
    # ========================================
    user_profile = {
        'cast': Counter(),
        'categories': Counter(),
        'studios': Counter(),
        'watched_codes': set(),
        'rated_codes': {},  # code -> rating
        'bookmarked_codes': set()
    }
    
    if user_id:
        # Get watch history (last 100 videos)
        watch_history = db.query(WatchHistory.video_code).filter(
            WatchHistory.user_id == user_id
        ).order_by(desc(WatchHistory.watched_at)).limit(100).all()
        user_profile['watched_codes'] = set(w.video_code for w in watch_history)
        
        # Get user ratings
        user_ratings = db.query(VideoRating.video_code, VideoRating.rating).filter(
            VideoRating.user_id == user_id
        ).all()
        user_profile['rated_codes'] = {r.video_code: r.rating for r in user_ratings}
        
        # Get bookmarks
        bookmarks = db.query(VideoBookmark.video_code).filter(
            VideoBookmark.user_id == user_id
        ).all()
        user_profile['bookmarked_codes'] = set(b.video_code for b in bookmarks)
        
        # Build preference profile from watched videos
        if user_profile['watched_codes']:
            watched_videos = db.query(Video).filter(
                Video.code.in_(user_profile['watched_codes'])
            ).all()
            
            for v in watched_videos:
                # Weight by rating if available (1-5 -> 0.5-1.5 multiplier)
                weight = 1.0
                if v.code in user_profile['rated_codes']:
                    weight = 0.5 + (user_profile['rated_codes'][v.code] / 5.0)
                
                # Count cast preferences
                for c in (v.cast or []):
                    user_profile['cast'][c.name] += weight
                
                # Count category preferences
                for c in (v.categories or []):
                    user_profile['categories'][c.name] += weight
                
                # Count studio preferences
                if v.studio:
                    user_profile['studios'][v.studio] += weight
    
    # ========================================
    # Collaborative Filtering: Find similar users
    # ========================================
    similar_user_videos = Counter()
    
    if user_id and user_profile['watched_codes']:
        # Find users who watched the same videos
        similar_users = db.query(WatchHistory.user_id).filter(
            WatchHistory.video_code.in_(list(user_profile['watched_codes'])[:20]),
            WatchHistory.user_id != user_id
        ).distinct().limit(50).all()
        
        similar_user_ids = [u.user_id for u in similar_users]
        
        if similar_user_ids:
            # Get videos those users watched (that current user hasn't)
            collab_videos = db.query(WatchHistory.video_code).filter(
                WatchHistory.user_id.in_(similar_user_ids),
                ~WatchHistory.video_code.in_(user_profile['watched_codes']),
                WatchHistory.video_code != code
            ).all()
            
            for v in collab_videos:
                similar_user_videos[v.video_code] += 1
    
    # ========================================
    # Get candidate videos
    # ========================================
    # Exclude current video and already watched
    exclude_codes = user_profile['watched_codes'] | {code}
    
    candidates = db.query(Video).filter(
        ~Video.code.in_(exclude_codes)
    ).all()
    
    if not candidates:
        # Fallback: include watched videos if no new ones
        candidates = db.query(Video).filter(Video.code != code).all()
    
    # ========================================
    # Get global metrics for normalization
    # ========================================
    max_views = db.query(func.max(Video.views)).scalar() or 1
    
    # Get ratings for all candidates
    candidate_codes = [v.code for v in candidates]
    ratings_query = db.query(
        VideoRating.video_code,
        func.avg(VideoRating.rating).label('avg'),
        func.count(VideoRating.id).label('count')
    ).filter(VideoRating.video_code.in_(candidate_codes)).group_by(VideoRating.video_code).all()
    
    ratings_map = {r.video_code: {'avg': float(r.avg), 'count': r.count} for r in ratings_query}
    
    # ========================================
    # Score each candidate
    # ========================================
    scored = []
    now = datetime.utcnow()
    
    for video in candidates:
        score = 0.0
        score_breakdown = {}
        
        video_cast = set(c.name for c in video.cast) if video.cast else set()
        video_categories = set(c.name for c in video.categories) if video.categories else set()
        
        # ----- Content-Based Score (40%) -----
        content_score = 0.0
        
        # Cast matches (+4 each, max 20)
        cast_matches = len(source_cast & video_cast)
        content_score += min(cast_matches * 4, 20)
        
        # Category matches (+2 each, max 10)
        cat_matches = len(source_categories & video_categories)
        content_score += min(cat_matches * 2, 10)
        
        # Studio match (+3)
        if source_studio and video.studio == source_studio:
            content_score += 3
        
        # Series match (+5)
        if source_series and video.series == source_series:
            content_score += 5
        
        # Normalize to 0-1 range (max possible ~38)
        content_score = min(content_score / 38.0, 1.0)
        score_breakdown['content'] = content_score
        
        # ----- User Preference Score (20%) -----
        pref_score = 0.0
        
        if user_profile['cast'] or user_profile['categories'] or user_profile['studios']:
            # Cast preference
            for cast_name in video_cast:
                if cast_name in user_profile['cast']:
                    pref_score += user_profile['cast'][cast_name] * 0.3
            
            # Category preference
            for cat_name in video_categories:
                if cat_name in user_profile['categories']:
                    pref_score += user_profile['categories'][cat_name] * 0.2
            
            # Studio preference
            if video.studio and video.studio in user_profile['studios']:
                pref_score += user_profile['studios'][video.studio] * 0.2
            
            # Normalize (rough estimate)
            max_pref = max(
                sum(user_profile['cast'].values()) * 0.3 +
                sum(user_profile['categories'].values()) * 0.2 +
                sum(user_profile['studios'].values()) * 0.2,
                1
            )
            pref_score = min(pref_score / max_pref, 1.0) if max_pref > 0 else 0
        
        score_breakdown['preference'] = pref_score
        
        # ----- Collaborative Score (30%) -----
        collab_score = 0.0
        if similar_user_videos and video.code in similar_user_videos:
            # Normalize by max collaborative count
            max_collab = max(similar_user_videos.values()) if similar_user_videos else 1
            collab_score = similar_user_videos[video.code] / max_collab
        
        score_breakdown['collaborative'] = collab_score
        
        # ----- Popularity & Quality Score (10%) -----
        pop_score = 0.0
        
        # View count (log scaled)
        views = video.views or 0
        if views > 0 and max_views > 0:
            pop_score += (math.log1p(views) / math.log1p(max_views)) * 0.4
        
        # Rating score
        if video.code in ratings_map:
            rating_info = ratings_map[video.code]
            # Bayesian average
            bayesian = _bayesian_average(rating_info['avg'], rating_info['count'], 3.0, 5)
            pop_score += (bayesian / 5.0) * 0.4
        
        # Recency bonus (videos from last 90 days get boost)
        if video.release_date:
            days_old = (now - video.release_date).days
            if days_old < 90:
                pop_score += (1 - days_old / 90) * 0.2
        
        score_breakdown['popularity'] = pop_score
        
        # ----- Final Weighted Score -----
        final_score = (
            content_score * 0.40 +
            pref_score * 0.20 +
            collab_score * 0.30 +
            pop_score * 0.10
        )
        
        # Bonus for bookmarked content (user explicitly saved similar)
        if video.code in user_profile['bookmarked_codes']:
            final_score *= 1.1
        
        scored.append((video, final_score, video.studio, video.series))
    
    # ========================================
    # Sort and apply diversity
    # ========================================
    scored.sort(key=lambda x: x[1], reverse=True)
    
    # Apply diversity: limit same studio/series
    diverse_results = []
    studio_counts = Counter()
    series_counts = Counter()
    max_per_studio = 3
    max_per_series = 2
    
    for video, score, studio, series in scored:
        studio_key = studio or 'unknown'
        series_key = series or 'unknown'
        
        # Skip if too many from same studio/series
        if studio_counts[studio_key] >= max_per_studio:
            continue
        if series_key != 'unknown' and series_counts[series_key] >= max_per_series:
            continue
        
        diverse_results.append(video)
        studio_counts[studio_key] += 1
        if series_key != 'unknown':
            series_counts[series_key] += 1
        
        if len(diverse_results) >= limit:
            break
    
    # If not enough diverse results, fill with top scored
    if len(diverse_results) < limit:
        for video, score, _, _ in scored:
            if video not in diverse_results:
                diverse_results.append(video)
                if len(diverse_results) >= limit:
                    break
    
    # ========================================
    # Build response with ratings
    # ========================================
    return [
        _video_to_list_item(v, ratings_map.get(v.code, {'average': 0, 'count': 0}))
        for v in diverse_results
    ]


def record_watch(
    db: Session,
    code: str,
    user_id: str,
    duration: int = 0,
    completed: bool = False
) -> bool:
    """
    Record a video watch event for recommendation tracking.
    
    Args:
        code: Video code
        user_id: User identifier (can be anonymous)
        duration: Seconds watched
        completed: Whether user watched > 80% of video
    
    Returns:
        True if recorded successfully
    """
    # Check if video exists
    video = db.query(Video).filter(Video.code == code).first()
    if not video:
        return False
    
    # Create watch history entry
    watch = WatchHistory(
        video_code=code,
        user_id=user_id,
        watch_duration=duration,
        completed=1 if completed else 0
    )
    db.add(watch)
    db.commit()
    
    return True


def get_watch_history(
    db: Session,
    user_id: str,
    page: int = 1,
    page_size: int = 20
) -> PaginatedResponse:
    """Get user's watch history with pagination."""
    # Get watch history ordered by most recent
    history_q = db.query(WatchHistory.video_code, func.max(WatchHistory.watched_at).label('last_watched')).filter(
        WatchHistory.user_id == user_id
    ).group_by(WatchHistory.video_code).order_by(desc('last_watched'))
    
    total = history_q.count()
    
    offset = (page - 1) * page_size
    history_items = history_q.offset(offset).limit(page_size).all()
    
    if not history_items:
        return PaginatedResponse(
            items=[],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=math.ceil(total / page_size) if total > 0 else 1
        )
    
    # Get videos
    video_codes = [h.video_code for h in history_items]
    videos = db.query(Video).filter(Video.code.in_(video_codes)).all()
    video_map = {v.code: v for v in videos}
    ordered_videos = [video_map[code] for code, _ in history_items if code in video_map]
    
    return _paginate(db, ordered_videos, total, page, page_size)


def get_personalized_recommendations(
    db: Session,
    user_id: str,
    page: int = 1,
    page_size: int = 12
) -> PaginatedResponse:
    """
    Get personalized "For You" recommendations based on user's complete profile.
    
    This is different from related videos - it doesn't need a source video,
    instead it builds recommendations purely from user preferences.
    """
    from collections import Counter
    
    # Build user profile
    user_profile = {
        'cast': Counter(),
        'categories': Counter(),
        'studios': Counter(),
        'watched_codes': set(),
        'rated_codes': {},
        'bookmarked_codes': set()
    }
    
    # Get watch history
    watch_history = db.query(WatchHistory.video_code).filter(
        WatchHistory.user_id == user_id
    ).order_by(desc(WatchHistory.watched_at)).limit(200).all()
    user_profile['watched_codes'] = set(w.video_code for w in watch_history)
    
    # Get ratings
    user_ratings = db.query(VideoRating.video_code, VideoRating.rating).filter(
        VideoRating.user_id == user_id
    ).all()
    user_profile['rated_codes'] = {r.video_code: r.rating for r in user_ratings}
    
    # Get bookmarks
    bookmarks = db.query(VideoBookmark.video_code).filter(
        VideoBookmark.user_id == user_id
    ).all()
    user_profile['bookmarked_codes'] = set(b.video_code for b in bookmarks)
    
    # If no history, return trending videos
    if not user_profile['watched_codes'] and not user_profile['bookmarked_codes']:
        return get_trending_videos(db, page, page_size)
    
    # Build preference profile
    watched_videos = db.query(Video).filter(
        Video.code.in_(user_profile['watched_codes'] | user_profile['bookmarked_codes'])
    ).all()
    
    for v in watched_videos:
        weight = 1.0
        if v.code in user_profile['rated_codes']:
            weight = 0.5 + (user_profile['rated_codes'][v.code] / 5.0)
        if v.code in user_profile['bookmarked_codes']:
            weight *= 1.5  # Bookmarks indicate strong preference
        
        for c in (v.cast or []):
            user_profile['cast'][c.name] += weight
        for c in (v.categories or []):
            user_profile['categories'][c.name] += weight
        if v.studio:
            user_profile['studios'][v.studio] += weight
    
    # Get top preferences
    top_cast = [name for name, _ in user_profile['cast'].most_common(10)]
    top_categories = [name for name, _ in user_profile['categories'].most_common(5)]
    top_studios = [name for name, _ in user_profile['studios'].most_common(3)]
    
    # Find candidate videos (not watched)
    candidates = db.query(Video).filter(
        ~Video.code.in_(user_profile['watched_codes'])
    )
    
    # Filter to videos matching preferences
    filters = []
    if top_cast:
        filters.append(Video.cast.any(CastMember.name.in_(top_cast)))
    if top_categories:
        filters.append(Video.categories.any(Category.name.in_(top_categories)))
    if top_studios:
        filters.append(Video.studio.in_(top_studios))
    
    if filters:
        candidates = candidates.filter(or_(*filters))
    
    all_candidates = candidates.all()
    
    if not all_candidates:
        return get_trending_videos(db, page, page_size)
    
    # Score candidates
    scored = []
    now = datetime.utcnow()
    
    for video in all_candidates:
        score = 0.0
        
        video_cast = set(c.name for c in video.cast) if video.cast else set()
        video_categories = set(c.name for c in video.categories) if video.categories else set()
        
        # Cast preference score
        for cast_name in video_cast:
            if cast_name in user_profile['cast']:
                score += user_profile['cast'][cast_name] * 2
        
        # Category preference score
        for cat_name in video_categories:
            if cat_name in user_profile['categories']:
                score += user_profile['categories'][cat_name] * 1.5
        
        # Studio preference score
        if video.studio and video.studio in user_profile['studios']:
            score += user_profile['studios'][video.studio]
        
        # Recency bonus
        if video.release_date:
            days_old = (now - video.release_date).days
            if days_old < 30:
                score *= 1.3
            elif days_old < 90:
                score *= 1.1
        
        # Popularity bonus
        if video.views:
            score += math.log1p(video.views) * 0.1
        
        scored.append((video, score))
    
    # Sort by score
    scored.sort(key=lambda x: x[1], reverse=True)
    
    # Apply diversity
    diverse_results = []
    studio_counts = Counter()
    max_per_studio = 3
    
    for video, score in scored:
        studio_key = video.studio or 'unknown'
        if studio_counts[studio_key] < max_per_studio:
            diverse_results.append(video)
            studio_counts[studio_key] += 1
    
    total = len(diverse_results)
    offset = (page - 1) * page_size
    paginated = diverse_results[offset:offset + page_size]
    
    return _paginate(db, paginated, total, page, page_size)


def clear_watch_history(db: Session, user_id: str) -> dict:
    """Clear all watch history for a user."""
    deleted = db.query(WatchHistory).filter(WatchHistory.user_id == user_id).delete()
    db.commit()
    return {"success": True, "deleted": deleted}


def merge_watch_history(db: Session, from_user_id: str, to_user_id: str) -> dict:
    """
    Merge watch history from anonymous user to logged-in user.
    Used when a user logs in after watching videos anonymously.
    """
    # Get all watch history from anonymous user
    anon_history = db.query(WatchHistory).filter(
        WatchHistory.user_id == from_user_id
    ).all()
    
    if not anon_history:
        return {"merged": 0}
    
    merged_count = 0
    for watch in anon_history:
        # Check if user already has this video in history
        existing = db.query(WatchHistory).filter(
            WatchHistory.user_id == to_user_id,
            WatchHistory.video_code == watch.video_code
        ).first()
        
        if existing:
            # Update if anonymous watch is more recent
            if watch.watched_at > existing.watched_at:
                existing.watched_at = watch.watched_at
                existing.watch_duration = max(existing.watch_duration, watch.watch_duration)
                existing.completed = max(existing.completed, watch.completed)
        else:
            # Create new entry for logged-in user
            new_watch = WatchHistory(
                video_code=watch.video_code,
                user_id=to_user_id,
                watched_at=watch.watched_at,
                watch_duration=watch.watch_duration,
                completed=watch.completed
            )
            db.add(new_watch)
            merged_count += 1
    
    # Delete anonymous history after merge
    db.query(WatchHistory).filter(WatchHistory.user_id == from_user_id).delete()
    
    # Also merge ratings
    anon_ratings = db.query(VideoRating).filter(
        VideoRating.user_id == from_user_id
    ).all()
    
    ratings_merged = 0
    for rating in anon_ratings:
        existing = db.query(VideoRating).filter(
            VideoRating.user_id == to_user_id,
            VideoRating.video_code == rating.video_code
        ).first()
        
        if not existing:
            new_rating = VideoRating(
                video_code=rating.video_code,
                user_id=to_user_id,
                rating=rating.rating,
                created_at=rating.created_at,
                updated_at=rating.updated_at
            )
            db.add(new_rating)
            ratings_merged += 1
    
    # Delete anonymous ratings after merge
    db.query(VideoRating).filter(VideoRating.user_id == from_user_id).delete()
    
    db.commit()
    
    return {
        "merged_history": merged_count,
        "merged_ratings": ratings_merged,
        "total_merged": merged_count + ratings_merged
    }
