"""Metadata service - stats, categories, studios, cast."""
from typing import List
import json
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.models import Video, Category, CastMember
from app.schemas import StatsResponse, CategoryResponse, CastResponse, StudioResponse
from app.schemas.metadata import CastWithImageResponse


def get_stats(db: Session) -> StatsResponse:
    """Get database statistics."""
    total_videos = db.query(func.count(Video.code)).scalar() or 0
    categories_count = db.query(func.count(Category.id)).scalar() or 0
    cast_count = db.query(func.count(CastMember.id)).scalar() or 0
    studios_count = db.query(func.count(func.distinct(Video.studio))).filter(Video.studio != '').scalar() or 0
    
    oldest = db.query(func.min(Video.release_date)).scalar()
    newest = db.query(func.max(Video.release_date)).scalar()
    
    return StatsResponse(
        total_videos=total_videos,
        categories_count=categories_count,
        studios_count=studios_count,
        cast_count=cast_count,
        oldest_video=oldest.isoformat() if oldest else None,
        newest_video=newest.isoformat() if newest else None,
    )


def get_all_categories(db: Session) -> List[CategoryResponse]:
    """Get all categories with video counts."""
    results = (
        db.query(Category.name, func.count(Video.code))
        .join(Category.videos)
        .group_by(Category.name)
        .order_by(desc(func.count(Video.code)))
        .all()
    )
    return [CategoryResponse(name=name, video_count=count) for name, count in results]


def get_all_studios(db: Session) -> List[StudioResponse]:
    """Get all studios with video counts."""
    results = (
        db.query(Video.studio, func.count(Video.code))
        .filter(Video.studio != '')
        .group_by(Video.studio)
        .order_by(desc(func.count(Video.code)))
        .all()
    )
    return [StudioResponse(name=name, video_count=count) for name, count in results]


def get_all_cast(db: Session) -> List[CastResponse]:
    """Get all cast members with video counts."""
    results = (
        db.query(CastMember.name, func.count(Video.code))
        .join(CastMember.videos)
        .group_by(CastMember.name)
        .order_by(desc(func.count(Video.code)))
        .all()
    )
    return [CastResponse(name=name, video_count=count) for name, count in results]



def get_cast_with_images(db: Session, limit: int = 20) -> List[CastWithImageResponse]:
    """Get cast members with their images and video counts."""
    import re
    
    # Get cast with video counts
    cast_results = (
        db.query(CastMember.name, func.count(Video.code))
        .join(CastMember.videos)
        .group_by(CastMember.name)
        .order_by(desc(func.count(Video.code)))
        .limit(limit * 10)  # Get more to filter
        .all()
    )
    
    cast_with_images = []
    for name, count in cast_results:
        # Find a video that has this cast member's image
        video = (
            db.query(Video)
            .join(Video.cast)
            .filter(CastMember.name == name)
            .filter(Video._cast_images != '{}')
            .filter(Video._cast_images.isnot(None))
            .first()
        )
        
        image_url = None
        if video and video.cast_images:
            image_url = video.cast_images.get(name)
        
        # Include cast with images
        if image_url:
            cast_with_images.append(CastWithImageResponse(
                name=name,
                image_url=image_url,
                video_count=count
            ))
        
        if len(cast_with_images) >= limit:
            break
    
    # If we didn't find enough with images, add some without images
    if len(cast_with_images) < limit:
        for name, count in cast_results:
            if any(c.name == name for c in cast_with_images):
                continue
            cast_with_images.append(CastWithImageResponse(
                name=name,
                image_url=None,
                video_count=count
            ))
            if len(cast_with_images) >= limit:
                break
    
    return cast_with_images


def get_all_cast_with_images(db: Session) -> List[CastWithImageResponse]:
    """Get all cast members with their images and video counts."""
    # Get all cast with video counts
    cast_results = (
        db.query(CastMember.name, func.count(Video.code))
        .join(CastMember.videos)
        .group_by(CastMember.name)
        .order_by(desc(func.count(Video.code)))
        .all()
    )
    
    # Build a map of cast images from all videos
    cast_image_map = {}
    videos_with_images = (
        db.query(Video._cast_images)
        .filter(Video._cast_images != '{}')
        .filter(Video._cast_images.isnot(None))
        .all()
    )
    
    for (cast_images_json,) in videos_with_images:
        if cast_images_json:
            try:
                images = json.loads(cast_images_json)
                for name, url in images.items():
                    if name not in cast_image_map and url:
                        cast_image_map[name] = url
            except:
                pass
    
    # Build response with images where available
    result = []
    for name, count in cast_results:
        result.append(CastWithImageResponse(
            name=name,
            image_url=cast_image_map.get(name),
            video_count=count
        ))
    
    return result
