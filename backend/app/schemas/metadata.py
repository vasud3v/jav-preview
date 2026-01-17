"""Metadata schemas."""
from typing import Optional
from pydantic import BaseModel


class StatsResponse(BaseModel):
    """Database statistics."""
    total_videos: int
    categories_count: int
    studios_count: int
    cast_count: int
    oldest_video: Optional[str] = None
    newest_video: Optional[str] = None
    database_size_bytes: int = 0


class CategoryResponse(BaseModel):
    """Category with video count."""
    name: str
    video_count: int


class CastResponse(BaseModel):
    """Cast member with video count."""
    name: str
    video_count: int


class CastWithImageResponse(BaseModel):
    """Cast member with image and video count."""
    name: str
    image_url: Optional[str] = None
    video_count: int


class StudioResponse(BaseModel):
    """Studio with video count."""
    name: str
    video_count: int
