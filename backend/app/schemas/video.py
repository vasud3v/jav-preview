"""Video schemas."""
from typing import List, Optional
from pydantic import BaseModel


class VideoListItem(BaseModel):
    """Compact video for list views."""
    code: str
    title: str
    thumbnail_url: str = ""
    duration: str = ""
    release_date: str = ""
    studio: str = ""
    views: int = 0
    rating_avg: float = 0
    rating_count: int = 0
    like_count: int = 0

    class Config:
        from_attributes = True


class VideoResponse(BaseModel):
    """Full video response."""
    code: str
    title: str
    content_id: str = ""
    duration: str = ""
    release_date: str = ""
    thumbnail_url: str = ""
    cover_url: str = ""
    studio: str = ""
    series: str = ""
    description: str = ""
    embed_urls: List[str] = []
    gallery_images: List[str] = []
    categories: List[str] = []
    cast: List[str] = []
    cast_images: dict = {}
    scraped_at: str = ""
    source_url: str = ""
    views: int = 0

    class Config:
        from_attributes = True


class PaginatedResponse(BaseModel):
    """Paginated response wrapper."""
    items: List[VideoListItem]
    total: int
    page: int
    page_size: int
    total_pages: int


class HomeFeedResponse(BaseModel):
    """Unified home feed with distinct videos per section."""
    featured: List[VideoListItem]
    trending: List[VideoListItem]
    popular: List[VideoListItem]
    top_rated: List[VideoListItem]
    most_liked: List[VideoListItem]
    new_releases: List[VideoListItem]
    classics: List[VideoListItem]
