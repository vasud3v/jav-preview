"""
SQLAlchemy database models for video storage.
Supports SQLite (default) and PostgreSQL backends.
"""

from datetime import datetime
from typing import List, Optional
import json

from sqlalchemy import (
    Column, String, Text, DateTime, Integer, Float, Table, ForeignKey, Index,
    create_engine, event, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base, Session

Base = declarative_base()

# Many-to-many association tables
video_categories = Table(
    'video_categories', Base.metadata,
    Column('video_code', String(50), ForeignKey('videos.code', ondelete='CASCADE'), primary_key=True),
    Column('category_id', Integer, ForeignKey('categories.id', ondelete='CASCADE'), primary_key=True)
)

video_cast = Table(
    'video_cast', Base.metadata,
    Column('video_code', String(50), ForeignKey('videos.code', ondelete='CASCADE'), primary_key=True),
    Column('cast_id', Integer, ForeignKey('cast_members.id', ondelete='CASCADE'), primary_key=True)
)


class Video(Base):
    """Video metadata model."""
    __tablename__ = 'videos'
    
    code = Column(String(50), primary_key=True)
    content_id = Column(String(100))
    title = Column(String(500), nullable=False)
    duration = Column(String(20))
    release_date = Column(DateTime)
    thumbnail_url = Column(String(500))
    cover_url = Column(String(500))
    studio = Column(String(200))
    series = Column(String(200))
    description = Column(Text)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    source_url = Column(String(500))
    views = Column(Integer, default=0)
    
    # JSON fields for simple lists (stored as JSON strings)
    _embed_urls = Column('embed_urls', Text, default='[]')
    _gallery_images = Column('gallery_images', Text, default='[]')
    _cast_images = Column('cast_images', Text, default='{}')
    
    # Relationships
    categories = relationship("Category", secondary=video_categories, back_populates="videos")
    cast = relationship("CastMember", secondary=video_cast, back_populates="videos")

    
    # Indexes for frequently queried columns
    __table_args__ = (
        Index('idx_video_studio', 'studio'),
        Index('idx_video_release_date', 'release_date'),
    )
    
    @property
    def embed_urls(self) -> List[str]:
        """Get embed URLs as list."""
        try:
            return json.loads(self._embed_urls) if self._embed_urls else []
        except json.JSONDecodeError:
            return []
    
    @embed_urls.setter
    def embed_urls(self, value: List[str]):
        """Set embed URLs from list."""
        self._embed_urls = json.dumps(value) if value else '[]'
    
    @property
    def gallery_images(self) -> List[str]:
        """Get gallery images as list."""
        try:
            return json.loads(self._gallery_images) if self._gallery_images else []
        except json.JSONDecodeError:
            return []
    
    @gallery_images.setter
    def gallery_images(self, value: List[str]):
        """Set gallery images from list."""
        self._gallery_images = json.dumps(value) if value else '[]'
    
    @property
    def cast_images(self) -> dict:
        """Get cast images as dict."""
        try:
            return json.loads(self._cast_images) if self._cast_images else {}
        except json.JSONDecodeError:
            return {}
    
    @cast_images.setter
    def cast_images(self, value: dict):
        """Set cast images from dict."""
        self._cast_images = json.dumps(value) if value else '{}'
    
    def to_dict(self) -> dict:
        """Convert video to dictionary format matching JSON storage."""
        return {
            'code': self.code,
            'content_id': self.content_id or '',
            'title': self.title,
            'duration': self.duration or '',
            'release_date': self.release_date.isoformat() if self.release_date else '',
            'thumbnail_url': self.thumbnail_url or '',
            'cover_url': self.cover_url or '',
            'embed_urls': self.embed_urls,
            'gallery_images': self.gallery_images,
            'categories': [c.name for c in self.categories],
            'cast': [c.name for c in self.cast],
            'cast_images': self.cast_images,
            'studio': self.studio or '',
            'series': self.series or '',
            'description': self.description or '',
            'scraped_at': self.scraped_at.isoformat() if self.scraped_at else '',
            'source_url': self.source_url or ''
        }


class Category(Base):
    """Category model for video categorization."""
    __tablename__ = 'categories'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    
    videos = relationship("Video", secondary=video_categories, back_populates="categories")
    
    __table_args__ = (
        Index('idx_category_name', 'name'),
    )


class CastMember(Base):
    """Cast member model."""
    __tablename__ = 'cast_members'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), unique=True, nullable=False)
    
    videos = relationship("Video", secondary=video_cast, back_populates="cast")
    
    __table_args__ = (
        Index('idx_cast_name', 'name'),
    )


class VideoRating(Base):
    """Video rating model - stores individual user ratings."""
    __tablename__ = 'video_ratings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    video_code = Column(String(50), ForeignKey('videos.code', ondelete='CASCADE'), nullable=False)
    user_id = Column(String(100), nullable=False)  # Can be user ID or anonymous session ID
    rating = Column(Integer, nullable=False)  # 1-5 stars
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('video_code', 'user_id', name='unique_user_video_rating'),
        Index('idx_rating_video', 'video_code'),
        Index('idx_rating_user', 'user_id'),
    )


class VideoBookmark(Base):
    """Video bookmark model - stores user bookmarks."""
    __tablename__ = 'video_bookmarks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    video_code = Column(String(50), ForeignKey('videos.code', ondelete='CASCADE'), nullable=False)
    user_id = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('video_code', 'user_id', name='unique_user_video_bookmark'),
        Index('idx_bookmark_video', 'video_code'),
        Index('idx_bookmark_user', 'user_id'),
        Index('idx_bookmark_user_created', 'user_id', 'created_at'),
    )


class WatchHistory(Base):
    """Watch history model - tracks user viewing history."""
    __tablename__ = 'watch_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    video_code = Column(String(50), ForeignKey('videos.code', ondelete='CASCADE'), nullable=False)
    user_id = Column(String(100), nullable=False)
    watched_at = Column(DateTime, default=datetime.utcnow)
    watch_duration = Column(Integer, default=0)  # seconds watched
    completed = Column(Integer, default=0)  # 1 if watched > 80%
    
    __table_args__ = (
        Index('idx_watch_video', 'video_code'),
        Index('idx_watch_user', 'user_id'),
        Index('idx_watch_user_time', 'user_id', 'watched_at'),
    )


class Comment(Base):
    """Reddit-style threaded comment model."""
    __tablename__ = 'comments'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    video_code = Column(String(50), ForeignKey('videos.code', ondelete='CASCADE'), nullable=False)
    user_id = Column(String(100), nullable=False)
    username = Column(String(100), nullable=True)  # Display name
    parent_id = Column(Integer, ForeignKey('comments.id', ondelete='CASCADE'), nullable=True)  # For threading
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deleted = Column(Integer, default=0)  # Soft delete for threads
    
    __table_args__ = (
        Index('idx_comment_video', 'video_code'),
        Index('idx_comment_user', 'user_id'),
        Index('idx_comment_parent', 'parent_id'),
        Index('idx_comment_created', 'created_at'),
    )


class CommentVote(Base):
    """Upvote/downvote for comments."""
    __tablename__ = 'comment_votes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id = Column(Integer, ForeignKey('comments.id', ondelete='CASCADE'), nullable=False)
    user_id = Column(String(100), nullable=False)
    vote = Column(Integer, nullable=False)  # 1 for upvote, -1 for downvote
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('comment_id', 'user_id', name='unique_comment_vote'),
        Index('idx_vote_comment', 'comment_id'),
        Index('idx_vote_user', 'user_id'),
    )


def enable_sqlite_foreign_keys(dbapi_conn, connection_record):
    """Enable foreign key support for SQLite."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def create_database(connection_string: str = None, database_path: str = "database/videos.db"):
    """
    Create database engine and tables.
    
    Args:
        connection_string: SQLAlchemy connection string for PostgreSQL
        database_path: Path for SQLite database (used if connection_string is None)
    
    Returns:
        SQLAlchemy engine
    """
    if connection_string:
        engine = create_engine(connection_string, echo=False)
    else:
        # Use SQLite
        from pathlib import Path
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(f"sqlite:///{database_path}", echo=False)
        # Enable foreign keys for SQLite
        event.listen(engine, "connect", enable_sqlite_foreign_keys)
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    return engine
