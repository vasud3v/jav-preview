"""
Database storage module for video metadata.
Drop-in replacement for VideoStorage with SQLite/PostgreSQL backend.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import json

from sqlalchemy import create_engine, event, select, func
from sqlalchemy.orm import sessionmaker, Session

from db_models import Base, Video, Category, CastMember, enable_sqlite_foreign_keys


class DatabaseStorage:
    """
    Database-backed storage for video metadata.
    Implements the same interface as VideoStorage for backward compatibility.
    """
    
    def __init__(self, connection_string: str = None, database_path: str = "database/videos.db"):
        """
        Initialize database storage.
        
        Args:
            connection_string: SQLAlchemy connection string (e.g., "postgresql://user:pass@host/db")
                             If None, uses SQLite at database_path
            database_path: Path for SQLite database file (ignored if connection_string provided)
        """
        self.connection_string = connection_string
        self.database_path = database_path
        self._engine = None
        self._Session = None
        self._init_database()
    
    def _init_database(self):
        """Initialize database connection and create tables."""
        if self.connection_string:
            self._engine = create_engine(self.connection_string, echo=False)
        else:
            # Use SQLite
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
            self._engine = create_engine(f"sqlite:///{self.database_path}", echo=False)
            event.listen(self._engine, "connect", enable_sqlite_foreign_keys)
        
        # Create tables if they don't exist
        Base.metadata.create_all(self._engine)
        
        # Create session factory
        self._Session = sessionmaker(bind=self._engine)
    
    def _get_session(self) -> Session:
        """Get a new database session."""
        return self._Session()

    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats to datetime."""
        if not date_str:
            return None
        
        # Try ISO format first
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            pass
        
        # Try common formats
        formats = [
            '%Y-%m-%d',
            '%d %b %Y',
            '%d %B %Y',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def _get_or_create_category(self, session: Session, name: str) -> Category:
        """Get existing category or create new one."""
        with session.no_autoflush:
            category = session.query(Category).filter(Category.name == name).first()
        if not category:
            category = Category(name=name)
            session.add(category)
        return category
    
    def _get_or_create_cast(self, session: Session, name: str) -> CastMember:
        """Get existing cast member or create new one."""
        with session.no_autoflush:
            cast = session.query(CastMember).filter(CastMember.name == name).first()
        if not cast:
            cast = CastMember(name=name)
            session.add(cast)
        return cast
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._Session = None

    
    def save_video(self, video_data: Any) -> bool:
        """
        Save or update a video record.
        
        Args:
            video_data: Dict or dataclass with video metadata
            
        Returns:
            True on success, False on failure
        """
        try:
            # Normalize input to dict
            if hasattr(video_data, '__dataclass_fields__'):
                from dataclasses import asdict
                data = asdict(video_data)
            else:
                data = dict(video_data) if video_data else {}
            
            # Validate required fields
            code = (data.get('code') or '').strip()
            title = (data.get('title') or '').strip()
            
            if not code:
                print("Error: Cannot save video without code")
                return False
            
            if not title:
                print(f"Error: Cannot save video {code} without title")
                return False
            
            # Validate title - reject if it contains HTML/SVG markup
            if '<' in title or '>' in title or 'clip-path' in title or 'fill=' in title:
                print(f"Error: Cannot save video {code} - title contains invalid markup")
                return False
            
            # Normalize None values to empty defaults
            categories = data.get('categories') or []
            cast_list = data.get('cast') or []
            embed_urls = data.get('embed_urls') or []
            gallery_images = data.get('gallery_images') or []
            cast_images = data.get('cast_images') or {}
            
            session = self._get_session()
            try:
                # Check if video exists
                video = session.query(Video).filter(Video.code == code).first()
                
                if video:
                    # Update existing video
                    video.title = title
                    video.content_id = data.get('content_id') or ''
                    video.duration = data.get('duration') or ''
                    video.release_date = self._parse_date(data.get('release_date') or '')
                    video.thumbnail_url = data.get('thumbnail_url') or ''
                    video.cover_url = data.get('cover_url') or ''
                    video.studio = data.get('studio') or ''
                    video.series = data.get('series') or ''
                    video.description = data.get('description') or ''
                    video.scraped_at = self._parse_date(data.get('scraped_at') or '') or datetime.utcnow()
                    video.source_url = data.get('source_url') or ''
                    video.embed_urls = embed_urls
                    video.gallery_images = gallery_images
                    video.cast_images = cast_images
                    
                    # Update categories
                    video.categories.clear()
                    for cat_name in categories:
                        if cat_name:
                            video.categories.append(self._get_or_create_category(session, cat_name))
                    
                    # Update cast
                    video.cast.clear()
                    for cast_name in cast_list:
                        if cast_name:
                            video.cast.append(self._get_or_create_cast(session, cast_name))
                else:
                    # Create new video
                    video = Video(
                        code=code,
                        title=title,
                        content_id=data.get('content_id') or '',
                        duration=data.get('duration') or '',
                        release_date=self._parse_date(data.get('release_date') or ''),
                        thumbnail_url=data.get('thumbnail_url') or '',
                        cover_url=data.get('cover_url') or '',
                        studio=data.get('studio') or '',
                        series=data.get('series') or '',
                        description=data.get('description') or '',
                        scraped_at=self._parse_date(data.get('scraped_at') or '') or datetime.utcnow(),
                        source_url=data.get('source_url') or ''
                    )
                    video.embed_urls = embed_urls
                    video.gallery_images = gallery_images
                    video.cast_images = cast_images
                    
                    # Add categories
                    for cat_name in categories:
                        if cat_name:
                            video.categories.append(self._get_or_create_category(session, cat_name))
                    
                    # Add cast
                    for cast_name in cast_list:
                        if cast_name:
                            video.cast.append(self._get_or_create_cast(session, cast_name))
                    
                    session.add(video)
                
                session.commit()
                return True
                
            except Exception as e:
                session.rollback()
                print(f"Error saving video {code}: {e}")
                return False
            finally:
                session.close()
                
        except Exception as e:
            print(f"Error processing video data: {e}")
            return False

    
    def get_video(self, code: str) -> Optional[dict]:
        """
        Retrieve video by code.
        
        Args:
            code: Video code to retrieve
            
        Returns:
            Video data as dict, or None if not found
        """
        if not code:
            return None
        
        session = self._get_session()
        try:
            video = session.query(Video).filter(Video.code == code).first()
            if video:
                return video.to_dict()
            return None
        except Exception as e:
            print(f"Error retrieving video {code}: {e}")
            return None
        finally:
            session.close()

    
    def video_exists(self, code: str) -> bool:
        """
        Check if video exists without loading full record.
        
        Args:
            code: Video code to check
            
        Returns:
            True if video exists, False otherwise
        """
        if not code:
            return False
        
        session = self._get_session()
        try:
            exists = session.query(Video.code).filter(Video.code == code).first() is not None
            return exists
        except Exception as e:
            print(f"Error checking video existence {code}: {e}")
            return False
        finally:
            session.close()
    
    def get_all_codes(self) -> List[str]:
        """
        Get list of all video codes.
        
        Returns:
            List of video codes
        """
        session = self._get_session()
        try:
            codes = [row[0] for row in session.query(Video.code).all()]
            return codes
        except Exception as e:
            print(f"Error getting all codes: {e}")
            return []
        finally:
            session.close()
    
    def get_stats(self) -> dict:
        """
        Get database statistics.
        
        Returns:
            Dict with total_videos, categories_count, studios_count, cast_count,
            oldest_video, newest_video, database_size
        """
        session = self._get_session()
        try:
            total_videos = session.query(func.count(Video.code)).scalar() or 0
            categories_count = session.query(func.count(Category.id)).scalar() or 0
            cast_count = session.query(func.count(CastMember.id)).scalar() or 0
            studios_count = session.query(func.count(func.distinct(Video.studio))).filter(Video.studio != '').scalar() or 0
            
            # Date range
            oldest = session.query(func.min(Video.release_date)).scalar()
            newest = session.query(func.max(Video.release_date)).scalar()
            
            # Database size (SQLite only)
            db_size = 0
            if not self.connection_string:
                try:
                    db_size = Path(self.database_path).stat().st_size
                except:
                    pass
            
            return {
                'total_videos': total_videos,
                'categories_count': categories_count,
                'studios_count': studios_count,
                'cast_count': cast_count,
                'oldest_video': oldest.isoformat() if oldest else None,
                'newest_video': newest.isoformat() if newest else None,
                'database_size_bytes': db_size,
                'last_updated': datetime.utcnow().isoformat()
            }
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {}
        finally:
            session.close()

    
    def save_videos_batch(self, videos: List[dict]) -> Tuple[int, List[str]]:
        """
        Save multiple videos in a single transaction.
        
        Args:
            videos: List of video data dicts
            
        Returns:
            Tuple of (success_count, list of failed codes with reasons)
        """
        if not videos:
            return (0, [])
        
        session = self._get_session()
        failed = []
        
        try:
            for video_data in videos:
                # Normalize input
                if hasattr(video_data, '__dataclass_fields__'):
                    from dataclasses import asdict
                    data = asdict(video_data)
                else:
                    data = dict(video_data) if video_data else {}
                
                code = data.get('code', '').strip()
                title = data.get('title', '').strip()
                
                if not code:
                    failed.append("unknown: Missing code")
                    continue
                
                if not title:
                    failed.append(f"{code}: Missing title")
                    continue
                
                try:
                    # Check if video exists
                    video = session.query(Video).filter(Video.code == code).first()
                    
                    if video:
                        # Update existing
                        video.title = title
                        video.content_id = data.get('content_id', '')
                        video.duration = data.get('duration', '')
                        video.release_date = self._parse_date(data.get('release_date', ''))
                        video.thumbnail_url = data.get('thumbnail_url', '')
                        video.cover_url = data.get('cover_url', '')
                        video.studio = data.get('studio', '')
                        video.series = data.get('series', '')
                        video.description = data.get('description', '')
                        video.scraped_at = self._parse_date(data.get('scraped_at', '')) or datetime.utcnow()
                        video.source_url = data.get('source_url', '')
                        video.embed_urls = data.get('embed_urls', [])
                        video.gallery_images = data.get('gallery_images', [])
                        video.cast_images = data.get('cast_images', {})
                        
                        video.categories.clear()
                        for cat_name in data.get('categories', []):
                            if cat_name:
                                video.categories.append(self._get_or_create_category(session, cat_name))
                        
                        video.cast.clear()
                        for cast_name in data.get('cast', []):
                            if cast_name:
                                video.cast.append(self._get_or_create_cast(session, cast_name))
                    else:
                        # Create new
                        video = Video(
                            code=code,
                            title=title,
                            content_id=data.get('content_id', ''),
                            duration=data.get('duration', ''),
                            release_date=self._parse_date(data.get('release_date', '')),
                            thumbnail_url=data.get('thumbnail_url', ''),
                            cover_url=data.get('cover_url', ''),
                            studio=data.get('studio', ''),
                            series=data.get('series', ''),
                            description=data.get('description', ''),
                            scraped_at=self._parse_date(data.get('scraped_at', '')) or datetime.utcnow(),
                            source_url=data.get('source_url', '')
                        )
                        video.embed_urls = data.get('embed_urls', [])
                        video.gallery_images = data.get('gallery_images', [])
                        video.cast_images = data.get('cast_images', {})
                        
                        for cat_name in data.get('categories', []):
                            if cat_name:
                                video.categories.append(self._get_or_create_category(session, cat_name))
                        
                        for cast_name in data.get('cast', []):
                            if cast_name:
                                video.cast.append(self._get_or_create_cast(session, cast_name))
                        
                        session.add(video)
                        
                except Exception as e:
                    failed.append(f"{code}: {str(e)}")
            
            # If any validation failures, rollback entire batch
            if failed:
                session.rollback()
                return (0, failed)
            
            session.commit()
            return (len(videos), [])
            
        except Exception as e:
            session.rollback()
            return (0, [f"Batch error: {str(e)}"])
        finally:
            session.close()

    
    def videos_exist_batch(self, codes: List[str]) -> Dict[str, bool]:
        """
        Check existence of multiple video codes efficiently.
        
        Args:
            codes: List of video codes to check
            
        Returns:
            Dict mapping code to existence (True/False)
        """
        if not codes:
            return {}
        
        session = self._get_session()
        try:
            # Query all existing codes in one go
            existing = set(
                row[0] for row in 
                session.query(Video.code).filter(Video.code.in_(codes)).all()
            )
            
            return {code: code in existing for code in codes}
        except Exception as e:
            print(f"Error checking batch existence: {e}")
            return {code: False for code in codes}
        finally:
            session.close()
    
    # Compatibility methods matching VideoStorage interface
    def query_by_category(self, category: str) -> List[str]:
        """Get video codes for a category (VideoStorage compatibility)."""
        session = self._get_session()
        try:
            codes = [
                row[0] for row in 
                session.query(Video.code)
                .join(Video.categories)
                .filter(Category.name == category)
                .all()
            ]
            return codes
        finally:
            session.close()
    
    def query_by_cast(self, cast_member: str) -> List[str]:
        """Get video codes for a cast member (VideoStorage compatibility)."""
        session = self._get_session()
        try:
            codes = [
                row[0] for row in 
                session.query(Video.code)
                .join(Video.cast)
                .filter(CastMember.name == cast_member)
                .all()
            ]
            return codes
        finally:
            session.close()
    
    def query_by_studio(self, studio: str) -> List[str]:
        """Get video codes for a studio (VideoStorage compatibility)."""
        session = self._get_session()
        try:
            codes = [
                row[0] for row in 
                session.query(Video.code)
                .filter(Video.studio == studio)
                .all()
            ]
            return codes
        finally:
            session.close()


class QueryEngine:
    """Advanced querying capabilities for video database."""
    
    def __init__(self, session_factory):
        """Initialize with SQLAlchemy session factory."""
        self._Session = session_factory
    
    def _get_session(self) -> Session:
        """Get a new database session."""
        return self._Session()
    
    def query_by_code(self, code: str) -> Optional[dict]:
        """Get video by exact code match."""
        if not code:
            return None
        
        session = self._get_session()
        try:
            video = session.query(Video).filter(Video.code == code).first()
            return video.to_dict() if video else None
        finally:
            session.close()
    
    def query_by_category(self, category: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """Get videos in a category with pagination."""
        session = self._get_session()
        try:
            videos = (
                session.query(Video)
                .join(Video.categories)
                .filter(Category.name == category)
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [v.to_dict() for v in videos]
        finally:
            session.close()
    
    def query_by_cast(self, cast_member: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """Get videos featuring a cast member."""
        session = self._get_session()
        try:
            videos = (
                session.query(Video)
                .join(Video.cast)
                .filter(CastMember.name == cast_member)
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [v.to_dict() for v in videos]
        finally:
            session.close()
    
    def query_by_studio(self, studio: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """Get videos from a studio."""
        session = self._get_session()
        try:
            videos = (
                session.query(Video)
                .filter(Video.studio == studio)
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [v.to_dict() for v in videos]
        finally:
            session.close()
    
    def query_by_date_range(self, start_date: str, end_date: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """Get videos within a date range."""
        session = self._get_session()
        try:
            query = session.query(Video)
            
            if start_date:
                try:
                    start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                    query = query.filter(Video.release_date >= start)
                except ValueError:
                    pass
            
            if end_date:
                try:
                    end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    query = query.filter(Video.release_date <= end)
                except ValueError:
                    pass
            
            videos = query.offset(offset).limit(limit).all()
            return [v.to_dict() for v in videos]
        finally:
            session.close()

    
    def search(self, query: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """
        Full-text search on title and description.
        Uses LIKE for SQLite compatibility.
        """
        if not query:
            return []
        
        session = self._get_session()
        try:
            search_term = f"%{query}%"
            videos = (
                session.query(Video)
                .filter(
                    (Video.title.ilike(search_term)) | 
                    (Video.description.ilike(search_term))
                )
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [v.to_dict() for v in videos]
        finally:
            session.close()
    
    def get_all_categories(self) -> List[str]:
        """Get all category names."""
        session = self._get_session()
        try:
            return [row[0] for row in session.query(Category.name).all()]
        finally:
            session.close()
    
    def get_all_studios(self) -> List[str]:
        """Get all studio names."""
        session = self._get_session()
        try:
            return [row[0] for row in session.query(Video.studio).distinct().filter(Video.studio != '').all()]
        finally:
            session.close()
    
    def get_all_cast(self) -> List[str]:
        """Get all cast member names."""
        session = self._get_session()
        try:
            return [row[0] for row in session.query(CastMember.name).all()]
        finally:
            session.close()
