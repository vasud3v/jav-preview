"""
Supabase storage module for video metadata and scraper state.
Replaces SQLite with Supabase PostgreSQL.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
import os
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker, Session

from db_models import (
    Base, Video, Category, CastMember,
    video_categories, video_cast
)


class SupabaseStorage:
    """
    Supabase-backed storage for video metadata.
    Uses PostgreSQL connection string from environment.
    """
    
    def __init__(self, connection_string: str = None):
        """
        Initialize Supabase storage.
        
        Args:
            connection_string: PostgreSQL connection string
                             If None, reads from SUPABASE_DB_URL environment variable
        """
        self.connection_string = connection_string or os.getenv('SUPABASE_DB_URL')
        if not self.connection_string:
            raise ValueError("SUPABASE_DB_URL must be set in environment or passed as argument")
        
        self._engine = None
        self._Session = None
        self._init_database()
    
    def _init_database(self):
        """Initialize database connection."""
        self._engine = create_engine(
            self.connection_string,
            echo=False,
            pool_pre_ping=True,
            pool_size=5,  # Smaller base pool
            max_overflow=10,  # Smaller overflow
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_timeout=30,  # Timeout after 30 seconds
            connect_args={
                'connect_timeout': 10,
                'options': '-c statement_timeout=30000'  # 30 second query timeout
            }
        )
        
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
            session.flush()
        return category
    
    def _get_or_create_cast(self, session: Session, name: str) -> CastMember:
        """Get existing cast member or create new one."""
        with session.no_autoflush:
            cast = session.query(CastMember).filter(CastMember.name == name).first()
        if not cast:
            cast = CastMember(name=name)
            session.add(cast)
            session.flush()
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
            oldest_video, newest_video
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
            
            return {
                'total_videos': total_videos,
                'categories_count': categories_count,
                'studios_count': studios_count,
                'cast_count': cast_count,
                'oldest_video': oldest.isoformat() if oldest else None,
                'newest_video': newest.isoformat() if newest else None,
                'last_updated': datetime.utcnow().isoformat()
            }
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {}
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
    
    # Compatibility methods
    def query_by_category(self, category: str) -> List[str]:
        """Get video codes for a category."""
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
        """Get video codes for a cast member."""
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
        """Get video codes for a studio."""
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
