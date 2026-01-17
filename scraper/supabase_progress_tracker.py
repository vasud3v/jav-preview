"""
Supabase-backed progress tracking for resumable extractions.
Replaces SQLite with Supabase PostgreSQL.
"""

from datetime import datetime
from typing import Optional, List
import os

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Boolean, select, delete
from sqlalchemy.orm import declarative_base, sessionmaker, Session

Base = declarative_base()


class ScraperProgress(Base):
    """Scraper progress tracking table."""
    __tablename__ = 'scraper_progress'
    
    id = Column(Integer, primary_key=True)
    start_page = Column(Integer, nullable=False)
    end_page = Column(Integer, nullable=False)
    current_page = Column(Integer, nullable=False)
    total_videos = Column(Integer, default=0)
    successful_videos = Column(Integer, default=0)
    failed_videos = Column(Integer, default=0)
    started_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)


class ScraperCompleted(Base):
    """Completed video codes."""
    __tablename__ = 'scraper_completed'
    
    id = Column(Integer, primary_key=True)
    progress_id = Column(Integer, nullable=True)
    code = Column(String(50), nullable=False)
    completed_at = Column(DateTime, default=datetime.utcnow)


class ScraperPending(Base):
    """Pending video codes."""
    __tablename__ = 'scraper_pending'
    
    id = Column(Integer, primary_key=True)
    progress_id = Column(Integer, nullable=True)
    code = Column(String(50), nullable=False)
    added_at = Column(DateTime, default=datetime.utcnow)


class ScraperFailed(Base):
    """Failed video codes with error details."""
    __tablename__ = 'scraper_failed'
    
    code = Column(String(50), primary_key=True)
    error_message = Column(Text)
    last_attempt = Column(DateTime, default=datetime.utcnow)
    attempt_count = Column(Integer, default=1)


class SupabaseProgressTracker:
    """Supabase-backed progress tracker for resumable extractions."""
    
    def __init__(self, connection_string: str = None):
        """
        Initialize tracker with Supabase connection.
        
        Args:
            connection_string: PostgreSQL connection string
                             If None, reads from SUPABASE_DB_URL environment variable
        """
        self.connection_string = connection_string or os.getenv('SUPABASE_DB_URL')
        if not self.connection_string:
            raise ValueError("SUPABASE_DB_URL must be set in environment or passed as argument")
        
        self._engine = create_engine(
            self.connection_string,
            echo=False,
            pool_pre_ping=True
        )
        
        self._Session = sessionmaker(bind=self._engine)
        self._current_progress_id = None
    
    def _get_session(self) -> Session:
        return self._Session()
    
    def load_state(self) -> Optional[dict]:
        """Load existing active state from database."""
        session = self._get_session()
        try:
            progress = session.query(ScraperProgress).filter(
                ScraperProgress.is_active == True
            ).order_by(ScraperProgress.id.desc()).first()
            
            if not progress:
                self._current_progress_id = None  # Explicitly set to None
                return None
            
            self._current_progress_id = progress.id
            
            # Get completed and pending codes
            completed = [row[0] for row in session.query(ScraperCompleted.code).filter(
                ScraperCompleted.progress_id == progress.id
            ).all()]
            
            pending = [row[0] for row in session.query(ScraperPending.code).filter(
                ScraperPending.progress_id == progress.id
            ).all()]
            
            return {
                'started_at': progress.started_at.isoformat() if progress.started_at else '',
                'last_updated': progress.updated_at.isoformat() if progress.updated_at else '',
                'mode': 'full',  # Can be extended
                'total_discovered': progress.total_videos or 0,
                'current_page': progress.current_page or 1,
                'total_pages': progress.end_page or 0,
                'completed_codes': completed,
                'pending_codes': pending
            }
        finally:
            session.close()
    
    def save_state(self, state_dict: dict):
        """Save state to database."""
        session = self._get_session()
        try:
            if self._current_progress_id:
                progress = session.query(ScraperProgress).filter(
                    ScraperProgress.id == self._current_progress_id
                ).first()
                
                if progress:
                    progress.updated_at = datetime.utcnow()
                    progress.current_page = state_dict.get('current_page', progress.current_page)
                    progress.total_videos = state_dict.get('total_discovered', progress.total_videos)
                    progress.end_page = state_dict.get('total_pages', progress.end_page)
                    session.commit()
        finally:
            session.close()
    
    def mark_completed(self, code: str):
        """Mark a video code as completed."""
        session = self._get_session()
        try:
            # Remove from pending if exists
            session.query(ScraperPending).filter(
                ScraperPending.code == code,
                ScraperPending.progress_id == self._current_progress_id
            ).delete()
            
            # Add to completed
            completed = ScraperCompleted(
                progress_id=self._current_progress_id,
                code=code
            )
            session.add(completed)
            
            # Update progress stats
            if self._current_progress_id:
                progress = session.query(ScraperProgress).filter(
                    ScraperProgress.id == self._current_progress_id
                ).first()
                if progress:
                    progress.successful_videos += 1
                    progress.updated_at = datetime.utcnow()
            
            session.commit()
        finally:
            session.close()
    
    def get_pending(self) -> List[str]:
        """Get list of pending video codes."""
        session = self._get_session()
        try:
            return [row[0] for row in session.query(ScraperPending.code).filter(
                ScraperPending.progress_id == self._current_progress_id
            ).all()]
        finally:
            session.close()
    
    def get_completed(self) -> List[str]:
        """Get list of completed video codes."""
        session = self._get_session()
        try:
            return [row[0] for row in session.query(ScraperCompleted.code).filter(
                ScraperCompleted.progress_id == self._current_progress_id
            ).all()]
        finally:
            session.close()
    
    def set_pending(self, codes: List[str]):
        """Set pending codes, excluding already completed ones."""
        session = self._get_session()
        try:
            # Get completed codes
            completed = set([row[0] for row in session.query(ScraperCompleted.code).filter(
                ScraperCompleted.progress_id == self._current_progress_id
            ).all()])
            
            # Add new pending codes
            for code in codes:
                if code not in completed:
                    # Check if already pending
                    exists = session.query(ScraperPending).filter(
                        ScraperPending.code == code,
                        ScraperPending.progress_id == self._current_progress_id
                    ).first()
                    
                    if not exists:
                        session.add(ScraperPending(
                            progress_id=self._current_progress_id,
                            code=code
                        ))
            
            # Update total discovered
            if self._current_progress_id:
                progress = session.query(ScraperProgress).filter(
                    ScraperProgress.id == self._current_progress_id
                ).first()
                if progress:
                    progress.total_videos = len(codes)
                    progress.updated_at = datetime.utcnow()
            
            session.commit()
        finally:
            session.close()
    
    def create_new_state(self, mode: str) -> dict:
        """Create a new progress state for fresh extraction."""
        session = self._get_session()
        try:
            # Deactivate old progress
            session.query(ScraperProgress).update({'is_active': False})
            
            # Create new progress
            progress = ScraperProgress(
                start_page=1,
                end_page=0,
                current_page=1,
                total_videos=0,
                successful_videos=0,
                failed_videos=0,
                is_active=True
            )
            session.add(progress)
            session.flush()
            
            self._current_progress_id = progress.id
            
            session.commit()
            
            return {
                'started_at': progress.started_at.isoformat(),
                'last_updated': progress.updated_at.isoformat(),
                'mode': mode,
                'total_discovered': 0,
                'current_page': 1,
                'total_pages': 0,
                'completed_codes': [],
                'pending_codes': []
            }
        finally:
            session.close()
    
    def reset(self):
        """Clear all progress state."""
        session = self._get_session()
        try:
            if self._current_progress_id:
                session.query(ScraperPending).filter(
                    ScraperPending.progress_id == self._current_progress_id
                ).delete()
                session.query(ScraperCompleted).filter(
                    ScraperCompleted.progress_id == self._current_progress_id
                ).delete()
                session.query(ScraperProgress).filter(
                    ScraperProgress.id == self._current_progress_id
                ).delete()
            session.commit()
            self._current_progress_id = None
        finally:
            session.close()
    
    def get_stats(self) -> dict:
        """Get current progress statistics."""
        session = self._get_session()
        try:
            if not self._current_progress_id:
                return {'completed': 0, 'pending': 0, 'total': 0, 'percent': 0.0}
            
            completed = session.query(ScraperCompleted).filter(
                ScraperCompleted.progress_id == self._current_progress_id
            ).count()
            
            pending = session.query(ScraperPending).filter(
                ScraperPending.progress_id == self._current_progress_id
            ).count()
            
            total = completed + pending
            
            return {
                'completed': completed,
                'pending': pending,
                'total': total,
                'percent': (completed / total * 100) if total > 0 else 0.0
            }
        finally:
            session.close()
    
    def record_failed(self, code: str, url: str, reason: str):
        """Record a failed video."""
        session = self._get_session()
        try:
            failed = session.query(ScraperFailed).filter(ScraperFailed.code == code).first()
            if failed:
                failed.attempt_count += 1
                failed.error_message = reason
                failed.last_attempt = datetime.utcnow()
            else:
                failed = ScraperFailed(
                    code=code,
                    error_message=reason,
                    attempt_count=1
                )
                session.add(failed)
            
            # Update progress stats
            if self._current_progress_id:
                progress = session.query(ScraperProgress).filter(
                    ScraperProgress.id == self._current_progress_id
                ).first()
                if progress:
                    progress.failed_videos += 1
                    progress.updated_at = datetime.utcnow()
            
            session.commit()
        finally:
            session.close()
    
    def get_failed(self) -> List[dict]:
        """Get all failed videos."""
        session = self._get_session()
        try:
            failed = session.query(ScraperFailed).all()
            return [{
                'code': f.code,
                'url': '',
                'reason': f.error_message,
                'attempts': f.attempt_count,
                'last_attempt': f.last_attempt.isoformat() if f.last_attempt else ''
            } for f in failed]
        finally:
            session.close()
    
    def clear_failed(self, code: str):
        """Remove a video from failed list (after successful retry)."""
        session = self._get_session()
        try:
            session.query(ScraperFailed).filter(ScraperFailed.code == code).delete()
            session.commit()
        finally:
            session.close()
    
    def update_page(self, current_page: int, total_pages: int = None):
        """Update current page position."""
        session = self._get_session()
        try:
            if self._current_progress_id:
                progress = session.query(ScraperProgress).filter(
                    ScraperProgress.id == self._current_progress_id
                ).first()
                if progress:
                    progress.current_page = current_page
                    if total_pages is not None:
                        progress.end_page = total_pages
                    progress.updated_at = datetime.utcnow()
                    session.commit()
        finally:
            session.close()
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
