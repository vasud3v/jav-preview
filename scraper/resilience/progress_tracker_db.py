"""
Database-backed progress tracking for resumable extractions.
Uses SQLite for persistent state storage.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Boolean, event
from sqlalchemy.orm import declarative_base, sessionmaker, Session

Base = declarative_base()


class ProgressState(Base):
    """Progress state table - single row for current state."""
    __tablename__ = 'progress_state'
    
    id = Column(Integer, primary_key=True, default=1)
    started_at = Column(DateTime)
    last_updated = Column(DateTime)
    mode = Column(String(50))
    total_discovered = Column(Integer, default=0)
    current_page = Column(Integer, default=1)
    total_pages = Column(Integer, default=0)


class VideoCode(Base):
    """Track individual video codes and their status."""
    __tablename__ = 'video_codes'
    
    code = Column(String(50), primary_key=True)
    status = Column(String(20), default='pending')  # pending, completed, failed
    added_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)


class FailedVideo(Base):
    """Track failed videos with error details."""
    __tablename__ = 'failed_videos'
    
    code = Column(String(50), primary_key=True)
    url = Column(String(500))
    reason = Column(Text)
    attempts = Column(Integer, default=1)
    last_attempt = Column(DateTime, default=datetime.utcnow)


def enable_sqlite_foreign_keys(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class ProgressTrackerDB:
    """Database-backed progress tracker for resumable extractions."""
    
    def __init__(self, db_path: str = "scraper_state/progress.db"):
        """
        Initialize tracker with database path.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._engine = create_engine(f"sqlite:///{db_path}", echo=False)
        event.listen(self._engine, "connect", enable_sqlite_foreign_keys)
        
        Base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine)
    
    def _get_session(self) -> Session:
        return self._Session()
    
    def load_state(self) -> Optional[dict]:
        """Load existing state from database."""
        session = self._get_session()
        try:
            state = session.query(ProgressState).filter(ProgressState.id == 1).first()
            if not state:
                return None
            
            return {
                'started_at': state.started_at.isoformat() if state.started_at else '',
                'last_updated': state.last_updated.isoformat() if state.last_updated else '',
                'mode': state.mode or 'full',
                'total_discovered': state.total_discovered or 0,
                'current_page': state.current_page or 1,
                'total_pages': state.total_pages or 0,
                'completed_codes': self._get_codes_by_status(session, 'completed'),
                'pending_codes': self._get_codes_by_status(session, 'pending')
            }
        finally:
            session.close()
    
    def _get_codes_by_status(self, session: Session, status: str) -> List[str]:
        """Get all codes with given status."""
        return [row[0] for row in session.query(VideoCode.code).filter(VideoCode.status == status).all()]
    
    def save_state(self, state_dict: dict):
        """Save state to database."""
        session = self._get_session()
        try:
            state = session.query(ProgressState).filter(ProgressState.id == 1).first()
            
            if state:
                state.last_updated = datetime.utcnow()
                state.mode = state_dict.get('mode', state.mode)
                state.total_discovered = state_dict.get('total_discovered', state.total_discovered)
                state.current_page = state_dict.get('current_page', state.current_page)
                state.total_pages = state_dict.get('total_pages', state.total_pages)
            else:
                state = ProgressState(
                    id=1,
                    started_at=datetime.utcnow(),
                    last_updated=datetime.utcnow(),
                    mode=state_dict.get('mode', 'full'),
                    total_discovered=state_dict.get('total_discovered', 0),
                    current_page=state_dict.get('current_page', 1),
                    total_pages=state_dict.get('total_pages', 0)
                )
                session.add(state)
            
            session.commit()
        finally:
            session.close()
    
    def mark_completed(self, code: str):
        """Mark a video code as completed."""
        session = self._get_session()
        try:
            video = session.query(VideoCode).filter(VideoCode.code == code).first()
            if video:
                video.status = 'completed'
                video.completed_at = datetime.utcnow()
            else:
                video = VideoCode(code=code, status='completed', completed_at=datetime.utcnow())
                session.add(video)
            session.commit()
        finally:
            session.close()
    
    def get_pending(self) -> List[str]:
        """Get list of pending video codes."""
        session = self._get_session()
        try:
            return self._get_codes_by_status(session, 'pending')
        finally:
            session.close()
    
    def get_completed(self) -> List[str]:
        """Get list of completed video codes."""
        session = self._get_session()
        try:
            return self._get_codes_by_status(session, 'completed')
        finally:
            session.close()

    
    def set_pending(self, codes: List[str]):
        """Set pending codes, excluding already completed ones."""
        session = self._get_session()
        try:
            # Get completed codes
            completed = set(self._get_codes_by_status(session, 'completed'))
            
            # Add new pending codes
            for code in codes:
                if code not in completed:
                    existing = session.query(VideoCode).filter(VideoCode.code == code).first()
                    if not existing:
                        session.add(VideoCode(code=code, status='pending'))
            
            # Update total discovered
            state = session.query(ProgressState).filter(ProgressState.id == 1).first()
            if state:
                state.total_discovered = len(codes)
                state.last_updated = datetime.utcnow()
            
            session.commit()
        finally:
            session.close()
    
    def create_new_state(self, mode: str) -> dict:
        """Create a new progress state for fresh extraction."""
        session = self._get_session()
        try:
            # Clear existing codes
            session.query(VideoCode).delete()
            session.query(ProgressState).delete()
            
            # Create new state
            state = ProgressState(
                id=1,
                started_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
                mode=mode,
                total_discovered=0,
                current_page=1,
                total_pages=0
            )
            session.add(state)
            session.commit()
            
            return {
                'started_at': state.started_at.isoformat(),
                'last_updated': state.last_updated.isoformat(),
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
            session.query(VideoCode).delete()
            session.query(ProgressState).delete()
            session.query(FailedVideo).delete()
            session.commit()
        finally:
            session.close()
    
    def get_stats(self) -> dict:
        """Get current progress statistics."""
        session = self._get_session()
        try:
            completed = session.query(VideoCode).filter(VideoCode.status == 'completed').count()
            pending = session.query(VideoCode).filter(VideoCode.status == 'pending').count()
            total = completed + pending
            
            return {
                'completed': completed,
                'pending': pending,
                'total': total,
                'percent': (completed / total * 100) if total > 0 else 0.0
            }
        finally:
            session.close()
    
    # Failed video tracking
    def record_failed(self, code: str, url: str, reason: str):
        """Record a failed video."""
        session = self._get_session()
        try:
            failed = session.query(FailedVideo).filter(FailedVideo.code == code).first()
            if failed:
                failed.attempts += 1
                failed.reason = reason
                failed.last_attempt = datetime.utcnow()
            else:
                failed = FailedVideo(code=code, url=url, reason=reason)
                session.add(failed)
            
            # Also mark in video_codes
            video = session.query(VideoCode).filter(VideoCode.code == code).first()
            if video:
                video.status = 'failed'
            else:
                session.add(VideoCode(code=code, status='failed'))
            
            session.commit()
        finally:
            session.close()
    
    def get_failed(self) -> List[dict]:
        """Get all failed videos."""
        session = self._get_session()
        try:
            failed = session.query(FailedVideo).all()
            return [{
                'code': f.code,
                'url': f.url,
                'reason': f.reason,
                'attempts': f.attempts,
                'last_attempt': f.last_attempt.isoformat() if f.last_attempt else ''
            } for f in failed]
        finally:
            session.close()
    
    def clear_failed(self, code: str):
        """Remove a video from failed list (after successful retry)."""
        session = self._get_session()
        try:
            session.query(FailedVideo).filter(FailedVideo.code == code).delete()
            session.commit()
        finally:
            session.close()
    
    def update_page(self, current_page: int, total_pages: int = None):
        """Update current page position."""
        session = self._get_session()
        try:
            state = session.query(ProgressState).filter(ProgressState.id == 1).first()
            if state:
                state.current_page = current_page
                if total_pages is not None:
                    state.total_pages = total_pages
                state.last_updated = datetime.utcnow()
                session.commit()
        finally:
            session.close()
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()

    
    # Compatibility with JSON ProgressTracker interface
    @property
    def _state(self):
        """Compatibility property - returns a state-like object."""
        state = self.load_state()
        if not state:
            return None
        
        # Return an object with the expected attributes
        class StateWrapper:
            def __init__(self, data):
                self.started_at = data.get('started_at', '')
                self.last_updated = data.get('last_updated', '')
                self.mode = data.get('mode', 'full')
                self.total_discovered = data.get('total_discovered', 0)
                self.current_page = data.get('current_page', 1)
                self.total_pages = data.get('total_pages', 0)
                self.completed_codes = data.get('completed_codes', [])
                self.pending_codes = data.get('pending_codes', [])
        
        return StateWrapper(state)
    
    def save_state(self, state):
        """Compatibility method - accepts state object or dict."""
        if hasattr(state, '__dict__'):
            # It's a state object, convert to dict
            state_dict = {
                'mode': getattr(state, 'mode', 'full'),
                'total_discovered': getattr(state, 'total_discovered', 0),
                'current_page': getattr(state, 'current_page', 1),
                'total_pages': getattr(state, 'total_pages', 0)
            }
        else:
            state_dict = state
        
        session = self._get_session()
        try:
            db_state = session.query(ProgressState).filter(ProgressState.id == 1).first()
            if db_state:
                db_state.last_updated = datetime.utcnow()
                db_state.mode = state_dict.get('mode', db_state.mode)
                db_state.total_discovered = state_dict.get('total_discovered', db_state.total_discovered)
                db_state.current_page = state_dict.get('current_page', db_state.current_page)
                db_state.total_pages = state_dict.get('total_pages', db_state.total_pages)
                session.commit()
        finally:
            session.close()
