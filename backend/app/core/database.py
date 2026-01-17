"""Database connection and session management.

Note: This module is kept for backwards compatibility but is not used
when running in REST API mode. All database operations now go through
the Supabase REST API client.
"""
from app.core.config import settings

# For REST API mode, we don't need SQLAlchemy
# Only initialize if SUPABASE_DB_URL is provided
engine = None
SessionLocal = None
Base = None

if settings.supabase_db_url:
    try:
        import sys
        from pathlib import Path
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        # Add project root to path for scraper imports
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
        
        from scraper.db_models import Base as ScraperBase
        
        engine = create_engine(settings.supabase_db_url, echo=settings.debug, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base = ScraperBase
    except Exception as e:
        print(f"Warning: Could not initialize SQLAlchemy database: {e}")


def get_db():
    """Dependency for database sessions."""
    if SessionLocal is None:
        raise RuntimeError("Database not available in REST API mode")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
