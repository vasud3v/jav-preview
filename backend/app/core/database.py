"""Database connection and session management."""
import sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Add project root to path for scraper imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scraper.db_models import Base
from backend.app.core.config import settings

# Use Supabase PostgreSQL connection
if not settings.supabase_db_url:
    raise RuntimeError("SUPABASE_DB_URL must be configured")

engine = create_engine(settings.supabase_db_url, echo=settings.debug, pool_pre_ping=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency for database sessions."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
