"""Migration script to add video_ratings table."""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from scraper.db_models import Base, VideoRating

def migrate():
    """Add video_ratings table to existing database."""
    db_path = Path(__file__).parent.parent / "database" / "videos.db"
    
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return
    
    engine = create_engine(f"sqlite:///{db_path}")
    
    # Check if table already exists
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='video_ratings'"
        ))
        if result.fetchone():
            print("video_ratings table already exists")
            return
    
    # Create the table
    VideoRating.__table__.create(engine)
    print("Created video_ratings table successfully")

if __name__ == "__main__":
    migrate()
