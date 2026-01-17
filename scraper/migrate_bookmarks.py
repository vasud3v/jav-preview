"""Migration script to add video_bookmarks table."""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text, inspect

DATABASE_PATH = "database/videos.db"

def migrate():
    """Add video_bookmarks table if it doesn't exist."""
    engine = create_engine(f"sqlite:///{DATABASE_PATH}")
    
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if 'video_bookmarks' in existing_tables:
        print("video_bookmarks table already exists")
        return
    
    with engine.connect() as conn:
        # Create the bookmarks table
        conn.execute(text("""
            CREATE TABLE video_bookmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_code VARCHAR(50) NOT NULL REFERENCES videos(code) ON DELETE CASCADE,
                user_id VARCHAR(100) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(video_code, user_id)
            )
        """))
        
        # Create indexes
        conn.execute(text("CREATE INDEX idx_bookmark_video ON video_bookmarks(video_code)"))
        conn.execute(text("CREATE INDEX idx_bookmark_user ON video_bookmarks(user_id)"))
        conn.execute(text("CREATE INDEX idx_bookmark_user_created ON video_bookmarks(user_id, created_at)"))
        
        conn.commit()
        print("Created video_bookmarks table with indexes")

if __name__ == "__main__":
    migrate()
