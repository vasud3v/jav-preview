"""Database models - only available when SQLAlchemy mode is enabled.

For REST API mode, the models are not needed as all database
operations go through the Supabase REST API client.
"""

# Try to import models, but don't fail if SQLAlchemy is not available
try:
    import sys
    from pathlib import Path
    
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
    
    from scraper.db_models import Video, Category, CastMember, VideoRating, VideoBookmark, WatchHistory, Comment, CommentVote, Base
    
    __all__ = ["Video", "Category", "CastMember", "VideoRating", "VideoBookmark", "WatchHistory", "Comment", "CommentVote", "Base"]
except Exception as e:
    # REST API mode - models are not available
    Video = None
    Category = None
    CastMember = None
    VideoRating = None
    VideoBookmark = None
    WatchHistory = None
    Comment = None
    CommentVote = None
    Base = None
    
    __all__ = []
