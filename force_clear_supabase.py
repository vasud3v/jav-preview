"""
Force clear all data from Supabase using direct SQL connection.
"""

import os
from pathlib import Path

# Load environment variables
backend_env = Path('backend/.env')
if backend_env.exists():
    with open(backend_env) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                os.environ[key.strip()] = value.strip()

db_url = os.getenv('SUPABASE_DB_URL')
if not db_url:
    print("Error: SUPABASE_DB_URL not found in environment")
    exit(1)

print("Connecting to Supabase...")
print(f"Connection: {db_url.split('@')[1] if '@' in db_url else 'unknown'}")

try:
    from sqlalchemy import create_engine, text
    
    engine = create_engine(db_url, echo=True)
    
    with engine.connect() as conn:
        print("\nDeleting all data...")
        
        # Delete in correct order to respect foreign keys
        tables = [
            'comment_votes',
            'comments',
            'watch_history',
            'video_bookmarks',
            'video_ratings',
            'video_cast',
            'video_categories',
            'videos',
            'cast_members',
            'categories',
            'scraper_failed',
            'scraper_pending',
            'scraper_completed',
            'scraper_progress',
            'scraper_random_state'
        ]
        
        for table in tables:
            try:
                result = conn.execute(text(f"DELETE FROM {table}"))
                conn.commit()
                print(f"✓ Cleared {table}: {result.rowcount} rows deleted")
            except Exception as e:
                print(f"✗ Error clearing {table}: {e}")
        
        print("\n" + "=" * 60)
        print("Verifying tables are empty...")
        print("=" * 60)
        
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                status = "✓" if count == 0 else "✗"
                print(f"{status} {table}: {count} rows")
            except Exception as e:
                print(f"✗ Error checking {table}: {e}")
    
    print("\n✓ All data cleared successfully!")
    
except ImportError:
    print("\nError: SQLAlchemy not installed")
    print("Install it with: pip install sqlalchemy psycopg2-binary")
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
