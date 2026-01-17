"""
Migration script to move data from SQLite to Supabase.
Run this once to migrate your existing data.
"""

import os
import sys
from pathlib import Path

# Add scraper to path
sys.path.insert(0, str(Path(__file__).parent / "scraper"))


def load_env_file(env_path):
    """Load environment variables from .env file."""
    if not env_path.exists():
        return False
    
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                os.environ[key.strip()] = value.strip()
    return True


def migrate_videos():
    """Migrate video data from SQLite to Supabase."""
    print("=" * 60)
    print("MIGRATING VIDEOS FROM SQLITE TO SUPABASE")
    print("=" * 60)
    
    # Check if SUPABASE_DB_URL is set
    if not os.getenv('SUPABASE_DB_URL'):
        print("\nError: SUPABASE_DB_URL environment variable not set")
        print("Run 'python setup_supabase.py' first to configure Supabase")
        return False
    
    # Check if SQLite database exists
    sqlite_path = Path("database/videos.db")
    if not sqlite_path.exists():
        print(f"\nNo SQLite database found at {sqlite_path}")
        print("Nothing to migrate.")
        return True
    
    try:
        from database_storage import DatabaseStorage
        from supabase_storage import SupabaseStorage
        
        # Connect to both databases
        print("\nConnecting to SQLite database...")
        sqlite_db = DatabaseStorage(database_path=str(sqlite_path))
        
        print("Connecting to Supabase...")
        supabase_db = SupabaseStorage()
        
        # Get all video codes from SQLite
        print("\nFetching videos from SQLite...")
        codes = sqlite_db.get_all_codes()
        print(f"Found {len(codes)} videos to migrate")
        
        if not codes:
            print("No videos to migrate.")
            return True
        
        # Migrate each video
        success_count = 0
        error_count = 0
        
        print("\nMigrating videos...")
        for i, code in enumerate(codes, 1):
            try:
                # Get video from SQLite
                video = sqlite_db.get_video(code)
                if not video:
                    print(f"[{i}/{len(codes)}] ✗ Could not load {code}")
                    error_count += 1
                    continue
                
                # Save to Supabase
                if supabase_db.save_video(video):
                    success_count += 1
                    if i % 10 == 0:
                        print(f"[{i}/{len(codes)}] Migrated {success_count} videos...")
                else:
                    print(f"[{i}/{len(codes)}] ✗ Failed to save {code}")
                    error_count += 1
                    
            except Exception as e:
                print(f"[{i}/{len(codes)}] ✗ Error migrating {code}: {e}")
                error_count += 1
        
        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE")
        print("=" * 60)
        print(f"Successfully migrated: {success_count}")
        print(f"Errors: {error_count}")
        print(f"Total: {len(codes)}")
        
        # Close connections
        sqlite_db.close()
        supabase_db.close()
        
        return error_count == 0
        
    except ImportError as e:
        print(f"\nError: Missing required modules: {e}")
        print("Make sure you have installed all dependencies:")
        print("  pip install -r scraper/requirements.txt")
        return False
    except Exception as e:
        print(f"\nError during migration: {e}")
        return False


def migrate_scraper_state():
    """Migrate scraper progress state from SQLite to Supabase."""
    print("\n" + "=" * 60)
    print("MIGRATING SCRAPER STATE FROM SQLITE TO SUPABASE")
    print("=" * 60)
    
    # Check if SQLite progress database exists
    progress_path = Path("database/progress.db")
    if not progress_path.exists():
        print(f"\nNo progress database found at {progress_path}")
        print("Nothing to migrate.")
        return True
    
    try:
        from resilience.progress_tracker_db import ProgressTrackerDB
        from supabase_progress_tracker import SupabaseProgressTracker
        
        # Connect to both databases
        print("\nConnecting to SQLite progress database...")
        sqlite_progress = ProgressTrackerDB(db_path=str(progress_path))
        
        print("Connecting to Supabase...")
        supabase_progress = SupabaseProgressTracker()
        
        # Load state from SQLite
        print("\nLoading progress state...")
        state = sqlite_progress.load_state()
        
        if not state:
            print("No active progress state to migrate.")
            return True
        
        print(f"Found progress state:")
        print(f"  Mode: {state.get('mode')}")
        print(f"  Total discovered: {state.get('total_discovered')}")
        print(f"  Completed: {len(state.get('completed_codes', []))}")
        print(f"  Pending: {len(state.get('pending_codes', []))}")
        
        # Create new state in Supabase
        print("\nMigrating to Supabase...")
        supabase_progress.create_new_state(state.get('mode', 'full'))
        
        # Set pending codes
        pending = state.get('pending_codes', [])
        if pending:
            print(f"Setting {len(pending)} pending codes...")
            supabase_progress.set_pending(pending)
        
        # Mark completed codes
        completed = state.get('completed_codes', [])
        if completed:
            print(f"Marking {len(completed)} completed codes...")
            for code in completed:
                supabase_progress.mark_completed(code)
        
        # Get failed videos
        failed = sqlite_progress.get_failed()
        if failed:
            print(f"Migrating {len(failed)} failed videos...")
            for f in failed:
                supabase_progress.record_failed(
                    f.get('code', ''),
                    f.get('url', ''),
                    f.get('reason', '')
                )
        
        print("\n✓ Scraper state migrated successfully")
        
        # Close connections
        sqlite_progress.close()
        supabase_progress.close()
        
        return True
        
    except ImportError as e:
        print(f"\nError: Missing required modules: {e}")
        return False
    except Exception as e:
        print(f"\nError during migration: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run migration."""
    print("\n" + "=" * 60)
    print("SQLITE TO SUPABASE MIGRATION")
    print("=" * 60)
    print("\nThis script will migrate your existing SQLite data to Supabase.")
    print("Make sure you have:")
    print("1. Configured Supabase (run setup_supabase.py)")
    print("2. Applied database migrations (supabase db push)")
    print()
    
    # Load environment variables from scraper/.env
    scraper_env = Path('scraper/.env')
    if scraper_env.exists():
        print(f"Loading environment from {scraper_env}...")
        load_env_file(scraper_env)
    else:
        print(f"Warning: {scraper_env} not found")
    
    # Also try backend/.env for additional vars
    backend_env = Path('backend/.env')
    if backend_env.exists():
        load_env_file(backend_env)
    
    response = input("\nContinue with migration? [y/N]: ").strip().lower()
    if response != 'y':
        print("Migration cancelled.")
        return
    
    # Migrate videos
    videos_ok = migrate_videos()
    
    # Migrate scraper state
    state_ok = migrate_scraper_state()
    
    print("\n" + "=" * 60)
    if videos_ok and state_ok:
        print("✓ MIGRATION COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nYou can now:")
        print("1. Backup your SQLite databases (database/*.db)")
        print("2. Delete them if you want (optional)")
        print("3. Start using Supabase!")
    else:
        print("✗ MIGRATION COMPLETED WITH ERRORS")
        print("=" * 60)
        print("\nPlease review the errors above and try again.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMigration cancelled.")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
