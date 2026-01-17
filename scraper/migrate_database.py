"""
Migration script to convert old database structure to new clean structure.
Run this once to migrate existing data.
"""

import json
import shutil
from pathlib import Path
from storage_v2 import VideoStorage


def migrate_database(base_path: str = "database"):
    """Migrate from old structure to new clean structure."""
    base = Path(base_path)
    
    print("=" * 50)
    print("Database Migration: Old Structure -> New Structure")
    print("=" * 50)
    
    # Step 1: Clean up video files (remove video_id field)
    print("\n[1/4] Cleaning video files...")
    videos_dir = base / "videos"
    video_count = 0
    
    if videos_dir.exists():
        for video_file in videos_dir.glob("*.json"):
            try:
                with open(video_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Remove video_id if present
                if 'video_id' in data:
                    del data['video_id']
                    with open(video_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                
                video_count += 1
            except Exception as e:
                print(f"  Warning: Could not process {video_file}: {e}")
    
    print(f"  Processed {video_count} video files")
    
    # Step 2: Delete obsolete directories
    print("\n[2/4] Removing obsolete directories...")
    obsolete_dirs = [
        base / "by_category",
        base / "by_cast", 
        base / "by_studio",
        base / "by_date",
        base / "embeds",
    ]
    
    for dir_path in obsolete_dirs:
        if dir_path.exists():
            try:
                shutil.rmtree(dir_path)
                print(f"  Deleted: {dir_path}")
            except Exception as e:
                print(f"  Warning: Could not delete {dir_path}: {e}")
        else:
            print(f"  Skipped (not found): {dir_path}")
    
    # Step 3: Delete old index files
    print("\n[3/4] Removing old index files...")
    old_index_files = [
        base / "indexes" / "all_videos.json",
        base / "indexes" / "by_code.json",
        base / "indexes" / "by_studio.json",
        base / "indexes" / "by_category.json",
        base / "indexes" / "by_cast.json",
        base / "indexes" / "stats.json",
    ]
    
    for index_file in old_index_files:
        if index_file.exists():
            try:
                index_file.unlink()
                print(f"  Deleted: {index_file}")
            except Exception as e:
                print(f"  Warning: Could not delete {index_file}: {e}")
        else:
            print(f"  Skipped (not found): {index_file}")
    
    # Step 4: Rebuild master index
    print("\n[4/4] Rebuilding master index...")
    storage = VideoStorage(base_path)
    storage.rebuild_index()
    
    # Print summary
    stats = storage.get_stats()
    print("\n" + "=" * 50)
    print("Migration Complete!")
    print("=" * 50)
    print(f"Total videos: {stats.get('total_videos', 0)}")
    print(f"Categories: {stats.get('categories_count', 0)}")
    print(f"Studios: {stats.get('studios_count', 0)}")
    print(f"Cast members: {stats.get('cast_count', 0)}")
    print("\nNew structure:")
    print("  database/")
    print("  ├── videos/          # Video JSON files")
    print("  └── indexes/")
    print("      └── master_index.json")


if __name__ == "__main__":
    import sys
    base_path = sys.argv[1] if len(sys.argv) > 1 else "../database"
    migrate_database(base_path)
