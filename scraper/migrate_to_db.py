"""
Migration tool to move JSON video data to SQLite database.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

from .database_storage import DatabaseStorage


@dataclass
class MigrationResult:
    """Result of a migration run."""
    total_files: int
    migrated: int
    skipped: int
    failed: int
    errors: List[str]


class MigrationTool:
    """Migrates JSON video files to database storage."""
    
    def __init__(self, storage: DatabaseStorage, json_dir: str = "database/videos"):
        """
        Initialize migration tool.
        
        Args:
            storage: DatabaseStorage instance to migrate to
            json_dir: Directory containing JSON video files
        """
        self.storage = storage
        self.json_dir = Path(json_dir)
    
    def get_json_files(self) -> List[Path]:
        """Get list of JSON files to migrate."""
        if not self.json_dir.exists():
            return []
        return list(self.json_dir.glob("*.json"))
    
    def migrate(self, progress_callback: Callable[[int, int, str], None] = None) -> MigrationResult:
        """
        Migrate all JSON files to database.
        
        Args:
            progress_callback: Optional callback(processed, total, current_file)
            
        Returns:
            MigrationResult with counts and any errors
        """
        json_files = self.get_json_files()
        total = len(json_files)
        
        result = MigrationResult(
            total_files=total,
            migrated=0,
            skipped=0,
            failed=0,
            errors=[]
        )
        
        if total == 0:
            print("No JSON files found to migrate")
            return result
        
        print(f"Found {total} JSON files to migrate")

        
        for i, json_file in enumerate(json_files):
            filename = json_file.name
            
            if progress_callback:
                progress_callback(i + 1, total, filename)
            
            try:
                # Read JSON file
                with open(json_file, 'r', encoding='utf-8') as f:
                    video_data = json.load(f)
                
                code = video_data.get('code', '')
                if not code:
                    result.failed += 1
                    result.errors.append(f"{filename}: Missing code")
                    continue
                
                # Check if already exists
                if self.storage.video_exists(code):
                    result.skipped += 1
                    continue
                
                # Save to database
                if self.storage.save_video(video_data):
                    result.migrated += 1
                else:
                    result.failed += 1
                    result.errors.append(f"{filename}: Save failed")
                    
            except json.JSONDecodeError as e:
                result.failed += 1
                result.errors.append(f"{filename}: Invalid JSON - {e}")
            except Exception as e:
                result.failed += 1
                result.errors.append(f"{filename}: {e}")
        
        print(f"\nMigration complete:")
        print(f"  Total files: {result.total_files}")
        print(f"  Migrated: {result.migrated}")
        print(f"  Skipped (already exist): {result.skipped}")
        print(f"  Failed: {result.failed}")
        
        if result.errors:
            print(f"\nErrors ({len(result.errors)}):")
            for error in result.errors[:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(result.errors) > 10:
                print(f"  ... and {len(result.errors) - 10} more")
        
        return result


def main():
    """Run migration from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate JSON video files to SQLite database')
    parser.add_argument('--json-dir', default='database/videos', help='Directory with JSON files')
    parser.add_argument('--db-path', default='database/videos.db', help='SQLite database path')
    args = parser.parse_args()
    
    print(f"Migrating from {args.json_dir} to {args.db_path}")
    
    storage = DatabaseStorage(database_path=args.db_path)
    tool = MigrationTool(storage, json_dir=args.json_dir)
    
    def progress(current, total, filename):
        if current % 100 == 0 or current == total:
            print(f"Progress: {current}/{total} ({100*current//total}%)")
    
    result = tool.migrate(progress_callback=progress)
    
    storage.close()
    
    return 0 if result.failed == 0 else 1


if __name__ == '__main__':
    exit(main())
