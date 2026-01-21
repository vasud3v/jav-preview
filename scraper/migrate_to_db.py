"""
Migration tool to move JSON video data to SQLite database.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Dict, Any

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
    
    def __init__(self, storage: DatabaseStorage, json_dir: str = "database/videos", batch_size: int = 50):
        """
        Initialize migration tool.
        
        Args:
            storage: DatabaseStorage instance to migrate to
            json_dir: Directory containing JSON video files
            batch_size: Number of videos to process in a single transaction
        """
        self.storage = storage
        self.json_dir = Path(json_dir)
        self.batch_size = batch_size
    
    def get_json_files(self) -> List[Path]:
        """Get list of JSON files to migrate."""
        if not self.json_dir.exists():
            return []
        return list(self.json_dir.glob("*.json"))
    
    def _process_batch(self, batch_files: List[Path], result: MigrationResult) -> None:
        """
        Process a batch of JSON files.

        Args:
            batch_files: List of JSON file paths
            result: MigrationResult object to update
        """
        # 1. Read all valid JSONs in the batch
        valid_videos: List[Dict[str, Any]] = []
        file_map: Dict[str, str] = {}  # code -> filename

        for json_file in batch_files:
            filename = json_file.name
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    video_data = json.load(f)

                code = video_data.get('code', '')
                if not code:
                    result.failed += 1
                    result.errors.append(f"{filename}: Missing code")
                    continue

                valid_videos.append(video_data)
                file_map[code] = filename

            except json.JSONDecodeError as e:
                result.failed += 1
                result.errors.append(f"{filename}: Invalid JSON - {e}")
            except Exception as e:
                result.failed += 1
                result.errors.append(f"{filename}: {e}")

        if not valid_videos:
            return

        # 2. Check for existence in batch
        codes = [v['code'] for v in valid_videos]
        existing_status = self.storage.videos_exist_batch(codes)

        videos_to_save: List[Dict[str, Any]] = []

        for video in valid_videos:
            code = video['code']
            if existing_status.get(code, False):
                result.skipped += 1
            else:
                videos_to_save.append(video)

        if not videos_to_save:
            return

        # 3. Save new videos in batch
        success_count, failures = self.storage.save_videos_batch(videos_to_save)

        if success_count > 0:
            result.migrated += success_count
            if failures:
                 # This branch shouldn't happen with current save_videos_batch implementation
                 # (because it's all or nothing), but for future safety:
                 for fail_msg in failures:
                     result.failed += 1
                     result.errors.append(fail_msg)
        else:
            # Batch failed (or empty), fallback to individual save to salvage valid records
            if failures:
                # We have failures, so we should retry individually
                for video in videos_to_save:
                    code = video['code']
                    filename = file_map.get(code, "unknown")
                    if self.storage.save_video(video):
                        result.migrated += 1
                    else:
                        result.failed += 1
                        result.errors.append(f"{filename}: Save failed")

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
        
        # Process in batches
        for i in range(0, total, self.batch_size):
            batch = json_files[i:i+self.batch_size]
            self._process_batch(batch, result)
            
            if progress_callback:
                # Update progress with the last file in the batch
                last_file = batch[-1].name if batch else ""
                current_count = min(i + self.batch_size, total)
                progress_callback(current_count, total, last_file)
        
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
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for database commits')
    args = parser.parse_args()
    
    print(f"Migrating from {args.json_dir} to {args.db_path}")
    
    storage = DatabaseStorage(database_path=args.db_path)
    tool = MigrationTool(storage, json_dir=args.json_dir, batch_size=args.batch_size)
    
    def progress(current, total, filename):
        if current % 100 == 0 or current == total:
            print(f"Progress: {current}/{total} ({100*current//total}%)")
    
    result = tool.migrate(progress_callback=progress)
    
    storage.close()
    
    return 0 if result.failed == 0 else 1


if __name__ == '__main__':
    exit(main())
