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
    
    def _process_batch(self, batch_data: List[dict], batch_filenames: List[str], result: MigrationResult) -> None:
        """
        Process a batch of video data.

        Args:
            batch_data: List of video data dictionaries
            batch_filenames: List of filenames corresponding to the data
            result: MigrationResult object to update
        """
        if not batch_data:
            return

        # Get all codes
        codes = [d.get('code') for d in batch_data]

        # Check existence in batch
        existing_status = self.storage.videos_exist_batch(codes)

        # Filter new videos
        new_videos = []
        new_filenames = []

        for i, video_data in enumerate(batch_data):
            code = video_data.get('code')
            if existing_status.get(code, False):
                result.skipped += 1
            else:
                new_videos.append(video_data)
                new_filenames.append(batch_filenames[i])

        # Save new videos in batch
        if new_videos:
            success_count, errors = self.storage.save_videos_batch(new_videos)
            result.migrated += success_count

            # Handle errors
            if errors:
                # If success_count is 0, it means the entire batch failed (rollback)
                # save_videos_batch returns (0, errors) on failure or (len(videos), []) on success
                # The errors list contains "code: reason" strings

                # We need to map errors back to filenames if possible
                code_to_filename = {v.get('code'): f for v, f in zip(new_videos, new_filenames)}

                for error in errors:
                    # Error format is "code: reason" or "unknown: reason" or "Batch error: reason"
                    parts = error.split(': ', 1)
                    if len(parts) == 2:
                        code_part, reason = parts
                        filename = code_to_filename.get(code_part, "batch")
                        result.errors.append(f"{filename}: {reason}")
                    else:
                        result.errors.append(f"Batch error: {error}")

                if success_count == 0:
                     # All failed
                    result.failed += len(new_videos)

    def migrate(self, progress_callback: Callable[[int, int, str], None] = None, batch_size: int = 50) -> MigrationResult:
        """
        Migrate all JSON files to database.
        
        Args:
            progress_callback: Optional callback(processed, total, current_file)
            batch_size: Number of videos to process in a batch
            
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

        batch_data = []
        batch_filenames = []
        
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
                
                batch_data.append(video_data)
                batch_filenames.append(filename)
                
                if len(batch_data) >= batch_size:
                    self._process_batch(batch_data, batch_filenames, result)
                    batch_data = []
                    batch_filenames = []
                    
            except json.JSONDecodeError as e:
                result.failed += 1
                result.errors.append(f"{filename}: Invalid JSON - {e}")
            except Exception as e:
                result.failed += 1
                result.errors.append(f"{filename}: {e}")
        
        # Process remaining
        if batch_data:
            self._process_batch(batch_data, batch_filenames, result)

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
    
    print(f"Migrating from {args.json_dir} to {args.db_path} (batch size: {args.batch_size})")
    
    storage = DatabaseStorage(database_path=args.db_path)
    tool = MigrationTool(storage, json_dir=args.json_dir)
    
    def progress(current, total, filename):
        if current % 100 == 0 or current == total:
            print(f"Progress: {current}/{total} ({100*current//total}%)")
    
    result = tool.migrate(progress_callback=progress, batch_size=args.batch_size)
    
    storage.close()
    
    return 0 if result.failed == 0 else 1


if __name__ == '__main__':
    exit(main())
