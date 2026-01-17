"""
Storage module v2 for organizing scraped video data.
Clean, professional storage with single source of truth and consolidated indexes.
"""

import json
import re
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

# Cross-platform file locking
try:
    import msvcrt
    WINDOWS = True
except ImportError:
    import fcntl
    WINDOWS = False


@contextmanager
def file_lock(file_handle, exclusive=True):
    """Cross-platform file locking context manager."""
    try:
        if WINDOWS:
            # Windows locking
            if exclusive:
                msvcrt.locking(file_handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                msvcrt.locking(file_handle.fileno(), msvcrt.LK_NBRLCK, 1)
        else:
            # Unix locking
            if exclusive:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
            else:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_SH)
        yield
    finally:
        if WINDOWS:
            try:
                msvcrt.locking(file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            except:
                pass
        else:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)


class VideoStorage:
    """Handles storage of scraped video metadata with clean structure."""
    
    # Required fields for video records
    REQUIRED_FIELDS = [
        'code', 'title', 'duration', 'release_date', 'thumbnail_url',
        'embed_urls', 'categories', 'cast', 'studio', 'series',
        'description', 'scraped_at', 'source_url'
    ]
    
    def __init__(self, base_path: str = "database"):
        """Initialize storage with base path."""
        self.base_path = Path(base_path)
        self._init_structure()
        
    def _init_structure(self):
        """Initialize the clean database folder structure."""
        # Only two directories needed
        dirs = [
            self.base_path / "videos",
            self.base_path / "indexes",
        ]
        
        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        # Initialize master index if it doesn't exist
        self._init_master_index()
        
    def _init_master_index(self):
        """Initialize master index file with empty structure."""
        index_file = self.base_path / "indexes" / "master_index.json"
        if not index_file.exists():
            empty_index = {
                "by_category": {},
                "by_cast": {},
                "by_studio": {},
                "by_date": {},
                "all_codes": [],
                "stats": {
                    "total_videos": 0,
                    "last_updated": None,
                    "categories_count": 0,
                    "studios_count": 0,
                    "cast_count": 0
                }
            }
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(empty_index, f, indent=2, ensure_ascii=False)

                
    def _sanitize_filename(self, name: str) -> str:
        """Sanitize string for use as filename."""
        if not name:
            return "unknown"
        # Remove invalid characters for Windows and Unix
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        # Remove control characters (0-31)
        name = ''.join(c for c in name if ord(c) > 31)
        # Remove trailing dots and spaces (invalid on Windows)
        name = name.rstrip('. ')
        # Normalize to lowercase for consistent lookups
        name = name.upper()  # Keep uppercase for video codes
        return name[:100] if name else "unknown"  # Limit length
    
    def _parse_date_to_year_month(self, date_str: str) -> Optional[str]:
        """
        Convert date string to year-month format.
        Examples: '10 Feb 2026' -> '2026-02', '27 February 2026' -> '2026-02'
        """
        if not date_str:
            return None
            
        # Month name to number mapping
        months = {
            'jan': '01', 'january': '01',
            'feb': '02', 'february': '02',
            'mar': '03', 'march': '03',
            'apr': '04', 'april': '04',
            'may': '05',
            'jun': '06', 'june': '06',
            'jul': '07', 'july': '07',
            'aug': '08', 'august': '08',
            'sep': '09', 'september': '09',
            'oct': '10', 'october': '10',
            'nov': '11', 'november': '11',
            'dec': '12', 'december': '12'
        }
        
        # Try to extract year and month
        date_lower = date_str.lower()
        
        # Find year (4 digits)
        year_match = re.search(r'(\d{4})', date_str)
        if not year_match:
            return None
        year = year_match.group(1)
        
        # Find month name
        for month_name, month_num in months.items():
            if month_name in date_lower:
                return f"{year}-{month_num}"
        
        # Try numeric month format (e.g., 2026-02-10)
        numeric_match = re.search(r'(\d{4})-(\d{2})', date_str)
        if numeric_match:
            return f"{numeric_match.group(1)}-{numeric_match.group(2)}"
            
        return None


    def _normalize_video_data(self, video_data: Any) -> dict:
        """Normalize video data to dict with required fields."""
        # Convert dataclass to dict if needed
        if hasattr(video_data, '__dataclass_fields__'):
            from dataclasses import asdict
            data = asdict(video_data)
        else:
            data = dict(video_data) if video_data else {}
        
        # Remove video_id if present (code is the identifier)
        data.pop('video_id', None)
        
        # Ensure all required fields exist with proper defaults
        defaults = {
            'code': '',
            'title': '',
            'duration': '',
            'release_date': '',
            'thumbnail_url': '',
            'embed_urls': [],
            'categories': [],
            'cast': [],
            'studio': '',
            'series': '',
            'description': '',
            'scraped_at': datetime.now().isoformat(),
            'source_url': ''
        }
        
        for field, default in defaults.items():
            if field not in data or data[field] is None:
                data[field] = default
            # Ensure lists are lists
            if isinstance(default, list) and not isinstance(data[field], list):
                data[field] = []
                
        return data

    def save_video(self, video_data: Any) -> bool:
        """
        Save video metadata to storage and update indexes.
        Returns True on success, False on failure.
        """
        try:
            data = self._normalize_video_data(video_data)
            code = data.get('code', '')
            
            if not code:
                print("Error: Cannot save video without code")
                return False
            
            # Save video file (single source of truth)
            video_file = self.base_path / "videos" / f"{self._sanitize_filename(code)}.json"
            with open(video_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Update master index
            self._update_master_index(data)
            
            return True
            
        except Exception as e:
            print(f"Error saving video {video_data.get('code', 'unknown') if isinstance(video_data, dict) else 'unknown'}: {e}")
            return False


    def _update_master_index(self, data: dict):
        """Update master index with video data using file locking."""
        index_file = self.base_path / "indexes" / "master_index.json"
        lock_file = self.base_path / "indexes" / ".master_index.lock"
        
        # Ensure lock file exists
        lock_file.touch(exist_ok=True)
        
        # Use file locking for thread/process safety
        with open(lock_file, 'r+') as lock_handle:
            try:
                if WINDOWS:
                    msvcrt.locking(lock_handle.fileno(), msvcrt.LK_LOCK, 1)
                else:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                
                # Load existing index
                with open(index_file, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                code = data.get('code', '')
                
                # Update by_category
                for category in data.get('categories', []):
                    if category:
                        if category not in index['by_category']:
                            index['by_category'][category] = []
                        if code not in index['by_category'][category]:
                            index['by_category'][category].append(code)
                
                # Update by_cast
                for cast_member in data.get('cast', []):
                    if cast_member:
                        if cast_member not in index['by_cast']:
                            index['by_cast'][cast_member] = []
                        if code not in index['by_cast'][cast_member]:
                            index['by_cast'][cast_member].append(code)
                
                # Update by_studio
                studio = data.get('studio', '')
                if studio:
                    if studio not in index['by_studio']:
                        index['by_studio'][studio] = []
                    if code not in index['by_studio'][studio]:
                        index['by_studio'][studio].append(code)
                
                # Update by_date
                year_month = self._parse_date_to_year_month(data.get('release_date', ''))
                if year_month:
                    if year_month not in index['by_date']:
                        index['by_date'][year_month] = []
                    if code not in index['by_date'][year_month]:
                        index['by_date'][year_month].append(code)
                
                # Update all_codes
                if code not in index['all_codes']:
                    index['all_codes'].append(code)
                
                # Update stats
                index['stats']['total_videos'] = len(index['all_codes'])
                index['stats']['last_updated'] = datetime.now().isoformat()
                index['stats']['categories_count'] = len(index['by_category'])
                index['stats']['studios_count'] = len(index['by_studio'])
                index['stats']['cast_count'] = len(index['by_cast'])
                
                # Write back atomically using temp file
                temp_file = self.base_path / "indexes" / "master_index.tmp.json"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(index, f, indent=2, ensure_ascii=False)
                
                # Atomic rename (with Windows workaround)
                if WINDOWS and index_file.exists():
                    index_file.unlink()
                temp_file.rename(index_file)
                
            finally:
                if WINDOWS:
                    try:
                        msvcrt.locking(lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
                    except:
                        pass
                else:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


    def get_video(self, code: str) -> Optional[dict]:
        """Retrieve video by code, returns None if not found or corrupted."""
        if not code:
            return None
        video_file = self.base_path / "videos" / f"{self._sanitize_filename(code)}.json"
        if video_file.exists():
            try:
                with open(video_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error reading video file {video_file}: {e}")
                return None
        return None
    
    def video_exists(self, code: str) -> bool:
        """Check if video exists without loading full record."""
        if not code:
            return False
        video_file = self.base_path / "videos" / f"{self._sanitize_filename(code)}.json"
        return video_file.exists()


    def _load_master_index(self) -> dict:
        """Load master index from file."""
        index_file = self.base_path / "indexes" / "master_index.json"
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading master index: {e}. Rebuilding...")
            self._init_master_index()
            with open(index_file, 'r', encoding='utf-8') as f:
                return json.load(f)

    def query_by_category(self, category: str) -> List[str]:
        """Get video codes for a category."""
        index = self._load_master_index()
        return index.get('by_category', {}).get(category, [])
    
    def query_by_cast(self, cast_member: str) -> List[str]:
        """Get video codes for a cast member."""
        index = self._load_master_index()
        return index.get('by_cast', {}).get(cast_member, [])
    
    def query_by_studio(self, studio: str) -> List[str]:
        """Get video codes for a studio."""
        index = self._load_master_index()
        return index.get('by_studio', {}).get(studio, [])
    
    def query_by_date(self, year_month: str) -> List[str]:
        """Get video codes for a year-month (e.g., '2026-02')."""
        index = self._load_master_index()
        return index.get('by_date', {}).get(year_month, [])


    def get_all_codes(self) -> List[str]:
        """Get list of all video codes."""
        index = self._load_master_index()
        return index.get('all_codes', [])
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        index = self._load_master_index()
        return index.get('stats', {})


    def rebuild_index(self) -> bool:
        """Rebuild master index from video files."""
        try:
            # Start with empty index
            index = {
                "by_category": {},
                "by_cast": {},
                "by_studio": {},
                "by_date": {},
                "all_codes": [],
                "stats": {
                    "total_videos": 0,
                    "last_updated": None,
                    "categories_count": 0,
                    "studios_count": 0,
                    "cast_count": 0
                }
            }
            
            videos_dir = self.base_path / "videos"
            video_files = list(videos_dir.glob("*.json"))
            
            for video_file in video_files:
                try:
                    with open(video_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    code = data.get('code', '')
                    if not code:
                        continue
                    
                    # Add to all_codes
                    if code not in index['all_codes']:
                        index['all_codes'].append(code)
                    
                    # Index by category
                    for category in data.get('categories', []):
                        if category:
                            if category not in index['by_category']:
                                index['by_category'][category] = []
                            if code not in index['by_category'][category]:
                                index['by_category'][category].append(code)
                    
                    # Index by cast
                    for cast_member in data.get('cast', []):
                        if cast_member:
                            if cast_member not in index['by_cast']:
                                index['by_cast'][cast_member] = []
                            if code not in index['by_cast'][cast_member]:
                                index['by_cast'][cast_member].append(code)
                    
                    # Index by studio
                    studio = data.get('studio', '')
                    if studio:
                        if studio not in index['by_studio']:
                            index['by_studio'][studio] = []
                        if code not in index['by_studio'][studio]:
                            index['by_studio'][studio].append(code)
                    
                    # Index by date
                    year_month = self._parse_date_to_year_month(data.get('release_date', ''))
                    if year_month:
                        if year_month not in index['by_date']:
                            index['by_date'][year_month] = []
                        if code not in index['by_date'][year_month]:
                            index['by_date'][year_month].append(code)
                            
                except Exception as e:
                    print(f"Error reading {video_file}: {e}")
                    continue
            
            # Update stats
            index['stats']['total_videos'] = len(index['all_codes'])
            index['stats']['last_updated'] = datetime.now().isoformat()
            index['stats']['categories_count'] = len(index['by_category'])
            index['stats']['studios_count'] = len(index['by_studio'])
            index['stats']['cast_count'] = len(index['by_cast'])
            
            # Write index
            index_file = self.base_path / "indexes" / "master_index.json"
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(index, f, indent=2, ensure_ascii=False)
            
            print(f"Rebuilt index with {len(index['all_codes'])} videos")
            return True
            
        except Exception as e:
            print(f"Error rebuilding index: {e}")
            return False
