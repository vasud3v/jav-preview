"""
Supabase REST API storage module for video metadata.
Uses Supabase REST API instead of direct PostgreSQL connection.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
import os
import requests
from urllib.parse import quote


class SupabaseRestStorage:
    """
    Supabase REST API-backed storage for video metadata.
    Uses HTTPS REST API instead of PostgreSQL connection.
    """
    
    def __init__(self, url: str = None, key: str = None):
        """
        Initialize Supabase REST storage.
        
        Args:
            url: Supabase project URL (e.g., https://xxx.supabase.co)
            key: Supabase API key (anon or service role key)
        """
        self.url = (url or os.getenv('SUPABASE_URL', '')).rstrip('/')
        self.key = key or os.getenv('SUPABASE_KEY', '')
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        
        self.base_url = f"{self.url}/rest/v1"
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO format string."""
        if not date_str:
            return None
        
        # Try ISO format first
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.isoformat()
        except (ValueError, AttributeError):
            pass
        
        # Try common formats
        formats = [
            '%Y-%m-%d',
            '%d %b %Y',
            '%d %B %Y',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.isoformat()
            except (ValueError, AttributeError):
                continue
        
        return None
    
    def close(self):
        """Close connection (no-op for REST API)."""
        pass
    
    def _get_or_create_category(self, name: str) -> Optional[int]:
        """Get or create a category and return its ID."""
        if not name:
            return None
        
        try:
            # Check if category exists
            response = requests.get(
                f"{self.base_url}/categories",
                headers=self.headers,
                params={'name': f'eq.{name}', 'select': 'id'},
                timeout=10
            )
            
            if response.status_code in (200, 206):
                data = response.json()
                if data:
                    return data[0]['id']
            
            # Create new category
            upsert_headers = {
                **self.headers,
                'Prefer': 'resolution=merge-duplicates,return=representation'
            }
            response = requests.post(
                f"{self.base_url}/categories",
                headers=upsert_headers,
                json={'name': name},
                timeout=10
            )
            
            if response.status_code in (200, 201, 206):
                data = response.json()
                if data:
                    return data[0]['id']
            
            return None
        except Exception as e:
            print(f"Error getting/creating category {name}: {e}")
            return None
    
    def _get_or_create_cast(self, name: str) -> Optional[int]:
        """Get or create a cast member and return its ID."""
        if not name:
            return None
        
        try:
            # Check if cast member exists
            response = requests.get(
                f"{self.base_url}/cast_members",
                headers=self.headers,
                params={'name': f'eq.{name}', 'select': 'id'},
                timeout=10
            )
            
            if response.status_code in (200, 206):
                data = response.json()
                if data:
                    return data[0]['id']
            
            # Create new cast member
            upsert_headers = {
                **self.headers,
                'Prefer': 'resolution=merge-duplicates,return=representation'
            }
            response = requests.post(
                f"{self.base_url}/cast_members",
                headers=upsert_headers,
                json={'name': name},
                timeout=10
            )
            
            if response.status_code in (200, 201, 206):
                data = response.json()
                if data:
                    return data[0]['id']
            
            return None
        except Exception as e:
            print(f"Error getting/creating cast member {name}: {e}")
            return None
    
    def _save_categories(self, video_code: str, categories: List[str]):
        """Save video categories to junction table."""
        if not video_code or not categories:
            return
        
        try:
            # First, delete existing associations
            requests.delete(
                f"{self.base_url}/video_categories",
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params={'video_code': f'eq.{video_code}'},
                timeout=10
            )
            
            # Then add new associations
            for category_name in categories:
                if not category_name:
                    continue
                
                category_id = self._get_or_create_category(category_name)
                if category_id:
                    upsert_headers = {
                        **self.headers,
                        'Prefer': 'resolution=merge-duplicates,return=minimal'
                    }
                    requests.post(
                        f"{self.base_url}/video_categories",
                        headers=upsert_headers,
                        json={
                            'video_code': video_code,
                            'category_id': category_id
                        },
                        timeout=10
                    )
        except Exception as e:
            print(f"Warning: Error saving categories for {video_code}: {e}")
    
    def _save_cast(self, video_code: str, cast_list: List[str]):
        """Save video cast to junction table."""
        if not video_code or not cast_list:
            return
        
        try:
            # First, delete existing associations
            requests.delete(
                f"{self.base_url}/video_cast",
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params={'video_code': f'eq.{video_code}'},
                timeout=10
            )
            
            # Then add new associations
            for cast_name in cast_list:
                if not cast_name:
                    continue
                
                cast_id = self._get_or_create_cast(cast_name)
                if cast_id:
                    upsert_headers = {
                        **self.headers,
                        'Prefer': 'resolution=merge-duplicates,return=minimal'
                    }
                    requests.post(
                        f"{self.base_url}/video_cast",
                        headers=upsert_headers,
                        json={
                            'video_code': video_code,
                            'cast_id': cast_id
                        },
                        timeout=10
                    )
        except Exception as e:
            print(f"Warning: Error saving cast for {video_code}: {e}")
    
    def save_video(self, video_data: Any) -> bool:
        """
        Save or update a video record via REST API.
        
        Args:
            video_data: Dict or dataclass with video metadata
            
        Returns:
            True on success, False on failure
        """
        try:
            # Normalize input to dict
            if hasattr(video_data, '__dataclass_fields__'):
                from dataclasses import asdict
                data = asdict(video_data)
            else:
                data = dict(video_data) if video_data else {}
            
            # Validate required fields
            code = (data.get('code') or '').strip()
            title = (data.get('title') or '').strip()
            
            if not code:
                print("Error: Cannot save video without code")
                return False
            
            if not title:
                print(f"Error: Cannot save video {code} without title")
                return False
            
            # Validate title
            if '<' in title or '>' in title or 'clip-path' in title or 'fill=' in title:
                print(f"Error: Cannot save video {code} - title contains invalid markup")
                return False
            
            # Prepare video record (without categories and cast - those are in junction tables)
            video_record = {
                'code': code,
                'title': title,
                'content_id': data.get('content_id') or '',
                'duration': data.get('duration') or '',
                'release_date': self._parse_date(data.get('release_date') or ''),
                'thumbnail_url': data.get('thumbnail_url') or '',
                'cover_url': data.get('cover_url') or '',
                'studio': data.get('studio') or '',
                'series': data.get('series') or '',
                'description': data.get('description') or '',
                'scraped_at': self._parse_date(data.get('scraped_at') or '') or datetime.utcnow().isoformat(),
                'source_url': data.get('source_url') or '',
                'embed_urls': data.get('embed_urls') or [],
                'gallery_images': data.get('gallery_images') or [],
                'cast_images': data.get('cast_images') or {}
            }
            
            # Extract categories and cast for separate handling
            categories = data.get('categories') or []
            cast_list = data.get('cast') or []
            
            # Upsert video (insert or update) - use resolution=merge-duplicates for upsert
            upsert_headers = {
                **self.headers,
                'Prefer': 'resolution=merge-duplicates,return=representation'
            }
            
            response = requests.post(
                f"{self.base_url}/videos",
                headers=upsert_headers,
                json=video_record,
                timeout=30
            )
            
            if response.status_code in (200, 201, 206):
                # Video saved successfully, now handle categories and cast
                self._save_categories(code, categories)
                self._save_cast(code, cast_list)
                return True
            elif response.status_code == 409:
                # Conflict - try update instead
                response = requests.patch(
                    f"{self.base_url}/videos",
                    headers=self.headers,
                    params={'code': f'eq.{code}'},
                    json=video_record,
                    timeout=30
                )
                if response.status_code in (200, 204, 206):
                    self._save_categories(code, categories)
                    self._save_cast(code, cast_list)
                    return True
                return False
            else:
                print(f"Error saving video {code}: HTTP {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return False
                
        except requests.exceptions.Timeout:
            print(f"Error saving video: Request timeout")
            return False
        except requests.exceptions.RequestException as e:
            print(f"Error saving video: Network error - {e}")
            return False
        except Exception as e:
            print(f"Error saving video: {e}")
            return False
    
    def get_video(self, code: str) -> Optional[dict]:
        """
        Retrieve video by code via REST API.
        
        Args:
            code: Video code to retrieve
            
        Returns:
            Video data as dict, or None if not found
        """
        if not code:
            return None
        
        try:
            response = requests.get(
                f"{self.base_url}/videos",
                headers=self.headers,
                params={'code': f'eq.{code}', 'limit': 1},
                timeout=10
            )
            
            if response.status_code in (200, 206):
                data = response.json()
                return data[0] if data else None
            return None
        except requests.exceptions.Timeout:
            print(f"Error retrieving video {code}: Request timeout")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving video {code}: Network error - {e}")
            return None
        except Exception as e:
            print(f"Error retrieving video {code}: {e}")
            return None
    
    def video_exists(self, code: str) -> bool:
        """
        Check if video exists via REST API.
        
        Args:
            code: Video code to check
            
        Returns:
            True if video exists, False otherwise
        """
        if not code:
            return False
        
        try:
            headers = {**self.headers, 'Prefer': 'count=exact'}
            response = requests.get(
                f"{self.base_url}/videos",
                headers=headers,
                params={'code': f'eq.{code}', 'select': 'code', 'limit': 0},
                timeout=10
            )
            
            if response.status_code in (200, 206):
                content_range = response.headers.get('Content-Range', '0-0/0')
                count = content_range.split('/')[-1] if '/' in content_range else '0'
                try:
                    return int(count) > 0
                except (ValueError, TypeError):
                    return False
            return False
        except requests.exceptions.Timeout:
            print(f"Error checking video existence {code}: Request timeout")
            return False
        except requests.exceptions.RequestException as e:
            print(f"Error checking video existence {code}: Network error - {e}")
            return False
        except Exception as e:
            print(f"Error checking video existence {code}: {e}")
            return False
    
    def get_all_codes(self) -> List[str]:
        """
        Get list of all video codes via REST API.
        
        Returns:
            List of video codes
        """
        try:
            codes = []
            offset = 0
            limit = 1000
            max_retries = 3
            
            while True:
                retry_count = 0
                success = False
                batch = []
                
                while retry_count < max_retries and not success:
                    try:
                        response = requests.get(
                            f"{self.base_url}/videos",
                            headers=self.headers,
                            params={'select': 'code', 'limit': limit, 'offset': offset, 'order': 'code'},
                            timeout=30
                        )
                        
                        if response.status_code not in (200, 206):
                            retry_count += 1
                            continue
                        
                        batch = response.json()
                        success = True
                    except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            print(f"Error getting codes at offset {offset}: {e}")
                            return codes
                
                if not batch:
                    break
                
                codes.extend([v['code'] for v in batch if 'code' in v])
                offset += limit
                
                if len(batch) < limit:
                    break
            
            return codes
        except Exception as e:
            print(f"Error getting all codes: {e}")
            return []
    
    def get_stats(self) -> dict:
        """
        Get database statistics via REST API.
        
        Returns:
            Dict with total_videos and last_updated
        """
        try:
            headers = {**self.headers, 'Prefer': 'count=exact'}
            response = requests.get(
                f"{self.base_url}/videos",
                headers=headers,
                params={'select': 'code', 'limit': 0},
                timeout=10
            )
            
            if response.status_code in (200, 206):
                content_range = response.headers.get('Content-Range', '0-0/0')
                total = content_range.split('/')[-1] if '/' in content_range else '0'
                
                try:
                    total_int = int(total)
                except (ValueError, TypeError):
                    total_int = 0
                
                return {
                    'total_videos': total_int,
                    'last_updated': datetime.utcnow().isoformat()
                }
            return {'total_videos': 0, 'last_updated': datetime.utcnow().isoformat()}
        except requests.exceptions.Timeout:
            print(f"Error getting stats: Request timeout")
            return {'total_videos': 0, 'last_updated': datetime.utcnow().isoformat()}
        except requests.exceptions.RequestException as e:
            print(f"Error getting stats: Network error - {e}")
            return {'total_videos': 0, 'last_updated': datetime.utcnow().isoformat()}
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {'total_videos': 0, 'last_updated': datetime.utcnow().isoformat()}
    
    def videos_exist_batch(self, codes: List[str]) -> Dict[str, bool]:
        """
        Check existence of multiple video codes efficiently via REST API.
        
        Args:
            codes: List of video codes to check
            
        Returns:
            Dict mapping code to existence (True/False)
        """
        if not codes:
            return {}
        
        try:
            # Query in batches
            batch_size = 100
            result = {}
            
            for i in range(0, len(codes), batch_size):
                batch = codes[i:i + batch_size]
                # Properly quote codes to handle special characters
                codes_filter = ','.join(f'"{code}"' for code in batch)
                
                response = requests.get(
                    f"{self.base_url}/videos",
                    headers=self.headers,
                    params={'code': f'in.({codes_filter})', 'select': 'code'},
                    timeout=30
                )
                
                if response.status_code in (200, 206):
                    existing = {v['code'] for v in response.json()}
                    for code in batch:
                        result[code] = code in existing
                else:
                    # On error, mark all as not existing to be safe
                    for code in batch:
                        result[code] = False
            
            return result
        except requests.exceptions.Timeout:
            print(f"Error checking batch existence: Request timeout")
            return {code: False for code in codes}
        except requests.exceptions.RequestException as e:
            print(f"Error checking batch existence: Network error - {e}")
            return {code: False for code in codes}
        except Exception as e:
            print(f"Error checking batch existence: {e}")
            return {code: False for code in codes}
