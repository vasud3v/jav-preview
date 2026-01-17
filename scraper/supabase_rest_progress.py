"""
Supabase REST API progress tracker for scraper state management.
Uses Supabase REST API instead of direct PostgreSQL connection.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import os
import requests
import json


class SupabaseRestProgressTracker:
    """
    REST API-based progress tracker for scraper state.
    """
    
    def __init__(self, url: str = None, key: str = None):
        """
        Initialize Supabase REST progress tracker.
        
        Args:
            url: Supabase project URL
            key: Supabase API key
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
        self.session_id = None
    
    def create_new_state(self, mode: str, **kwargs) -> str:
        """
        Create a new scraper session.
        
        Args:
            mode: Scraper mode (e.g., 'random', 'date_range')
            **kwargs: Additional session parameters
            
        Returns:
            Session ID or None if failed
        """
        try:
            # Deactivate all existing sessions
            try:
                requests.patch(
                    f"{self.base_url}/scraper_progress",
                    headers={**self.headers, 'Prefer': 'return=minimal'},
                    params={'is_active': 'eq.true'},
                    json={'is_active': False},
                    timeout=10
                )
            except Exception as e:
                print(f"Warning: Could not deactivate existing sessions: {e}")
            
            # Create new session - match actual schema
            session_data = {
                'start_page': kwargs.get('start_page', 1),
                'end_page': kwargs.get('end_page', 999999),
                'current_page': kwargs.get('current_page', 1),
                'total_videos': 0,
                'successful_videos': 0,
                'failed_videos': 0,
                'started_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'is_active': True
            }
            
            response = requests.post(
                f"{self.base_url}/scraper_progress",
                headers=self.headers,
                json=session_data,
                timeout=10
            )
            
            if response.status_code in (200, 201, 206):
                data = response.json()
                if data and len(data) > 0 and 'id' in data[0]:
                    self.session_id = data[0]['id']
                    return self.session_id
                else:
                    print(f"Error: Session created but no ID returned")
                    return None
            else:
                print(f"Error creating session: HTTP {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"Error creating new state: Request timeout")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error creating new state: Network error - {e}")
            return None
        except Exception as e:
            print(f"Error creating new state: {e}")
            return None
    
    def update_progress(self, **kwargs):
        """
        Update current session progress.
        
        Args:
            **kwargs: Fields to update (current_page, successful_videos, failed_videos, etc.)
        """
        if not self.session_id:
            print("Warning: Cannot update progress - no active session")
            return
        
        try:
            update_data = {
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Map to actual schema fields
            if 'current_page' in kwargs:
                update_data['current_page'] = kwargs['current_page']
            if 'videos_scraped' in kwargs:
                update_data['successful_videos'] = kwargs['videos_scraped']
            if 'videos_failed' in kwargs:
                update_data['failed_videos'] = kwargs['videos_failed']
            if 'total_videos' in kwargs:
                update_data['total_videos'] = kwargs['total_videos']
            
            response = requests.patch(
                f"{self.base_url}/scraper_progress",
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params={'id': f'eq.{self.session_id}'},
                json=update_data,
                timeout=10
            )
            
            if response.status_code not in (200, 204, 206):
                print(f"Warning: Failed to update progress: HTTP {response.status_code}")
        except requests.exceptions.Timeout:
            print(f"Warning: Progress update timeout (non-critical)")
        except requests.exceptions.RequestException as e:
            print(f"Warning: Progress update network error (non-critical): {e}")
        except Exception as e:
            print(f"Error updating progress: {e}")
    
    def mark_complete(self, success: bool = True):
        """
        Mark current session as complete.
        
        Args:
            success: Whether session completed successfully
        """
        if not self.session_id:
            return
        
        try:
            requests.patch(
                f"{self.base_url}/scraper_progress",
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params={'id': f'eq.{self.session_id}'},
                json={
                    'is_active': False,
                    'updated_at': datetime.now(timezone.utc).isoformat()
                },
                timeout=10
            )
        except Exception as e:
            print(f"Error marking complete: {e}")
    
    def get_last_state(self, mode: str = None) -> Optional[Dict]:
        """
        Get the last session state.
        
        Args:
            mode: Filter by mode (optional)
            
        Returns:
            Session data dict or None
        """
        try:
            params = {'order': 'started_at.desc', 'limit': 1}
            if mode:
                params['mode'] = f'eq.{mode}'
            
            response = requests.get(
                f"{self.base_url}/scraper_progress",
                headers=self.headers,
                params=params,
                timeout=10
            )
            
            if response.status_code in (200, 206):
                data = response.json()
                if data:
                    state = data[0]
                    if state.get('state_data'):
                        try:
                            state['state_data'] = json.loads(state['state_data'])
                        except:
                            pass
                    return state
            return None
        except Exception as e:
            print(f"Error getting last state: {e}")
            return None
    
    def record_failed(self, code: str, error: str, page: int = None):
        """
        Record a failed video scrape.
        
        Args:
            code: Video code that failed
            error: Error message
            page: Page number (optional - not stored in current schema)
        """
        if not code:
            return
        
        try:
            # Check if already exists to increment attempt_count
            existing_response = requests.get(
                f"{self.base_url}/scraper_failed",
                headers=self.headers,
                params={'code': f'eq.{code}', 'select': 'attempt_count'},
                timeout=10
            )
            
            attempt_count = 1
            if existing_response.status_code in (200, 206):
                data = existing_response.json()
                if data:
                    attempt_count = data[0].get('attempt_count', 0) + 1
            
            failed_data = {
                'code': code,
                'error_message': str(error)[:500] if error else 'Unknown error',
                'last_attempt': datetime.now(timezone.utc).isoformat(),
                'attempt_count': attempt_count
            }
            
            # Use upsert to avoid duplicates
            upsert_headers = {
                **self.headers,
                'Prefer': 'resolution=merge-duplicates,return=minimal'
            }
            
            response = requests.post(
                f"{self.base_url}/scraper_failed",
                headers=upsert_headers,
                json=failed_data,
                timeout=10
            )
            
            if response.status_code not in (200, 201, 204, 206):
                print(f"Warning: Failed to record failure for {code}: HTTP {response.status_code}")
        except requests.exceptions.Timeout:
            print(f"Warning: Timeout recording failed video {code} (non-critical)")
        except requests.exceptions.RequestException as e:
            print(f"Warning: Network error recording failed video {code} (non-critical): {e}")
        except Exception as e:
            print(f"Error recording failed video: {e}")
    
    def get_failed(self) -> List[Dict]:
        """
        Get list of failed video records.
        
        Returns:
            List of dicts with code, url, reason, attempts, last_attempt
        """
        try:
            failed = []
            offset = 0
            limit = 1000
            
            while True:
                response = requests.get(
                    f"{self.base_url}/scraper_failed",
                    headers=self.headers,
                    params={'select': '*', 'limit': limit, 'offset': offset, 'order': 'last_attempt.desc'},
                    timeout=30
                )
                
                if response.status_code not in (200, 206):
                    break
                
                batch = response.json()
                if not batch:
                    break
                
                # Convert to expected format
                for record in batch:
                    failed.append({
                        'code': record.get('code', ''),
                        'url': f"https://javtrailers.com/video/{record.get('code', '')}",
                        'reason': record.get('error_message', 'Unknown error'),
                        'attempts': record.get('attempt_count', 1),
                        'last_attempt': record.get('last_attempt', '')
                    })
                
                offset += limit
                
                if len(batch) < limit:
                    break
            
            return failed
        except requests.exceptions.Timeout:
            print(f"Error getting failed codes: Request timeout")
            return []
        except requests.exceptions.RequestException as e:
            print(f"Error getting failed codes: Network error - {e}")
            return []
        except Exception as e:
            print(f"Error getting failed codes: {e}")
            return []
    
    def clear_failed(self, code: str = None):
        """
        Clear failed records.
        
        Args:
            code: Specific code to clear, or None to clear all
        """
        try:
            params = {}
            if code:
                params['code'] = f'eq.{code}'
            
            requests.delete(
                f"{self.base_url}/scraper_failed",
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params=params,
                timeout=10
            )
        except Exception as e:
            print(f"Error clearing failed records: {e}")
    
    def close(self):
        """Close connection (no-op for REST API)."""
        pass
    
    # Compatibility methods for scraper controller
    def load_state(self) -> Optional[Dict]:
        """Load the last active state (alias for get_last_state)."""
        return self.get_last_state()
    
    def save_state(self, state: Any):
        """Save state (no-op - state is auto-saved via update_progress)."""
        pass
    
    def set_pending(self, codes: List[str]):
        """Set pending codes (no-op - not needed for REST API tracking)."""
        pass
    
    def mark_completed(self, code: str):
        """Mark a code as completed (no-op - tracked via videos_scraped counter)."""
        pass
    
    def get_stats(self) -> Dict:
        """Get progress statistics."""
        return {
            'completed': 0,
            'pending': 0,
            'total': 0,
            'percent': 0.0
        }
