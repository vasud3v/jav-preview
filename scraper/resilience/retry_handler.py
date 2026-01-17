"""
Retry handling with exponential backoff.
Manages retries for failed operations and tracks permanent failures in Supabase.
"""

import time
from datetime import datetime
from typing import Callable, Tuple, Any, Optional, List
import os

from config import RetryConfig
from models import FailedVideo


class RetryHandler:
    """Manages retry logic with exponential backoff and failure tracking in Supabase."""
    
    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        failed_file: str = None  # Deprecated, kept for compatibility
    ):
        """
        Initialize retry handler.
        
        Args:
            config: RetryConfig instance, uses defaults if None
            failed_file: Deprecated - failures are stored in Supabase
        """
        self.config = config or RetryConfig()
        self._progress_tracker = None  # Will be set by controller
    
    def set_progress_tracker(self, tracker):
        """Set the progress tracker for failed video storage."""
        self._progress_tracker = tracker
    
    def execute_with_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Tuple[bool, Any]:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Tuple of (success: bool, result: Any)
        """
        last_error = None
        delay = self.config.base_delay
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return True, result
                else:
                    last_error = "Function returned None"
            except Exception as e:
                last_error = str(e)
                print(f"  Attempt {attempt}/{self.config.max_retries} failed: {e}")
            
            # Don't sleep after last attempt
            if attempt < self.config.max_retries:
                sleep_time = min(delay, self.config.max_delay)
                print(f"  Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                delay *= self.config.backoff_factor
        
        return False, last_error
    
    def record_permanent_failure(self, code: str, url: str, reason: str):
        """
        Record a video that failed all retries in Supabase.
        
        Args:
            code: Video code
            url: Video URL
            reason: Failure reason
        """
        if self._progress_tracker:
            self._progress_tracker.record_failed(code, url, reason)
            print(f"Recorded permanent failure for {code}: {reason}")
        else:
            print(f"Warning: Cannot record failure for {code} - no progress tracker set")
    
    def get_failed_codes(self) -> List[dict]:
        """
        Get list of permanently failed videos with reasons from Supabase.
        
        Returns:
            List of dicts with code, url, reason, attempts, last_attempt
        """
        if self._progress_tracker:
            return self._progress_tracker.get_failed()
        return []
    
    def get_failed_urls(self) -> List[str]:
        """
        Get list of failed video URLs for retry mode.
        
        Returns:
            List of URLs
        """
        failed = self.get_failed_codes()
        return [v.get('url', '') for v in failed if v.get('url')]
    
    def clear_failed(self, code: str):
        """
        Remove a code from failed list (after successful retry).
        
        Args:
            code: Video code to remove
        """
        if self._progress_tracker:
            self._progress_tracker.clear_failed(code)
            print(f"Cleared {code} from failed list")
    
    def clear_all_failed(self):
        """Clear all failed videos - not implemented for Supabase."""
        print("Warning: clear_all_failed not implemented for Supabase storage")
    
    def get_stats(self) -> dict:
        """
        Get retry handler statistics.
        
        Returns:
            Dict with handler state info
        """
        failed_count = len(self.get_failed_codes()) if self._progress_tracker else 0
        return {
            'total_failed': failed_count,
            'max_retries': self.config.max_retries,
            'base_delay': self.config.base_delay
        }
