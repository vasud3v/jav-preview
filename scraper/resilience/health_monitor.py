"""
Health monitoring for browser session management.
Detects failures and triggers recovery when needed.
"""

import time
from datetime import datetime
from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from javtrailers_scraper import JavTrailersScraper


class HealthMonitor:
    """Monitors browser session health and triggers recovery."""
    
    def __init__(
        self,
        scraper: "JavTrailersScraper",
        max_failures: int = 5,
        failure_window: float = 600.0
    ):
        """
        Initialize monitor with scraper reference.
        
        Args:
            scraper: JavTrailersScraper instance to monitor
            max_failures: Max failures within window before pause
            failure_window: Time window in seconds for counting failures
        """
        self.scraper = scraper
        self.max_failures = max_failures
        self.failure_window = failure_window
        self._failure_times: List[float] = []
        self._recovery_count = 0
        self._last_health_check: Optional[float] = None
    
    def check_health(self) -> bool:
        """
        Check if browser session is healthy.
        
        Returns:
            True if session is responsive, False otherwise
        """
        self._last_health_check = time.time()
        
        if self.scraper.driver is None:
            return False
        
        try:
            # Try to get current URL - will fail if browser is unresponsive
            _ = self.scraper.driver.current_url
            return True
        except Exception as e:
            print(f"Health check failed: {e}")
            return False
    
    def record_failure(self):
        """Record a session failure event."""
        now = time.time()
        self._failure_times.append(now)
        
        # Clean up old failures outside the window
        cutoff = now - self.failure_window
        self._failure_times = [t for t in self._failure_times if t > cutoff]
        
        print(f"Session failure recorded ({len(self._failure_times)} in window)")
    
    def should_pause(self) -> bool:
        """
        Check if too many failures occurred, requiring pause.
        
        Returns:
            True if failures exceed threshold within window
        """
        now = time.time()
        cutoff = now - self.failure_window
        recent_failures = [t for t in self._failure_times if t > cutoff]
        return len(recent_failures) >= self.max_failures
    
    def recover(self) -> bool:
        """
        Attempt to recover the session (restart browser).
        
        Returns:
            True if recovery successful, False otherwise
        """
        self._recovery_count += 1
        print(f"Attempting session recovery (attempt #{self._recovery_count})...")
        
        try:
            # Close existing driver
            self.scraper._close_driver()
            
            # Wait a moment before restarting
            time.sleep(2)
            
            # Reinitialize driver (this will pass Cloudflare)
            self.scraper._init_driver()
            
            # Verify recovery worked
            if self.check_health():
                print("Session recovery successful")
                return True
            else:
                print("Session recovery failed - browser not responsive")
                return False
                
        except Exception as e:
            print(f"Session recovery failed: {e}")
            return False
    
    def get_failure_count(self) -> int:
        """
        Get recent failure count within window.
        
        Returns:
            Number of failures in current window
        """
        now = time.time()
        cutoff = now - self.failure_window
        return len([t for t in self._failure_times if t > cutoff])
    
    def get_stats(self) -> dict:
        """
        Get health monitor statistics.
        
        Returns:
            Dict with monitor state info
        """
        return {
            'recent_failures': self.get_failure_count(),
            'total_recoveries': self._recovery_count,
            'max_failures': self.max_failures,
            'failure_window': self.failure_window,
            'should_pause': self.should_pause()
        }
    
    def reset(self):
        """Reset failure tracking."""
        self._failure_times = []
        print("Health monitor reset")
