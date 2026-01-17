"""
Adaptive rate limiting with jitter and cooldown.
Prevents blocking by adjusting request timing based on success/failure.
"""

import random
import time
from datetime import datetime
from typing import Optional

from config import RateLimitConfig


class RateLimiter:
    """Implements adaptive rate limiting with exponential backoff and jitter."""
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        """
        Initialize rate limiter with configuration.
        
        Args:
            config: RateLimitConfig instance, uses defaults if None
        """
        self.config = config or RateLimitConfig()
        self._current_delay = self.config.initial_delay
        self._consecutive_failures = 0
        self._last_request_time: Optional[float] = None
        self._in_cooldown = False
        self._cooldown_until: Optional[float] = None
    
    def wait(self):
        """Wait for the appropriate delay before next request."""
        # Check if in cooldown
        if self._in_cooldown and self._cooldown_until:
            now = time.time()
            if now < self._cooldown_until:
                remaining = self._cooldown_until - now
                print(f"  In cooldown, waiting {remaining:.1f}s...")
                time.sleep(remaining)
            self._in_cooldown = False
            self._cooldown_until = None
        
        # Calculate delay with jitter
        jitter_range = self._current_delay * self.config.jitter_percent
        jitter = random.uniform(-jitter_range, jitter_range)
        actual_delay = max(self.config.min_delay, self._current_delay + jitter)
        
        # If we made a request recently, account for elapsed time
        if self._last_request_time:
            elapsed = time.time() - self._last_request_time
            remaining_delay = actual_delay - elapsed
            if remaining_delay > 0:
                time.sleep(remaining_delay)
        else:
            time.sleep(actual_delay)
        
        self._last_request_time = time.time()
    
    def record_success(self):
        """Record successful request, potentially decrease delay."""
        self._consecutive_failures = 0
        
        # Gradually decrease delay on success (but not below minimum)
        decrease_factor = 0.9  # 10% decrease
        new_delay = self._current_delay * decrease_factor
        self._current_delay = max(self.config.min_delay, new_delay)
    
    def record_failure(self):
        """Record failed request, increase delay with backoff."""
        self._consecutive_failures += 1
        
        # Exponential backoff
        new_delay = self._current_delay * self.config.backoff_factor
        self._current_delay = min(self.config.max_delay, new_delay)
    
    def should_cooldown(self) -> bool:
        """
        Check if cooldown period should be triggered.
        
        Returns:
            True if consecutive failures exceed threshold
        """
        return self._consecutive_failures >= self.config.cooldown_threshold
    
    def cooldown(self):
        """Enter cooldown period."""
        self._in_cooldown = True
        self._cooldown_until = time.time() + self.config.cooldown_duration
        print(f"Entering cooldown for {self.config.cooldown_duration}s due to {self._consecutive_failures} consecutive failures")
        
        # Reset failure count after cooldown triggered
        self._consecutive_failures = 0
        
        # Keep current delay elevated after cooldown (don't reset to initial)
        # This prevents immediate failures after cooldown ends
        # The delay will naturally decrease on successful requests
    
    def get_current_delay(self) -> float:
        """
        Get current delay value (for reporting).
        
        Returns:
            Current base delay in seconds
        """
        return self._current_delay
    
    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.
        
        Returns:
            Dict with current state info
        """
        return {
            'current_delay': self._current_delay,
            'consecutive_failures': self._consecutive_failures,
            'in_cooldown': self._in_cooldown,
            'min_delay': self.config.min_delay,
            'max_delay': self.config.max_delay
        }
    
    def reset(self):
        """Reset rate limiter to initial state."""
        self._current_delay = self.config.initial_delay
        self._consecutive_failures = 0
        self._last_request_time = None
        self._in_cooldown = False
        self._cooldown_until = None
