"""
Configuration dataclasses for the full-site scraper.
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting behavior."""
    min_delay: float = 2.0
    max_delay: float = 30.0
    initial_delay: float = 3.0
    backoff_factor: float = 1.5
    jitter_percent: float = 0.2
    cooldown_threshold: int = 5
    cooldown_duration: float = 300.0


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay: float = 5.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0


@dataclass
class ScraperConfig:
    """Main configuration for the scraper system."""
    # Browser settings
    headless: bool = False
    save_debug: bool = False
    
    # Rate limiting
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    
    # Retry settings
    retry: RetryConfig = field(default_factory=RetryConfig)
    
    # Health monitoring
    max_session_failures: int = 5
    session_failure_window: float = 600.0
    
    # Mode-specific
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    specific_codes: Optional[List[str]] = None
