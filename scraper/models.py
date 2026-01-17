"""
Data models for the full-site scraper.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ProgressState:
    """Persistent state for resumable extractions."""
    started_at: str
    last_updated: str
    mode: str
    total_discovered: int
    completed_codes: List[str] = field(default_factory=list)
    pending_codes: List[str] = field(default_factory=list)
    current_page: int = 1
    total_pages: int = 0


@dataclass
class FailedVideo:
    """Record of a video that failed extraction."""
    code: str
    url: str
    reason: str
    attempts: int
    last_attempt: str


@dataclass
class ExtractionResult:
    """Result of an extraction run."""
    success: bool
    mode: str
    started_at: str
    completed_at: str
    total_discovered: int
    total_completed: int
    total_skipped: int
    total_failed: int
    failed_codes: List[dict] = field(default_factory=list)
    duration_seconds: float = 0.0
    videos_per_hour: float = 0.0
