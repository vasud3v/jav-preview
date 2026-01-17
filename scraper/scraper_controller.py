"""
Main orchestrator for the full-site scraper.
Coordinates all components and manages extraction workflow.
Uses Supabase for all storage - no SQLite or JSON files.
"""

import re
import time
from datetime import datetime
from typing import Optional, List

from config import ScraperConfig
from models import ProgressState, ExtractionResult
from javtrailers_scraper import JavTrailersScraper
from storage_factory import create_storage, create_progress_tracker
from resilience.rate_limiter import RateLimiter
from resilience.health_monitor import HealthMonitor
from resilience.retry_handler import RetryHandler
from resilience.content_discovery import ContentDiscovery
from utils import extract_code_from_url, code_to_url


class ScraperController:
    """Main orchestrator that coordinates all scraper components."""
    
    VALID_MODES = ['full', 'incremental', 'retry-failed', 'date-range', 'codes', 'random']
    
    def __init__(self, config: Optional[ScraperConfig] = None):
        """
        Initialize controller with configuration.
        
        Args:
            config: ScraperConfig instance, uses defaults if None
        """
        self.config = config or ScraperConfig()
        self._stopped = False
        self._started_at: Optional[str] = None
        
        # Initialize components
        self.scraper = JavTrailersScraper(
            headless=self.config.headless,
            save_debug=self.config.save_debug
        )
        
        # Use Supabase storage
        self.storage = create_storage()
        print("✓ Connected to Supabase storage")
        
        # Use Supabase progress tracker
        self.progress = create_progress_tracker()
        print("✓ Connected to Supabase progress tracking")
        
        self.rate_limiter = RateLimiter(config=self.config.rate_limit)
        self.health_monitor = HealthMonitor(
            scraper=self.scraper,
            max_failures=self.config.max_session_failures,
            failure_window=self.config.session_failure_window
        )
        self.retry_handler = RetryHandler(config=self.config.retry)
        # Connect retry handler to progress tracker for failed video storage
        self.retry_handler.set_progress_tracker(self.progress)
        
        self.discovery = ContentDiscovery(scraper=self.scraper)
    
    def run(self, mode: str = "full", resume: bool = True) -> ExtractionResult:
        """
        Run extraction in specified mode.
        
        Args:
            mode: "full", "incremental", "retry-failed", "date-range", "codes"
            resume: Whether to resume from saved state
            
        Returns:
            ExtractionResult with statistics and status
        """
        if mode not in self.VALID_MODES:
            raise ValueError(f"Invalid mode: {mode}. Must be one of {self.VALID_MODES}")
        
        self._stopped = False
        self._started_at = datetime.now().isoformat()
        
        print(f"Starting extraction in '{mode}' mode...")
        
        try:
            if mode == "full":
                return self._run_full_extraction(resume)
            elif mode == "incremental":
                return self._run_incremental()
            elif mode == "retry-failed":
                return self._run_retry_failed()
            elif mode == "date-range":
                return self._run_date_range()
            elif mode == "codes":
                return self._run_specific_codes()
            elif mode == "random":
                return self._run_random_extraction()
        except Exception as e:
            print(f"Extraction failed: {e}")
            return self._create_result(
                success=False,
                mode=mode,
                total_discovered=0,
                total_completed=0,
                total_skipped=0,
                total_failed=0
            )
        finally:
            self.scraper.close()
    
    def _run_full_extraction(self, resume: bool) -> ExtractionResult:
        """Run full site extraction with streaming discovery."""
        # Check for existing state
        state = self.progress.load_state() if resume else None
        
        # Handle both dict (from DB tracker) and object (from JSON tracker)
        if state:
            state_mode = state.get('mode') if isinstance(state, dict) else getattr(state, 'mode', None)
            pending_codes = state.get('pending_codes', []) if isinstance(state, dict) else getattr(state, 'pending_codes', [])
            completed_codes = state.get('completed_codes', []) if isinstance(state, dict) else getattr(state, 'completed_codes', [])
            current_page = state.get('current_page', 1) if isinstance(state, dict) else getattr(state, 'current_page', 1)
            total_pages = state.get('total_pages', 0) if isinstance(state, dict) else getattr(state, 'total_pages', 0)
            
            if state_mode == "full":
                if pending_codes:
                    print(f"Resuming from saved state: {len(completed_codes)} completed, {len(pending_codes)} pending")
                    urls_to_scrape = [self._code_to_url(c) for c in pending_codes]
                    return self._scrape_videos(urls_to_scrape, "full")
                elif completed_codes:
                    # Have completed work, resume streaming (will skip already scraped)
                    print(f"Resuming streaming with {len(completed_codes)} already completed")
                    return self._scrape_streaming("full", start_page=max(1, current_page))
        
        # Fresh start - use streaming discovery (scrape as we discover)
        print("Starting streaming extraction (discover + scrape simultaneously)...")
        self.progress.create_new_state("full")
        return self._scrape_streaming("full")
    
    def _run_incremental(self) -> ExtractionResult:
        """Run incremental extraction (new videos only)."""
        print("Running incremental extraction...")
        
        # Get known codes from storage
        known_codes = self.storage.get_all_codes()
        print(f"Database has {len(known_codes)} existing videos")
        
        # Find new videos
        new_urls = self.discovery.get_new_videos(known_codes)
        
        if not new_urls:
            print("No new videos found")
            return self._create_result(
                success=True,
                mode="incremental",
                total_discovered=0,
                total_completed=0,
                total_skipped=0,
                total_failed=0
            )
        
        # Create state for incremental
        state = self.progress.create_new_state("incremental")
        codes = self.discovery.extract_codes_from_urls(new_urls)
        self.progress.set_pending(codes)
        
        return self._scrape_videos(new_urls, "incremental")
    
    def _run_retry_failed(self) -> ExtractionResult:
        """Run retry of previously failed videos."""
        print("Retrying failed videos...")
        
        failed_urls = self.retry_handler.get_failed_urls()
        
        if not failed_urls:
            print("No failed videos to retry")
            return self._create_result(
                success=True,
                mode="retry-failed",
                total_discovered=0,
                total_completed=0,
                total_skipped=0,
                total_failed=0
            )
        
        print(f"Found {len(failed_urls)} failed videos to retry")
        
        # Create state for retry
        state = self.progress.create_new_state("retry-failed")
        codes = self.discovery.extract_codes_from_urls(failed_urls)
        self.progress.set_pending(codes)
        
        return self._scrape_videos(failed_urls, "retry-failed")
    
    def _run_date_range(self) -> ExtractionResult:
        """Run extraction for specific date range."""
        if not self.config.date_range_start:
            raise ValueError("date_range_start must be set for date-range mode")
        
        print(f"Running date-range extraction: {self.config.date_range_start} to {self.config.date_range_end or 'now'}")
        
        # Discover all videos first
        all_urls = self.discovery.get_all_video_urls()
        
        # Filter by date would require scraping metadata first
        # For now, we'll scrape all and filter during processing
        state = self.progress.create_new_state("date-range")
        codes = self.discovery.extract_codes_from_urls(all_urls)
        self.progress.set_pending(codes)
        
        return self._scrape_videos(all_urls, "date-range", filter_by_date=True)
    
    def _run_specific_codes(self) -> ExtractionResult:
        """Run extraction for specific video codes."""
        if not self.config.specific_codes:
            raise ValueError("specific_codes must be set for codes mode")
        
        codes = self.config.specific_codes
        print(f"Running extraction for {len(codes)} specific codes")
        
        urls = [self._code_to_url(c) for c in codes]
        
        state = self.progress.create_new_state("codes")
        self.progress.set_pending(codes)
        
        return self._scrape_videos(urls, "codes")
    
    def _run_random_extraction(self) -> ExtractionResult:
        """Run extraction with random page order (resumable via Supabase)."""
        import random
        
        print("Running random page extraction...")
        
        # Get total pages
        try:
            total_pages = self.discovery.get_total_pages()
        except Exception as e:
            print(f"Failed to get total pages: {e}")
            return self._create_result(
                success=False, mode="random",
                total_discovered=0, total_completed=0,
                total_skipped=0, total_failed=0
            )
        
        if total_pages <= 0:
            print("No pages found to scrape")
            return self._create_result(
                success=True, mode="random",
                total_discovered=0, total_completed=0,
                total_skipped=0, total_failed=0
            )
        
        print(f"Found {total_pages} total pages")
        
        # Create shuffled list of pages
        pages = list(range(1, total_pages + 1))
        random.shuffle(pages)
        
        # Get known codes to skip
        try:
            known_codes = set(self.storage.get_all_codes())
        except Exception as e:
            print(f"Warning: Could not get existing codes: {e}")
            known_codes = set()
        
        print(f"Database has {len(known_codes)} existing videos")
        
        self.progress.create_new_state("random")
        
        return self._scrape_random_pages(pages, known_codes, total_pages)
    
    def _scrape_random_pages(self, pages: list, known_codes: set, total_pages: int) -> ExtractionResult:
        """
        Scrape videos from pages in random order.
        
        Args:
            pages: List of page numbers in random order
            known_codes: Set of already scraped video codes
            total_pages: Total page count for progress display
        """
        completed = 0
        skipped = 0
        failed = 0
        total_discovered = 0
        
        pages_processed = 0
        for page_num in pages:
            if self._stopped:
                break
            
            pages_processed += 1
            print(f"\n[{pages_processed}/{total_pages}] Fetching page {page_num}...")
            
            try:
                urls = self.discovery.get_video_urls_for_page(page_num)
            except Exception as e:
                print(f"  Error fetching page {page_num}: {e}")
                continue
            
            if not urls:
                print(f"  No videos found (page may be empty or beyond range)")
                continue
            
            total_discovered += len(urls)
            print(f"  Found {len(urls)} videos")
            
            videos_on_page = 0
            
            for i, url in enumerate(urls, 1):
                if self._stopped:
                    break
                
                code = self._extract_code_from_url(url)
                
                if not code:
                    print(f"  [{i}/{len(urls)}] Could not extract code from {url[:50]}, skipping")
                    skipped += 1
                    continue
                
                if code in known_codes:
                    skipped += 1
                    continue
                
                try:
                    if self.storage.video_exists(code):
                        known_codes.add(code)
                        skipped += 1
                        continue
                except Exception:
                    pass
                
                if not self.health_monitor.check_health():
                    self.health_monitor.record_failure()
                    if self.health_monitor.should_pause():
                        print("Too many failures, stopping...")
                        return self._create_result(
                            success=False, mode="random",
                            total_discovered=total_discovered,
                            total_completed=completed,
                            total_skipped=skipped,
                            total_failed=failed
                        )
                    if not self.health_monitor.recover():
                        print("Recovery failed, stopping...")
                        return self._create_result(
                            success=False, mode="random",
                            total_discovered=total_discovered,
                            total_completed=completed,
                            total_skipped=skipped,
                            total_failed=failed
                        )
                
                self.rate_limiter.wait()
                
                print(f"  [{i}/{len(urls)}] {code}", end=" ")
                
                try:
                    success, result = self.retry_handler.execute_with_retry(
                        self.scraper.scrape_video_page, url
                    )
                except Exception as e:
                    print(f"✗ Exception: {str(e)[:50]}")
                    failed += 1
                    self.rate_limiter.record_failure()
                    continue
                
                if success and result:
                    try:
                        if self.storage.save_video(result):
                            completed += 1
                            videos_on_page += 1
                            known_codes.add(code)
                            self.rate_limiter.record_success()
                            if code:
                                self.progress.mark_completed(code)
                            print("✓")
                        else:
                            failed += 1
                            self.rate_limiter.record_failure()
                            print("✗ save failed")
                    except Exception as e:
                        failed += 1
                        self.rate_limiter.record_failure()
                        print(f"✗ save error: {str(e)[:30]}")
                else:
                    failed += 1
                    self.rate_limiter.record_failure()
                    error_msg = str(result)[:100] if result else "Unknown error"
                    self.retry_handler.record_permanent_failure(code, url, error_msg)
                    print("✗")
                
                if self.rate_limiter.should_cooldown():
                    self.rate_limiter.cooldown()
            
            print(f"  Page {page_num} done: +{videos_on_page} new, {completed} total saved, {skipped} skipped, {failed} failed")
        
        return self._create_result(
            success=not self._stopped and failed == 0,
            mode="random",
            total_discovered=total_discovered,
            total_completed=completed,
            total_skipped=skipped,
            total_failed=failed
        )
    
    def _scrape_videos(
        self,
        urls: List[str],
        mode: str,
        filter_by_date: bool = False
    ) -> ExtractionResult:
        """
        Core scraping loop with resilience.
        
        Args:
            urls: List of video URLs to scrape
            mode: Current extraction mode
            filter_by_date: Whether to filter by date range
            
        Returns:
            ExtractionResult
        """
        total = len(urls)
        completed = 0
        skipped = 0
        failed = 0
        
        print(f"\nScraping {total} videos...")
        
        for i, url in enumerate(urls, 1):
            if self._stopped:
                print("Extraction stopped by user")
                break
            
            code = self._extract_code_from_url(url)
            
            # Skip if already exists (for non-retry modes)
            if mode != "retry-failed" and code and self.storage.video_exists(code):
                print(f"[{i}/{total}] Skipping {code} (exists)")
                skipped += 1
                self.progress.mark_completed(code)
                continue
            
            # Check health and recover if needed
            if not self.health_monitor.check_health():
                self.health_monitor.record_failure()
                if self.health_monitor.should_pause():
                    print("Too many failures, pausing...")
                    return self._create_result(
                        success=False,
                        mode=mode,
                        total_discovered=total,
                        total_completed=completed,
                        total_skipped=skipped,
                        total_failed=failed
                    )
                if not self.health_monitor.recover():
                    print("Recovery failed, stopping")
                    break
            
            # Rate limit
            self.rate_limiter.wait()
            
            # Scrape with retry
            print(f"[{i}/{total}] Scraping: {code or url}")
            success, result = self.retry_handler.execute_with_retry(
                self.scraper.scrape_video_page, url
            )
            
            if success and result:
                # Date filter check
                if filter_by_date and not self._in_date_range(result.release_date):
                    print(f"  Skipped (outside date range)")
                    skipped += 1
                    continue
                
                # Save to storage
                if self.storage.save_video(result):
                    completed += 1
                    self.rate_limiter.record_success()
                    self.progress.mark_completed(code)
                    
                    # Clear from failed list if it was a retry
                    if mode == "retry-failed":
                        self.retry_handler.clear_failed(code)
                    
                    print(f"  ✓ Saved: {result.code}")
                else:
                    failed += 1
                    self.rate_limiter.record_failure()
                    print(f"  ✗ Failed to save")
            else:
                failed += 1
                self.rate_limiter.record_failure()
                self.retry_handler.record_permanent_failure(
                    code or "unknown",
                    url,
                    str(result) if result else "Unknown error"
                )
                print(f"  ✗ Failed: {result}")
            
            # Check for cooldown
            if self.rate_limiter.should_cooldown():
                self.rate_limiter.cooldown()
            
            # Progress update
            if i % 10 == 0:
                stats = self.progress.get_stats()
                print(f"  Progress: {stats['percent']:.1f}% ({stats['completed']}/{stats['total']})")
        
        return self._create_result(
            success=True,
            mode=mode,
            total_discovered=total,
            total_completed=completed,
            total_skipped=skipped,
            total_failed=failed
        )
    
    def stop(self):
        """Gracefully stop extraction, preserving state."""
        print("\nStopping extraction gracefully...")
        self._stopped = True
        
        # Save current state before closing
        try:
            if hasattr(self, 'progress') and self.progress and hasattr(self.progress, '_state') and self.progress._state:
                print("Saving progress state...")
                self.progress.save_state(self.progress._state)
                print("✓ Progress state saved")
        except Exception as e:
            print(f"⚠️  Warning: Could not save state: {e}")
        
        # Close browser to ensure clean shutdown
        try:
            if hasattr(self, 'scraper') and self.scraper:
                print("Closing browser...")
                self.scraper.close()
                print("✓ Browser closed")
        except Exception as e:
            print(f"⚠️  Error closing browser: {e}")
    
    def get_status(self) -> dict:
        """
        Get current extraction status and statistics.
        
        Returns:
            Dict with status info
        """
        progress_stats = self.progress.get_stats() or {'completed': 0, 'pending': 0, 'total': 0, 'percent': 0.0}
        rate_stats = self.rate_limiter.get_stats()
        health_stats = self.health_monitor.get_stats()
        retry_stats = self.retry_handler.get_stats()
        
        return {
            'progress': progress_stats,
            'rate_limiter': rate_stats,
            'health': health_stats,
            'retry': retry_stats,
            'stopped': self._stopped
        }
    
    def _create_result(
        self,
        success: bool,
        mode: str,
        total_discovered: int,
        total_completed: int,
        total_skipped: int,
        total_failed: int
    ) -> ExtractionResult:
        """Create ExtractionResult with calculated fields."""
        completed_at = datetime.now().isoformat()
        
        # Calculate duration
        duration = 0.0
        if self._started_at:
            start = datetime.fromisoformat(self._started_at)
            end = datetime.fromisoformat(completed_at)
            duration = (end - start).total_seconds()
        
        # Calculate videos per hour
        videos_per_hour = 0.0
        if duration > 0:
            videos_per_hour = total_completed / (duration / 3600)
        
        return ExtractionResult(
            success=success,
            mode=mode,
            started_at=self._started_at or completed_at,
            completed_at=completed_at,
            total_discovered=total_discovered,
            total_completed=total_completed,
            total_skipped=total_skipped,
            total_failed=total_failed,
            failed_codes=self.retry_handler.get_failed_codes(),
            duration_seconds=duration,
            videos_per_hour=videos_per_hour
        )
    
    def _code_to_url(self, code: str) -> str:
        """Convert video code to URL."""
        return code_to_url(code)
    
    def _extract_code_from_url(self, url: str) -> Optional[str]:
        """Extract video code from URL."""
        return extract_code_from_url(url)
    
    def _in_date_range(self, release_date: str) -> bool:
        """Check if date is within configured range."""
        if not release_date or not self.config.date_range_start:
            return True
        
        # Simple string comparison for YYYY-MM-DD format
        if self.config.date_range_start and release_date < self.config.date_range_start:
            return False
        if self.config.date_range_end and release_date > self.config.date_range_end:
            return False
        
        return True
    
    def _scrape_streaming(self, mode: str, start_page: int = 1) -> ExtractionResult:
        """
        Streaming extraction - scrape videos as pages are discovered.
        Much faster than discovering all pages first.
        
        Args:
            mode: Extraction mode
            start_page: Page to start/resume from
        """
        total_pages = self.discovery.get_total_pages()
        print(f"Found {total_pages} pages to process")
        
        completed = 0
        skipped = 0
        failed = 0
        total_discovered = 0
        current_page = start_page
        
        if start_page > 1:
            print(f"Resuming from page {start_page}")
        
        # Update state with total pages
        if self.progress._state:
            self.progress._state.total_pages = total_pages
            self.progress.save_state(self.progress._state)
        
        while current_page <= total_pages and not self._stopped:
            # Get videos from current page
            print(f"\n[Page {current_page}/{total_pages}] Fetching...")
            urls = self.discovery.get_video_urls_for_page(current_page)
            
            if not urls:
                print(f"  No videos found on page {current_page}")
                current_page += 1
                continue
            
            total_discovered += len(urls)
            print(f"  Found {len(urls)} videos")
            
            # Extract codes and update pending list
            page_codes = self.discovery.extract_codes_from_urls(urls)
            
            # Scrape each video on this page
            for i, url in enumerate(urls, 1):
                if self._stopped:
                    break
                
                code = self._extract_code_from_url(url)
                
                # Skip if already exists
                if code and self.storage.video_exists(code):
                    skipped += 1
                    if code:
                        self.progress.mark_completed(code)
                    continue
                
                # Check health
                if not self.health_monitor.check_health():
                    self.health_monitor.record_failure()
                    if self.health_monitor.should_pause():
                        print("Too many failures, stopping...")
                        return self._create_result(
                            success=False, mode=mode,
                            total_discovered=total_discovered,
                            total_completed=completed,
                            total_skipped=skipped,
                            total_failed=failed
                        )
                    self.health_monitor.recover()
                
                # Rate limit
                self.rate_limiter.wait()
                
                # Scrape
                print(f"  [{i}/{len(urls)}] {code or url[:50]}", end=" ")
                success, result = self.retry_handler.execute_with_retry(
                    self.scraper.scrape_video_page, url
                )
                
                if success and result:
                    if self.storage.save_video(result):
                        completed += 1
                        self.rate_limiter.record_success()
                        if code:
                            self.progress.mark_completed(code)
                        print("✓")
                    else:
                        failed += 1
                        self.rate_limiter.record_failure()
                        print("✗ save failed")
                else:
                    failed += 1
                    self.rate_limiter.record_failure()
                    self.retry_handler.record_permanent_failure(
                        code or "unknown", url, str(result)[:100]
                    )
                    print("✗")
                
                if self.rate_limiter.should_cooldown():
                    self.rate_limiter.cooldown()
            
            # Update current page in state
            if self.progress._state:
                self.progress._state.current_page = current_page
                self.progress._state.total_discovered = total_discovered
                self.progress.save_state(self.progress._state)
            
            # Progress summary
            print(f"  Page {current_page} done: {completed} saved, {skipped} skipped, {failed} failed")
            current_page += 1
        
        return self._create_result(
            success=not self._stopped,
            mode=mode,
            total_discovered=total_discovered,
            total_completed=completed,
            total_skipped=skipped,
            total_failed=failed
        )
