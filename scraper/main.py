"""
Main entry point for JavTrailers scraper with full-site extraction support.
"""

import argparse
import signal
import sys
from typing import Optional
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = Path(__file__).parent / '.env'
if not env_path.exists():
    print(f"⚠️  Warning: .env file not found at {env_path}")
    print("Using environment variables from system")
else:
    load_dotenv(env_path)
    print(f"✓ Loaded environment from .env")

from config import ScraperConfig, RateLimitConfig, RetryConfig
from scraper_controller import ScraperController


# Global controller for signal handling
_controller: Optional[ScraperController] = None


def signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    print("\n\n" + "=" * 60)
    print("STOP SIGNAL RECEIVED - SHUTTING DOWN GRACEFULLY")
    print("=" * 60)
    if _controller:
        _controller.stop()
        print("Waiting for current operation to complete...")
    else:
        print("Exiting immediately...")
        sys.exit(0)


def run_extraction(args):
    """Run extraction with ScraperController."""
    global _controller
    
    # Build configuration
    rate_config = RateLimitConfig(
        min_delay=args.min_delay,
        initial_delay=args.delay,
        cooldown_duration=args.cooldown
    )
    
    retry_config = RetryConfig(
        max_retries=args.retries
    )
    
    config = ScraperConfig(
        headless=args.headless,
        save_debug=args.debug,
        rate_limit=rate_config,
        retry=retry_config,
        date_range_start=args.date_start,
        date_range_end=args.date_end,
        specific_codes=args.codes.split(',') if args.codes else None
    )
    
    # Create controller
    _controller = ScraperController(config)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    # SIGTERM is not reliably available on Windows
    if hasattr(signal, 'SIGTERM') and sys.platform != 'win32':
        signal.signal(signal.SIGTERM, signal_handler)
    
    # Run extraction
    result = _controller.run(mode=args.mode, resume=not args.no_resume)
    
    # Print summary
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Mode:        {result.mode}")
    print(f"Success:     {result.success}")
    print(f"Duration:    {result.duration_seconds / 60:.1f} minutes")
    print(f"Discovered:  {result.total_discovered}")
    print(f"Completed:   {result.total_completed}")
    print(f"Skipped:     {result.total_skipped}")
    print(f"Failed:      {result.total_failed}")
    print(f"Speed:       {result.videos_per_hour:.1f} videos/hour")
    
    if result.failed_codes:
        print(f"\nFailed videos ({len(result.failed_codes)}):")
        for f in result.failed_codes[:10]:
            print(f"  - {f['code']}: {f['reason'][:50]}")
        if len(result.failed_codes) > 10:
            print(f"  ... and {len(result.failed_codes) - 10} more")
    
    return 0 if result.success else 1


def run_legacy(args):
    """Run legacy single-page scraper for backward compatibility."""
    from javtrailers_scraper import JavTrailersScraper
    from storage_factory import create_storage
    import time
    import re
    
    scraper = JavTrailersScraper(headless=args.headless)
    storage = create_storage()  # Uses Supabase
    
    try:
        if args.url:
            # Single URL mode
            print(f"Scraping: {args.url}")
            video_data = scraper.scrape_video_page(args.url)
            
            if video_data:
                if storage.save_video(video_data):
                    print(f"✓ Saved: {video_data.code}")
                else:
                    print("✗ Failed to save")
            else:
                print("✗ Failed to scrape")
        else:
            # Legacy pages mode
            print(f"Starting legacy scrape (max {args.pages} pages)...")
            
            all_video_urls = []
            for page in range(1, args.pages + 1):
                print(f"Fetching video list page {page}...")
                urls = scraper.get_video_list_page(page)
                all_video_urls.extend(urls)
                print(f"  Found {len(urls)} videos on page {page}")
                time.sleep(args.delay)
            
            all_video_urls = list(set(all_video_urls))
            print(f"\nTotal unique videos found: {len(all_video_urls)}")
            
            success_count = 0
            skip_count = 0
            error_count = 0
            
            for i, url in enumerate(all_video_urls, 1):
                code_match = re.search(r'/([A-Z]+-\d+)', url, re.IGNORECASE)
                code = code_match.group(1).upper() if code_match else None
                
                if not args.no_skip and code and storage.video_exists(code):
                    print(f"[{i}/{len(all_video_urls)}] Skipping {code} (already exists)")
                    skip_count += 1
                    continue
                
                print(f"[{i}/{len(all_video_urls)}] Scraping: {url}")
                video_data = scraper.scrape_video_page(url)
                
                if video_data:
                    if storage.save_video(video_data):
                        success_count += 1
                        print(f"  ✓ Saved: {video_data.code}")
                    else:
                        error_count += 1
                        print(f"  ✗ Failed to save")
                else:
                    error_count += 1
                    print(f"  ✗ Failed to scrape")
                
                time.sleep(args.delay)
            
            print("\n" + "=" * 50)
            print("SCRAPING COMPLETE")
            print("=" * 50)
            print(f"Successful: {success_count}")
            print(f"Skipped:    {skip_count}")
            print(f"Errors:     {error_count}")
            
    finally:
        scraper.close()
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='JavTrailers.com Full-Site Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full site extraction (resumable)
  python main.py --mode full
  
  # Random page order extraction
  python main.py --mode random
  
  # Incremental update (new videos only)
  python main.py --mode incremental
  
  # Retry previously failed videos
  python main.py --mode retry-failed
  
  # Extract specific codes
  python main.py --mode codes --codes "SSIS-345,IPZZ-292,OFJE-696"
  
  # Extract videos from date range
  python main.py --mode date-range --date-start 2025-01-01 --date-end 2025-12-31
  
  # Legacy mode (limited pages)
  python main.py --legacy --pages 10
  
  # Single URL
  python main.py --url https://javtrailers.com/video/ssis345
"""
    )
    
    # Mode selection
    parser.add_argument(
        '--mode', 
        type=str, 
        default='random',
        choices=['full', 'incremental', 'retry-failed', 'date-range', 'codes', 'random', 'cast'],
        help='Extraction mode (default: random)'
    )
    
    # Resume control
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh instead of resuming from saved state'
    )
    
    # Mode-specific options
    parser.add_argument(
        '--codes',
        type=str,
        help='Comma-separated list of video codes (for codes mode)'
    )
    parser.add_argument(
        '--date-start',
        type=str,
        help='Start date YYYY-MM-DD (for date-range mode)'
    )
    parser.add_argument(
        '--date-end',
        type=str,
        help='End date YYYY-MM-DD (for date-range mode)'
    )
    
    # Rate limiting
    parser.add_argument(
        '--delay',
        type=float,
        default=3.0,
        help='Initial delay between requests in seconds (default: 3.0)'
    )
    parser.add_argument(
        '--min-delay',
        type=float,
        default=2.0,
        help='Minimum delay between requests (default: 2.0)'
    )
    parser.add_argument(
        '--cooldown',
        type=float,
        default=300.0,
        help='Cooldown duration in seconds after failures (default: 300)'
    )
    
    # Retry settings
    parser.add_argument(
        '--retries',
        type=int,
        default=3,
        help='Max retry attempts per video (default: 3)'
    )
    
    # Browser settings
    parser.add_argument(
        '--headless',
        action='store_true',
        help='Run browser in headless mode (may not bypass Cloudflare)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Save debug HTML files'
    )
    
    # Legacy mode
    parser.add_argument(
        '--legacy',
        action='store_true',
        help='Use legacy scraper (limited pages, no resilience)'
    )
    parser.add_argument(
        '--pages',
        type=int,
        default=5,
        help='Number of pages to scrape in legacy mode (default: 5)'
    )
    parser.add_argument(
        '--url',
        type=str,
        help='Scrape a single URL'
    )
    parser.add_argument(
        '--no-skip',
        action='store_true',
        help='Do not skip existing videos in legacy mode'
    )
    
    args = parser.parse_args()
    
    # Route to appropriate handler
    if args.legacy or args.url:
        return run_legacy(args)
    else:
        return run_extraction(args)


if __name__ == '__main__':
    sys.exit(main())
