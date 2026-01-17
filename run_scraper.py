"""
Simple runner - just run: python run_scraper.py

Usage:
    python run_scraper.py              # Random page order extraction (default)
    python run_scraper.py --mode random  # Random page order extraction
    python run_scraper.py --mode full  # Full site extraction (page 1 to end)
    python run_scraper.py --mode incremental  # New videos only
    python run_scraper.py --mode retry-failed  # Retry failed videos
    python run_scraper.py --visible    # Show browser window
    python run_scraper.py --no-resume  # Start fresh
"""
import sys
import os
import argparse

# Get the directory containing this script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Add scraper directory to path
scraper_dir = os.path.join(script_dir, 'scraper')
sys.path.insert(0, scraper_dir)
os.chdir(scraper_dir)

from scraper_controller import ScraperController
from config import ScraperConfig


def main():
    parser = argparse.ArgumentParser(description='JavTrailers Scraper')
    parser.add_argument('--mode', type=str, default='random',
                        choices=['full', 'incremental', 'retry-failed', 'random'],
                        help='Extraction mode (default: random)')
    parser.add_argument('--visible', action='store_true',
                        help='Show browser window (default: headless)')
    parser.add_argument('--no-resume', action='store_true',
                        help='Start fresh instead of resuming')
    parser.add_argument('--debug', action='store_true',
                        help='Save debug HTML files')
    args = parser.parse_args()

    print(f"Starting JavTrailers scraper ({args.mode} mode)...")
    print("Press Ctrl+C to stop (progress is saved automatically)\n")
    
    config = ScraperConfig(
        headless=not args.visible,
        save_debug=args.debug
    )
    controller = ScraperController(config)
    
    try:
        result = controller.run(mode=args.mode, resume=not args.no_resume)
        
        print(f"\nDone! Scraped {result.total_completed} videos")
        print(f"Failed: {result.total_failed} | Skipped: {result.total_skipped}")
        
        if result.duration_seconds > 0:
            print(f"Speed: {result.videos_per_hour:.1f} videos/hour")
            
        return 0 if result.success else 1
        
    except KeyboardInterrupt:
        print("\nStopped. Run again to resume.")
        return 0


if __name__ == '__main__':
    sys.exit(main())
