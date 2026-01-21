"""
Content discovery for finding all videos on the site.
Handles pagination and detects new content.
"""

import re
import time
from typing import List, Callable, Optional, Set, TYPE_CHECKING

from bs4 import BeautifulSoup

from utils import extract_code_from_url

if TYPE_CHECKING:
    from javtrailers_scraper import JavTrailersScraper


class ContentDiscovery:
    """Discovers all video URLs across the site."""
    
    BASE_URL = "https://javtrailers.com"
    
    def __init__(self, scraper: "JavTrailersScraper"):
        """
        Initialize with scraper for page fetching.
        
        Args:
            scraper: JavTrailersScraper instance
        """
        self.scraper = scraper
        self._total_pages: Optional[int] = None
    
    def get_total_pages(self) -> int:
        """
        Discover total number of listing pages.
        
        Returns:
            Total page count
        """
        if self._total_pages is not None:
            return self._total_pages
        
        self.scraper._ensure_driver()
        
        # Load first page to find pagination info
        url = f"{self.BASE_URL}/videos"
        self.scraper.driver.get(url)
        time.sleep(5)
        
        soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')
        
        # Look for pagination links to find max page
        max_page = 1
        
        # Method 1: Look for page numbers in pagination
        for link in soup.find_all('a', href=True):
            href = link['href']
            match = re.search(r'\?page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        
        # Method 2: Look for "last" page link
        last_link = soup.find('a', {'aria-label': 'Last'}) or soup.find('a', string=re.compile(r'Last|»'))
        if last_link and last_link.get('href'):
            match = re.search(r'\?page=(\d+)', last_link['href'])
            if match:
                max_page = max(max_page, int(match.group(1)))
        
        self._total_pages = max_page
        print(f"Discovered {max_page} total pages")
        return max_page
    
    def get_video_urls_for_page(self, page: int, max_retries: int = 3) -> List[str]:
        """
        Get video URLs from a specific listing page.
        
        Args:
            page: Page number to fetch
            max_retries: Number of retry attempts
            
        Returns:
            List of video URLs found on page
        """
        self.scraper._ensure_driver()
        
        url = f"{self.BASE_URL}/videos" if page == 1 else f"{self.BASE_URL}/videos?page={page}"
        
        for attempt in range(max_retries):
            try:
                self.scraper.driver.get(url)
                time.sleep(5)
                
                video_links = []
                soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')
                
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('/video/') and 'videos' not in href:
                        full_url = f"{self.BASE_URL}{href}"
                        if full_url not in video_links:
                            video_links.append(full_url)
                
                if video_links:
                    return video_links
                    
                # Empty page might mean we've gone past the end
                if attempt == 0:
                    print(f"  Page {page} returned no videos, retrying...")
                    
            except Exception as e:
                print(f"  Error fetching page {page} (attempt {attempt + 1}): {e}")
                time.sleep(2)
        
        print(f"  Failed to fetch page {page} after {max_retries} attempts")
        return []
    
    def get_all_video_urls(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        delay: float = 3.0
    ) -> List[str]:
        """
        Discover all video URLs across all pages.
        
        Args:
            progress_callback: Called with (current_page, total_pages) for progress
            delay: Delay between page fetches
            
        Returns:
            List of all video URLs found
        """
        total_pages = self.get_total_pages()
        all_urls: Set[str] = set()
        failed_pages: List[int] = []
        
        for page in range(1, total_pages + 1):
            if progress_callback:
                progress_callback(page, total_pages)
            
            print(f"Discovering page {page}/{total_pages}...")
            urls = self.get_video_urls_for_page(page)
            
            if urls:
                all_urls.update(urls)
                print(f"  Found {len(urls)} videos (total: {len(all_urls)})")
            else:
                failed_pages.append(page)
            
            if page < total_pages:
                time.sleep(delay)
        
        if failed_pages:
            print(f"Warning: Failed to fetch pages: {failed_pages}")
        
        return list(all_urls)
    
    def get_new_videos(self, known_codes: List[str]) -> List[str]:
        """
        Find videos not in the known codes list (for incremental mode).
        
        Args:
            known_codes: List of already known video codes
            
        Returns:
            List of URLs for new videos
        """
        known_set = set(c.upper() for c in known_codes)
        all_urls = self.get_all_video_urls()
        
        new_urls = []
        for url in all_urls:
            code = self._extract_code_from_url(url)
            if code and code.upper() not in known_set:
                new_urls.append(url)
        
        print(f"Found {len(new_urls)} new videos out of {len(all_urls)} total")
        return new_urls
    
    def _extract_code_from_url(self, url: str) -> Optional[str]:
        """
        Extract video code from URL.
        
        Args:
            url: Video URL
            
        Returns:
            Formatted video code or None
        """
        return extract_code_from_url(url)
    
    def extract_codes_from_urls(self, urls: List[str]) -> List[str]:
        """
        Extract video codes from a list of URLs.
        
        Args:
            urls: List of video URLs
            
        Returns:
            List of video codes
        """
        codes = []
        for url in urls:
            code = self._extract_code_from_url(url)
            if code:
                codes.append(code)
        return codes

    def get_all_cast_urls(self) -> List[str]:
        """
        Discovers all cast URLs across all pages.

        Returns:
            List of all cast URLs found
        """
        self.scraper._ensure_driver()

        # Load first page to find pagination info
        url = f"{self.BASE_URL}/casts"
        self.scraper.driver.get(url)
        time.sleep(5)

        soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

        # Look for pagination links to find max page
        max_page = 1
        for link in soup.find_all('a', href=True):
            href = link['href']
            match = re.search(r'\?page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        print(f"Discovered {max_page} total cast pages")

        all_urls: Set[str] = set()

        for page in range(1, max_page + 1):
            print(f"Discovering cast page {page}/{max_page}...")

            page_url = f"{self.BASE_URL}/casts?page={page}"
            self.scraper.driver.get(page_url)
            time.sleep(5)

            soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/casts/'):
                    full_url = f"{self.BASE_URL}{href}"
                    if full_url not in all_urls:
                        all_urls.add(full_url)

            print(f"  Found {len(all_urls)} total casts")

        return list(all_urls)

    def get_video_urls_for_cast(self, cast_url: str) -> List[str]:
        """
        Get video URLs from a specific cast listing page.

        Args:
            cast_url: Cast page URL to fetch

        Returns:
            List of video URLs found on page
        """
        self.scraper._ensure_driver()

        self.scraper.driver.get(cast_url)
        time.sleep(5)

        soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

        max_page = 1
        for link in soup.find_all('a', href=True):
            href = link['href']
            match = re.search(r'\?page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        all_urls: Set[str] = set()

        for page in range(1, max_page + 1):
            page_url = f"{cast_url}?page={page}"
            self.scraper.driver.get(page_url)
            time.sleep(5)

            soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/video/'):
                    full_url = f"{self.BASE_URL}{href}"
                    if full_url not in all_urls:
                        all_urls.add(full_url)

        return list(all_urls)

    def get_all_cast_urls(self) -> List[str]:
        """
        Discovers all cast URLs across all pages.

        Returns:
            List of all cast URLs found
        """
        self.scraper._ensure_driver()

        # Load first page to find pagination info
        url = f"{self.BASE_URL}/casts"
        self.scraper.driver.get(url)
        time.sleep(5)

        soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

        # Look for pagination links to find max page
        max_page = 1
        for link in soup.find_all('a', href=True):
            href = link['href']
            match = re.search(r'\?page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        print(f"Discovered {max_page} total cast pages")

        all_urls: Set[str] = set()

        for page in range(1, max_page + 1):
            print(f"Discovering cast page {page}/{max_page}...")

            page_url = f"{self.BASE_URL}/casts?page={page}"
            self.scraper.driver.get(page_url)
            time.sleep(5)

            soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/casts/'):
                    full_url = f"{self.BASE_URL}{href}"
                    if full_url not in all_urls:
                        all_urls.add(full_url)

            print(f"  Found {len(all_urls)} total casts")

        return list(all_urls)

    def get_video_urls_for_cast(self, cast_url: str) -> List[str]:
        """
        Get video URLs from a specific cast listing page.

        Args:
            cast_url: Cast page URL to fetch

        Returns:
            List of video URLs found on page
        """
        self.scraper._ensure_driver()

        self.scraper.driver.get(cast_url)
        time.sleep(5)

        soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

        max_page = 1
        for link in soup.find_all('a', href=True):
            href = link['href']
            match = re.search(r'\?page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        all_urls: Set[str] = set()

        for page in range(1, max_page + 1):
            page_url = f"{cast_url}?page={page}"
            self.scraper.driver.get(page_url)
            time.sleep(5)

            soup = BeautifulSoup(self.scraper.driver.page_source, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/video/'):
                    full_url = f"{self.BASE_URL}{href}"
                    if full_url not in all_urls:
                        all_urls.add(full_url)

        return list(all_urls)
