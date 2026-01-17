"""
JavTrailers.com Scraper
Extracts video embed URLs and metadata from javtrailers.com
Uses SeleniumBase to bypass Cloudflare protection
"""

import json
import re
import time
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

from bs4 import BeautifulSoup
from seleniumbase import Driver

from utils import format_code


@dataclass
class VideoMetadata:
    """Data class for video metadata"""
    code: str
    content_id: str
    title: str
    duration: str
    release_date: str
    thumbnail_url: str
    cover_url: str
    embed_urls: list
    gallery_images: list
    categories: list
    cast: list
    cast_images: dict
    studio: str
    series: str
    description: str
    scraped_at: str
    source_url: str


class JavTrailersScraper:
    """Scraper for javtrailers.com"""
    
    BASE_URL = "https://javtrailers.com"
    
    def __init__(self, headless: bool = False, save_debug: bool = False):
        self.headless = headless
        self.save_debug = save_debug
        self.driver = None
        
    def _init_driver(self):
        """Initialize SeleniumBase Driver with UC mode"""
        if self.driver is None:
            print("Initializing browser and passing Cloudflare check...")
            
            # Disable snap connections on Linux
            import sys
            import os
            if sys.platform.startswith('linux'):
                # Disable snap
                os.environ['SNAP_NAME'] = ''
                os.environ['SNAP'] = ''
                os.environ['SNAP_INSTANCE_NAME'] = ''
                
                # Try to find Google Chrome (not snap)
                chrome_paths = [
                    '/usr/bin/google-chrome',
                    '/usr/bin/google-chrome-stable',
                    '/usr/bin/chromium-browser',
                    '/usr/bin/chromium'
                ]
                
                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        print(f"Using Chrome binary: {chrome_path}")
                        break
            
            self.driver = Driver(
                uc=True, 
                headless=self.headless
            )
            self.driver.get(self.BASE_URL)
            time.sleep(8)
    
    def _ensure_driver(self):
        """Ensure driver is alive, recreate if needed"""
        if self.driver is None:
            self._init_driver()
            return
        try:
            self.driver.current_url
        except (ConnectionRefusedError, OSError, AttributeError) as e:
            print(f"  Browser connection lost ({type(e).__name__}), restarting...")
            self._close_driver()
            self._init_driver()
        except Exception as e:
            # Log unexpected exceptions but still try to recover
            print(f"  Unexpected browser error ({type(e).__name__}: {e}), restarting...")
            self._close_driver()
            self._init_driver()
            
    def _close_driver(self):
        """Close WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _is_placeholder_image(self, url: str) -> bool:
        """
        Check if an image URL returns a placeholder/nowprinting image.
        DMM returns small placeholder images (~3-5KB) for unavailable content.
        Real thumbnails are typically 10KB+.
        """
        import requests
        
        try:
            # Use HEAD request first to get content length without downloading
            response = requests.head(url, timeout=5, allow_redirects=True)
            
            if response.status_code != 200:
                return True  # Treat errors as placeholder
            
            content_length = response.headers.get('Content-Length')
            if content_length:
                size = int(content_length)
                # Placeholder images are typically very small (under 8KB)
                # Real cover images are usually 15KB+ for thumbnails, 50KB+ for covers
                if size < 8000:
                    return True
            
            return False
            
        except Exception:
            # If we can't check, assume it's valid to avoid false positives
            return False

    def get_video_list_page(self, page: int = 1) -> list:
        """Get list of video URLs from a listing page"""
        self._ensure_driver()
        
        url = f"{self.BASE_URL}/videos" if page == 1 else f"{self.BASE_URL}/videos?page={page}"
        self.driver.get(url)
        time.sleep(5)
        
        video_links = []
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('/video/') and 'videos' not in href:
                full_url = f"{self.BASE_URL}{href}"
                if full_url not in video_links:
                    video_links.append(full_url)
                    
        return video_links


    def scrape_video_page(self, url: str) -> Optional[VideoMetadata]:
        """Scrape metadata from a single video page"""
        self._ensure_driver()
        
        try:
            self.driver.get(url)
            time.sleep(4)
            
            code_match = re.search(r'/video/([a-zA-Z0-9_-]+)', url)
            raw_code = code_match.group(1) if code_match else ""
            code = self._format_code(raw_code)
            url_code = raw_code.lower()
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            page_source = self.driver.page_source
            
            if self.save_debug:
                with open(f'debug_{url_code}.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
            
            nuxt_data = self._extract_nuxt_data(page_source)
            
            # Extract title - prefer H1 element as it's specific to the current page
            # NUXT data contains multiple videos (related/recommended) so it's unreliable for title
            title = ""
            h1_elem = soup.find('h1')
            if h1_elem:
                # Get text content, excluding any nested SVG or script elements
                for svg in h1_elem.find_all('svg'):
                    svg.decompose()
                for script in h1_elem.find_all('script'):
                    script.decompose()
                h1_text = h1_elem.get_text(strip=True)
                # H1 usually contains the full title with code prefix
                if h1_text and len(h1_text) > 3 and h1_text.lower() != 'page not found':
                    title = h1_text
            
            # Fallback to code only - don't use NUXT data for title as it contains related videos
            if not title:
                title = code
            
            # Validate title - reject if it contains HTML/SVG markup
            if '<' in title or '>' in title or 'clip-path' in title or 'fill=' in title:
                print(f"  Warning: Invalid title detected, using code only")
                title = code
                
            # Extract duration
            duration = ""
            if nuxt_data and 'duration' in nuxt_data:
                total_mins = nuxt_data['duration']
                hours = total_mins // 60
                remaining_mins = total_mins % 60
                duration = f"{hours}:{remaining_mins:02d}:00" if hours else f"{remaining_mins}:00"
            else:
                duration_match = re.search(r'Duration:\s*</span>\s*(\d+)\s*mins', page_source)
                if duration_match:
                    total_mins = int(duration_match.group(1))
                    hours = total_mins // 60
                    remaining_mins = total_mins % 60
                    duration = f"{hours}:{remaining_mins:02d}:00" if hours else f"{remaining_mins}:00"
                
            # Extract release date - prefer HTML extraction as it's more reliable
            release_date = ""
            # First try to extract from visible HTML (most reliable) - handle span tags
            date_match = re.search(r'Release Date:(?:</span>)?\s*(\d{1,2}\s+\w+\s+\d{4})', page_source)
            if date_match:
                release_date = date_match.group(1)
            # Fallback to NUXT data if HTML extraction failed
            if not release_date and nuxt_data and 'releaseDate' in nuxt_data:
                release_date = nuxt_data['releaseDate']
                
            # Extract thumbnail (small) and cover (large) URLs
            # ps.jpg = small poster, pl.jpg = large poster
            thumbnail_url = ""
            cover_url = ""
            if nuxt_data and 'image' in nuxt_data:
                img_url = nuxt_data['image']
                # Determine which one we got and derive the other
                if 'pl.jpg' in img_url:
                    cover_url = img_url
                    thumbnail_url = img_url.replace('pl.jpg', 'ps.jpg')
                elif 'ps.jpg' in img_url:
                    thumbnail_url = img_url
                    cover_url = img_url.replace('ps.jpg', 'pl.jpg')
                else:
                    thumbnail_url = img_url
                    cover_url = img_url
            else:
                og_img = soup.find('meta', property='og:image')
                if og_img:
                    img_url = og_img.get('content', '')
                    if 'pl.jpg' in img_url:
                        cover_url = img_url
                        thumbnail_url = img_url.replace('pl.jpg', 'ps.jpg')
                    elif 'ps.jpg' in img_url:
                        thumbnail_url = img_url
                        cover_url = img_url.replace('ps.jpg', 'pl.jpg')
                    else:
                        thumbnail_url = img_url
                        cover_url = img_url
                
            # Click play button to load video player and get trailer URLs
            embed_urls = self._extract_trailer_by_click(url_code, page_source, soup)
            
            # Extract gallery images - click gallery button for high quality
            gallery_images = self._extract_gallery_by_click(url_code)
            if not gallery_images and nuxt_data and 'gallery' in nuxt_data:
                gallery_images = nuxt_data['gallery']
                print(f"  Found {len(gallery_images)} gallery images from NUXT data")
            
            # Extract categories - only from video info section, not navbar
            categories = []
            if nuxt_data and 'categories' in nuxt_data:
                cat_data = nuxt_data['categories']
                if isinstance(cat_data, list):
                    for cat in cat_data:
                        if isinstance(cat, dict) and 'name' in cat:
                            categories.append(cat['name'])
                        elif isinstance(cat, str):
                            categories.append(cat)
            else:
                # Look for categories in the video description area only
                cat_section = soup.find('span', string=re.compile(r'Categories?:', re.IGNORECASE))
                if cat_section:
                    parent = cat_section.find_parent('p')
                    if parent:
                        for link in parent.find_all('a', href=re.compile(r'/categories/')):
                            cat_text = link.get_text(strip=True)
                            if cat_text and len(cat_text) < 50 and cat_text not in categories:
                                categories.append(cat_text)
                            
            # Extract cast
            cast = []
            cast_images = {}
            nuxt_cast_avatars = {}
            nuxt_cast_data = {}  # Store full cast data from NUXT
            try:
                match = re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', page_source, re.DOTALL)
                if match:
                    nuxt_json = json.loads(match.group(1))
                    # First pass: collect all actjpgs URLs
                    for item in nuxt_json:
                        if isinstance(item, str) and 'actjpgs' in item and '.jpg' in item:
                            filename_match = re.search(r'actjpgs/([^.]+)\.jpg', item)
                            if filename_match:
                                filename = filename_match.group(1)
                                nuxt_cast_avatars[filename] = item
                                parts = filename.split('_')
                                if len(parts) == 2:
                                    reversed_name = f"{parts[1]}_{parts[0]}"
                                    nuxt_cast_avatars[reversed_name] = item
                    
                    # Second pass: collect cast objects with avatar info
                    for item in nuxt_json:
                        if isinstance(item, dict) and 'name' in item and 'slug' in item:
                            # Check if this is a cast member (has jpName or avatar)
                            if 'jpName' in item or 'avatar' in item:
                                name = item.get('name', '')
                                slug = item.get('slug', '')
                                # Ensure slug is a string before replacing
                                if isinstance(slug, str):
                                    slug = slug.replace('-', '_')
                                else:
                                    slug = str(slug)
                                avatar = item.get('avatar', '')
                                if name and isinstance(name, str):
                                    nuxt_cast_data[name] = {
                                        'slug': slug,
                                        'avatar': avatar if isinstance(avatar, str) else ''
                                    }
            except:
                pass
            
            def find_cast_image(cast_text, href, nuxt_cast_data, nuxt_cast_avatars):
                """Find cast image URL using multiple matching strategies."""
                # Strategy 1: Check nuxt_cast_data by exact name
                if cast_text in nuxt_cast_data and nuxt_cast_data[cast_text].get('avatar'):
                    return nuxt_cast_data[cast_text]['avatar']
                
                # Strategy 2: Match by href slug
                slug_match = re.search(r'/casts/([^/]+)', href)
                if slug_match:
                    slug = slug_match.group(1).replace('-', '_')
                    if slug in nuxt_cast_avatars:
                        return nuxt_cast_avatars[slug]
                
                # Strategy 3: Match by name parts (handles typos in href)
                # Extract English name parts
                english_match = re.match(r'^([A-Za-z\s]+)', cast_text)
                if english_match:
                    name_parts = english_match.group(1).strip().lower().split()
                    if len(name_parts) >= 2:
                        first = name_parts[0]
                        last = name_parts[-1]
                        # Try different combinations
                        for slug_try in [f"{last}_{first}", f"{first}_{last}"]:
                            if slug_try in nuxt_cast_avatars:
                                return nuxt_cast_avatars[slug_try]
                
                return None
            
            cast_section = soup.find('span', string=re.compile(r'Cast\(s\):'))
            if cast_section:
                parent = cast_section.find_parent('p')
                if parent:
                    for link in parent.find_all('a', href=re.compile(r'/casts/')):
                        cast_text = link.get_text(strip=True)
                        if cast_text and len(cast_text) < 100 and cast_text not in cast:
                            cast.append(cast_text)
                            href = link.get('href', '')
                            img_url = find_cast_image(cast_text, href, nuxt_cast_data, nuxt_cast_avatars)
                            if img_url:
                                cast_images[cast_text] = img_url
            
            if not cast:
                desc_div = soup.find('div', id='description')
                if desc_div:
                    for link in desc_div.find_all('a', href=re.compile(r'/casts/')):
                        cast_text = link.get_text(strip=True)
                        if cast_text and len(cast_text) < 100 and cast_text not in cast:
                            cast.append(cast_text)
                            href = link.get('href', '')
                            img_url = find_cast_image(cast_text, href, nuxt_cast_data, nuxt_cast_avatars)
                            if img_url:
                                cast_images[cast_text] = img_url
                        
            # Extract studio - look in video info section, not navbar
            studio = ""
            # First try NUXT data
            if nuxt_data and 'studio' in nuxt_data:
                studio_info = nuxt_data['studio']
                if isinstance(studio_info, dict) and 'name' in studio_info:
                    studio = studio_info['name']
                elif isinstance(studio_info, str) and not studio_info.isdigit():
                    studio = studio_info
                # If studio_info is an int or numeric string, ignore it and use fallback
            
            # Fallback: look for studio in the video description area
            if not studio:
                studio_section = soup.find('span', string=re.compile(r'Studio:', re.IGNORECASE))
                if studio_section:
                    parent = studio_section.find_parent('p')
                    if parent:
                        studio_link = parent.find('a', href=re.compile(r'/studios/'))
                        if studio_link:
                            studio = studio_link.get_text(strip=True)
            
            # Second fallback: look in description div
            if not studio:
                desc_div = soup.find('div', id='description')
                if desc_div:
                    studio_link = desc_div.find('a', href=re.compile(r'/studios/'))
                    if studio_link:
                        studio = studio_link.get_text(strip=True)
            
            # Third fallback: find studio link in main content area (not nav)
            if not studio:
                main_content = soup.find('main') or soup.find('div', class_='container') or soup
                # Exclude navigation areas - create a copy to avoid modifying while iterating
                nav_elements = main_content.find_all(['nav', 'header', 'footer'])
                for nav in nav_elements:
                    if hasattr(nav, 'decompose'):
                        nav.decompose()
                studio_link = main_content.find('a', href=re.compile(r'/studios/[^/]+'))
                if studio_link:
                    studio = studio_link.get_text(strip=True)
                        
            # Extract series
            series = ""
            if nuxt_data and 'series' in nuxt_data and nuxt_data['series']:
                series = nuxt_data['series']
            else:
                series_link = soup.find('a', href=re.compile(r'/series/'))
                if series_link:
                    series = series_link.get_text(strip=True)
                        
            # Extract description
            description = ""
            desc_elem = soup.find('meta', {'name': 'description'})
            if desc_elem:
                description = desc_elem.get('content', '')
            
            # Validate that we have real content - skip placeholder/incomplete videos
            # Check if thumbnail exists and is not a placeholder
            if not thumbnail_url or not cover_url:
                print(f"  Skipping {code}: No thumbnail/cover image found")
                return None
            
            # Check if the image is a "now printing" placeholder by making a HEAD request
            if self._is_placeholder_image(thumbnail_url):
                print(f"  Skipping {code}: Thumbnail is a placeholder image")
                return None
                
            return VideoMetadata(
                code=code,
                content_id=url_code,
                title=title,
                duration=duration,
                release_date=release_date,
                thumbnail_url=thumbnail_url,
                cover_url=cover_url,
                embed_urls=embed_urls,
                gallery_images=gallery_images,
                categories=categories,
                cast=cast,
                cast_images=cast_images,
                studio=studio,
                series=series if series else "",
                description=description,
                scraped_at=datetime.now().isoformat(),
                source_url=url
            )
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _extract_trailer_by_click(self, url_code: str, page_source: str, soup) -> list:
        """Extract trailer URLs by clicking play button"""
        embed_urls = []
        
        try:
            play_clicked = False
            
            # Try to click the play button
            try:
                # Method 1: JavaScript click on big play button
                self.driver.execute_script(
                    "document.querySelector('.vjs-big-play-button').click();"
                )
                play_clicked = True
            except Exception:
                pass  # Silent fail, will use fallback
            
            if play_clicked:
                time.sleep(3)  # Wait for video to start loading
                player_source = self.driver.page_source
                
                # Save debug HTML if enabled
                if self.save_debug:
                    with open(f'debug_{url_code}_player.html', 'w', encoding='utf-8') as f:
                        f.write(player_source)
                
                # Extract m3u8 URLs from player
                m3u8_pattern = r'(https?://[^"\'<>\s]+\.m3u8[^"\'<>\s]*)'
                matches = re.findall(m3u8_pattern, player_source)
                for match in matches:
                    if 'cloudflare' not in match.lower() and match not in embed_urls:
                        embed_urls.append(match)
                
                # Also check for mp4 URLs
                mp4_pattern = r'(https?://[^"\'<>\s]+\.mp4[^"\'<>\s]*)'
                mp4_matches = re.findall(mp4_pattern, player_source)
                for match in mp4_matches:
                    if 'cloudflare' not in match.lower() and match not in embed_urls:
                        embed_urls.append(match)
                
                if embed_urls:
                    print(f"  Found {len(embed_urls)} trailer URLs from player")
        except Exception as e:
            print(f"  Could not extract trailer by click: {e}")
        
        # Fallback to page source extraction if no URLs found
        if not embed_urls:
            embed_urls = self._extract_embed_urls(page_source, soup)
        
        # Filter out blob URLs
        embed_urls = [u for u in embed_urls if not u.startswith('blob:')]
        return list(set(embed_urls))

    def _extract_nuxt_data(self, page_source: str) -> Optional[dict]:
        """Extract video data from NUXT JSON embedded in page"""
        try:
            match = re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', page_source, re.DOTALL)
            if not match:
                return None
            
            nuxt_json = json.loads(match.group(1))
            if not isinstance(nuxt_json, list) or len(nuxt_json) < 3:
                return None
            
            data = nuxt_json
            gallery_jp = []
            gallery_std = []
            
            for item in data:
                if isinstance(item, str):
                    if ('pics.dmm.co.jp' in item or 'pics.r18.com' in item) and '.jpg' in item:
                        # Check for high quality jp images first
                        if re.search(r'jp-\d+\.jpg', item):
                            gallery_jp.append(item)
                        elif re.search(r'-\d+\.jpg', item):
                            gallery_std.append(item)
            
            # Prefer jp images, fallback to standard
            result = {'gallery': gallery_jp if gallery_jp else gallery_std}
            
            for i, item in enumerate(data):
                if isinstance(item, str):
                    
                    if re.match(r'\d{4}-\d{2}-\d{2}', item):
                        result['releaseDate'] = item
                    if 'pl.jpg' in item and 'image' not in result:
                        result['image'] = item
                elif isinstance(item, int) and 50 < item < 500:
                    if 'duration' not in result:
                        result['duration'] = item
                elif isinstance(item, dict):
                    # Safe check for dict keys
                    if 'name' in item and 'slug' in item:
                        if 'jpName' in item and 'avatar' in item:
                            if 'casts' not in result:
                                result['casts'] = []
                            result['casts'].append(item)
                        elif 'link' in item:
                            # Check if this is a studio link
                            link = item.get('link', '')
                            if isinstance(link, str) and '/studios/' in link:
                                # Store the full object so we can extract name later
                                result['studio'] = item
                            elif isinstance(link, str) and '/series/' in link and 'series' not in result:
                                result['series'] = item.get('name', '')
                elif isinstance(item, list) and len(item) > 0:
                    # Check if it's a list of dicts with name/slug
                    if all(isinstance(x, dict) for x in item):
                        if all('name' in x and 'slug' in x for x in item):
                            if any('jpName' in x for x in item):
                                result['categories'] = item
            
            return result if len(result) > 1 else None
            
        except Exception as e:
            print(f"  Could not parse NUXT data: {e}")
            return None

    def _extract_gallery_by_click(self, url_code: str) -> list:
        """Extract high quality gallery images by clicking gallery button"""
        gallery_images = []
        standard_images = []
        
        try:
            gallery_clicked = False
            
            # Try multiple methods to click the gallery button
            try:
                # Method 1: SeleniumBase click
                if self.driver.is_element_present(".gallery-button"):
                    self.driver.execute_script(
                        "document.querySelector('.gallery-button').scrollIntoView();"
                    )
                    time.sleep(0.5)
                    self.driver.click(".gallery-button")
                    gallery_clicked = True
            except Exception:
                pass  # Silent fail, try method 2
            
            if not gallery_clicked:
                try:
                    # Method 2: JavaScript click
                    self.driver.execute_script(
                        "document.querySelector('.gallery-button').click();"
                    )
                    gallery_clicked = True
                except Exception:
                    pass  # Silent fail
            
            if gallery_clicked:
                time.sleep(3)  # Wait for modal to load images
                gallery_source = self.driver.page_source
                
                # Save gallery HTML for debugging if enabled
                if self.save_debug:
                    with open(f'debug_{url_code}_gallery.html', 'w', encoding='utf-8') as f:
                        f.write(gallery_source)
                
                # Look for high quality jp images (e.g., ofje00696jp-1.jpg)
                jp_patterns = [
                    rf'(https?://pics\.dmm\.co\.jp/digital/video/{url_code}/{url_code}jp-\d+\.jpg)',
                    rf'(https?://pics\.r18\.com/digital/video/{url_code}/{url_code}jp-\d+\.jpg)',
                ]
                for pattern in jp_patterns:
                    jp_matches = re.findall(pattern, gallery_source, re.IGNORECASE)
                    gallery_images.extend(jp_matches)
                
                # Also collect standard images as fallback
                std_patterns = [
                    rf'(https?://pics\.dmm\.co\.jp/digital/video/{url_code}/{url_code}-\d+\.jpg)',
                    rf'(https?://pics\.r18\.com/digital/video/{url_code}/{url_code}-\d+\.jpg)',
                ]
                for pattern in std_patterns:
                    std_matches = re.findall(pattern, gallery_source, re.IGNORECASE)
                    standard_images.extend(std_matches)
                
                # Close modal - try multiple methods
                modal_closed = False
                try:
                    from selenium.webdriver.common.keys import Keys
                    self.driver.find_element("tag name", "body").send_keys(Keys.ESCAPE)
                    time.sleep(0.5)
                    modal_closed = True
                except:
                    pass
                
                # Fallback: click outside modal or close button
                if not modal_closed:
                    try:
                        # Try clicking a close button if it exists
                        close_btn = self.driver.find_element("css selector", ".modal-close, .close-button, [aria-label='Close']")
                        close_btn.click()
                        time.sleep(0.5)
                    except:
                        pass
                
                # Final fallback: navigate away and back
                if not modal_closed:
                    try:
                        # Just continue - the next page load will clear any modal
                        pass
                    except:
                        pass
            
            # Prefer high quality jp images, fallback to standard if none found
            if gallery_images:
                gallery_images = sorted(list(set(gallery_images)))
            elif standard_images:
                gallery_images = sorted(list(set(standard_images)))
            
        except Exception:
            pass  # Silent fail
        
        return gallery_images


    def _extract_embed_urls(self, page_source: str, soup) -> list:
        """Extract video embed/trailer URLs"""
        embed_urls = []
        
        trailer_patterns = [
            r'"trailer"\s*:\s*"([^"]+)"',
            r"'trailer'\s*:\s*'([^']+)'",
            r'trailer["\']?\s*:\s*["\']([a-zA-Z0-9_-]+)["\']',
        ]
        trailer_id = None
        for pattern in trailer_patterns:
            match = re.search(pattern, page_source)
            if match:
                trailer_id = match.group(1)
                break
        
        if trailer_id:
            stream_patterns = [
                r'apiStream\s*:\s*"([^"]+)"',
                r"apiStream\s*:\s*'([^']+)'",
                r'"apiStream"\s*:\s*"([^"]+)"',
            ]
            for pattern in stream_patterns:
                match = re.search(pattern, page_source)
                if match:
                    stream_base = match.group(1)
                    trailer_url = f"{stream_base}/{trailer_id}/playlist.m3u8"
                    embed_urls.append(trailer_url)
                    print(f"  Found trailer URL")
                    break
                    
        video_patterns = [
            r'(https?://[^"\'<>\s]+\.m3u8[^"\'<>\s]*)',
            r'(https?://[^"\'<>\s]+\.mp4[^"\'<>\s]*)',
        ]
        for pattern in video_patterns:
            matches = re.findall(pattern, page_source)
            for match in matches:
                if 'cloudflare' not in match.lower() and match not in embed_urls:
                    embed_urls.append(match)
        
        embed_urls = [u for u in embed_urls if not u.startswith('blob:')]
        return list(set(embed_urls))
            
    def _format_code(self, raw_code: str) -> str:
        """Format video code from URL format to standard format"""
        return format_code(raw_code)
    
    def _cleanup_debug_files(self):
        """Remove all debug HTML files"""
        import glob
        import os
        for f in glob.glob('debug_*.html'):
            try:
                os.remove(f)
            except:
                pass
            
    def close(self):
        """Clean up resources"""
        self._close_driver()
        self._cleanup_debug_files()
