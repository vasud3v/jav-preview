"""
Shared utility functions for the scraper.
"""

import re
from typing import Optional


def extract_code_from_url(url: str) -> Optional[str]:
    """
    Extract and format video code from URL.
    
    Args:
        url: Video URL (e.g., https://javtrailers.com/video/ssis345)
        
    Returns:
        Formatted video code (e.g., SSIS-345) or None if not found
    """
    match = re.search(r'/video/([a-zA-Z0-9_-]+)', url)
    if not match:
        return None
    
    raw_code = match.group(1)
    # Remove prefixes like h_123 or 1
    code = re.sub(r'^(h_\d+|1)', '', raw_code)
    
    # Format as LETTERS-NUMBERS
    code_match = re.match(r'([a-zA-Z]+)(\d+)', code)
    if code_match:
        letters = code_match.group(1).upper()
        numbers = code_match.group(2).lstrip('0') or '0'
        return f"{letters}-{numbers}"
    
    return raw_code.upper()


def code_to_url(code: str) -> str:
    """
    Convert video code to URL.
    
    Args:
        code: Video code (e.g., SSIS-345)
        
    Returns:
        Video URL
    """
    # Remove dash and lowercase for URL format
    url_code = code.replace('-', '').lower()
    return f"https://javtrailers.com/video/{url_code}"


def format_code(raw_code: str) -> str:
    """
    Format video code from URL format to standard format.
    
    Args:
        raw_code: Raw code from URL (e.g., ssis345, 15htd00003)
        
    Returns:
        Formatted code (e.g., SSIS-345, HTD-003)
    """
    # Remove common prefixes: h_XXX, numeric prefixes (1, 15, 118, etc.)
    code = re.sub(r'^(h_\d+|\d+)', '', raw_code)
    
    # If nothing left after removing prefix, try extracting letters from original
    if not code:
        match = re.search(r'([a-zA-Z]+)(\d+)', raw_code)
        if match:
            letters = match.group(1).upper()
            numbers = match.group(2).lstrip('0') or '0'
            return f"{letters}-{numbers}"
        return raw_code.upper()
    
    match = re.match(r'([a-zA-Z]+)(\d+)', code)
    if match:
        letters = match.group(1).upper()
        numbers = match.group(2).lstrip('0') or '0'
        return f"{letters}-{numbers}"
    return raw_code.upper()
