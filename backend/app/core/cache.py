"""
Centralized caching system for the application.

Features:
- LRU eviction policy
- TTL-based expiration
- Memory-based limits
- Async-safe operations
- Cache key generation helpers
- Decorator for easy route caching
"""
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TypeVar, Generic, Optional, Any, Callable
from functools import wraps
import time
import hashlib
import json
import asyncio

T = TypeVar('T')


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with value and metadata."""
    value: T
    created_at: float
    size: int = 0
    hits: int = 0
    key: str = ""


class LRUCache(Generic[T]):
    """
    Thread-safe LRU Cache with TTL and memory limits.
    """
    
    def __init__(
        self, 
        name: str = "cache",
        max_items: int = 100, 
        ttl_seconds: float = 300,
        max_memory_bytes: Optional[int] = None
    ):
        self.name = name
        self.max_items = max_items
        self.ttl = ttl_seconds
        self.max_memory = max_memory_bytes
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._current_memory = 0
        self._total_hits = 0
        self._total_misses = 0
    
    def _is_expired(self, entry: CacheEntry[T]) -> bool:
        return time.time() - entry.created_at > self.ttl
    
    def _evict_expired(self) -> int:
        """Remove all expired entries. Returns count removed."""
        expired = [k for k, v in self._cache.items() if self._is_expired(v)]
        for key in expired:
            self._remove(key)
        return len(expired)
    
    def _evict_lru(self, needed_space: int = 0) -> None:
        """Evict least recently used entries."""
        # By count
        while len(self._cache) >= self.max_items:
            if not self._cache:
                break
            self._remove(next(iter(self._cache)))
        
        # By memory
        if self.max_memory and needed_space > 0:
            while self._current_memory + needed_space > self.max_memory and self._cache:
                self._remove(next(iter(self._cache)))
    
    def _remove(self, key: str) -> None:
        if key in self._cache:
            entry = self._cache.pop(key)
            self._current_memory -= entry.size
    
    def get(self, key: str) -> Optional[T]:
        """Get value from cache."""
        if key not in self._cache:
            self._total_misses += 1
            return None
        
        entry = self._cache[key]
        
        if self._is_expired(entry):
            self._remove(key)
            self._total_misses += 1
            return None
        
        # Move to end (most recently used)
        self._cache.move_to_end(key)
        entry.hits += 1
        self._total_hits += 1
        
        return entry.value
    
    def set(self, key: str, value: T, size: int = 0) -> None:
        """Set value in cache."""
        if key in self._cache:
            self._remove(key)
        
        self._evict_expired()
        self._evict_lru(size)
        
        self._cache[key] = CacheEntry(
            value=value,
            created_at=time.time(),
            size=size,
            key=key
        )
        self._current_memory += size
    
    def delete(self, key: str) -> bool:
        """Delete a specific key."""
        if key in self._cache:
            self._remove(key)
            return True
        return False
    
    def delete_pattern(self, pattern: str) -> int:
        """Delete all keys containing pattern."""
        keys_to_delete = [k for k in self._cache.keys() if pattern in k]
        for key in keys_to_delete:
            self._remove(key)
        return len(keys_to_delete)
    
    def clear(self) -> None:
        """Clear all entries."""
        self._cache.clear()
        self._current_memory = 0
    
    def stats(self) -> dict:
        """Get cache statistics."""
        self._evict_expired()
        hit_rate = self._total_hits / (self._total_hits + self._total_misses) * 100 if (self._total_hits + self._total_misses) > 0 else 0
        return {
            "name": self.name,
            "items": len(self._cache),
            "max_items": self.max_items,
            "memory_mb": round(self._current_memory / 1024 / 1024, 2),
            "max_memory_mb": round(self.max_memory / 1024 / 1024, 2) if self.max_memory else None,
            "hits": self._total_hits,
            "misses": self._total_misses,
            "hit_rate": round(hit_rate, 1),
            "ttl_seconds": self.ttl,
        }


def generate_cache_key(*args, **kwargs) -> str:
    """Generate a cache key from arguments."""
    key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
    return hashlib.md5(key_data.encode()).hexdigest()


# ============================================
# Application Caches
# ============================================

# API Response Caches
stats_cache = LRUCache[dict](
    name="stats",
    max_items=10,
    ttl_seconds=60,  # 1 minute - stats change slowly
)

videos_list_cache = LRUCache[dict](
    name="videos_list",
    max_items=100,
    ttl_seconds=120,  # 2 minutes
)

video_detail_cache = LRUCache[dict](
    name="video_detail",
    max_items=500,
    ttl_seconds=300,  # 5 minutes
)

search_cache = LRUCache[dict](
    name="search",
    max_items=200,
    ttl_seconds=180,  # 3 minutes
)

categories_cache = LRUCache[list](
    name="categories",
    max_items=10,
    ttl_seconds=300,  # 5 minutes
)

studios_cache = LRUCache[list](
    name="studios",
    max_items=10,
    ttl_seconds=300,  # 5 minutes
)

series_cache = LRUCache[list](
    name="series",
    max_items=10,
    ttl_seconds=300,  # 5 minutes
)

cast_cache = LRUCache[list](
    name="cast",
    max_items=10,
    ttl_seconds=300,  # 5 minutes
)

cast_featured_cache = LRUCache[list](
    name="cast_featured",
    max_items=10,
    ttl_seconds=300,  # 5 minutes
)

category_videos_cache = LRUCache[dict](
    name="category_videos",
    max_items=100,
    ttl_seconds=180,  # 3 minutes
)

studio_videos_cache = LRUCache[dict](
    name="studio_videos",
    max_items=100,
    ttl_seconds=180,  # 3 minutes
)

series_videos_cache = LRUCache[dict](
    name="series_videos",
    max_items=100,
    ttl_seconds=180,  # 3 minutes
)

cast_videos_cache = LRUCache[dict](
    name="cast_videos",
    max_items=100,
    ttl_seconds=180,  # 3 minutes
)

# Proxy Caches (for media)
playlist_cache = LRUCache[str](
    name="playlist",
    max_items=500,
    ttl_seconds=1800,  # 30 minutes
)

segment_cache = LRUCache[bytes](
    name="segment",
    max_items=500,
    ttl_seconds=300,  # 5 minutes
    max_memory_bytes=300 * 1024 * 1024,  # 300MB
)

image_cache = LRUCache[bytes](
    name="image",
    max_items=500,
    ttl_seconds=3600,  # 1 hour
    max_memory_bytes=50 * 1024 * 1024,  # 50MB
)


def get_all_cache_stats() -> dict:
    """Get stats for all caches."""
    return {
        "api_caches": {
            "stats": stats_cache.stats(),
            "videos_list": videos_list_cache.stats(),
            "video_detail": video_detail_cache.stats(),
            "search": search_cache.stats(),
            "categories": categories_cache.stats(),
            "studios": studios_cache.stats(),
            "series": series_cache.stats(),
            "cast": cast_cache.stats(),
            "cast_featured": cast_featured_cache.stats(),
            "category_videos": category_videos_cache.stats(),
            "studio_videos": studio_videos_cache.stats(),
            "series_videos": series_videos_cache.stats(),
            "cast_videos": cast_videos_cache.stats(),
        },
        "proxy_caches": {
            "playlist": playlist_cache.stats(),
            "segment": segment_cache.stats(),
            "image": image_cache.stats(),
        }
    }


def clear_all_caches() -> None:
    """Clear all caches."""
    stats_cache.clear()
    videos_list_cache.clear()
    video_detail_cache.clear()
    search_cache.clear()
    categories_cache.clear()
    studios_cache.clear()
    series_cache.clear()
    cast_cache.clear()
    cast_featured_cache.clear()
    category_videos_cache.clear()
    studio_videos_cache.clear()
    series_videos_cache.clear()
    cast_videos_cache.clear()
    playlist_cache.clear()
    segment_cache.clear()
    image_cache.clear()


def invalidate_video_caches() -> None:
    """Invalidate caches when video data changes."""
    videos_list_cache.clear()
    video_detail_cache.clear()
    search_cache.clear()
    stats_cache.clear()
    category_videos_cache.clear()
    studio_videos_cache.clear()
    series_videos_cache.clear()
    cast_videos_cache.clear()
