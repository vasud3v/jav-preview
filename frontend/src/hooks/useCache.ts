/**
 * Persistent caching hook with stale-while-revalidate pattern
 */
import { useState, useEffect, useCallback, useRef } from 'react';

interface CacheEntry<T> {
  data: T;
  timestamp: number;
  ttl: number;
}

// Cache storage using sessionStorage for persistence across page navigations
const CACHE_PREFIX = 'app_cache_';

function getCacheKey(key: string): string {
  return `${CACHE_PREFIX}${key}`;
}

function getFromStorage<T>(key: string): CacheEntry<T> | null {
  try {
    const item = sessionStorage.getItem(getCacheKey(key));
    if (!item) return null;
    return JSON.parse(item);
  } catch {
    return null;
  }
}

function setToStorage<T>(key: string, entry: CacheEntry<T>): void {
  try {
    sessionStorage.setItem(getCacheKey(key), JSON.stringify(entry));
  } catch {
    // Storage full - clear old entries
    clearOldEntries();
    try {
      sessionStorage.setItem(getCacheKey(key), JSON.stringify(entry));
    } catch {
      // Still failing, ignore
    }
  }
}

function clearOldEntries(): void {
  const now = Date.now();
  const keysToRemove: string[] = [];
  
  for (let i = 0; i < sessionStorage.length; i++) {
    const key = sessionStorage.key(i);
    if (key?.startsWith(CACHE_PREFIX)) {
      try {
        const entry = JSON.parse(sessionStorage.getItem(key) || '');
        if (now - entry.timestamp > entry.ttl) {
          keysToRemove.push(key);
        }
      } catch {
        keysToRemove.push(key!);
      }
    }
  }
  
  keysToRemove.forEach(key => sessionStorage.removeItem(key));
}

// In-memory cache for faster access (primary cache, sessionStorage is backup)
const memoryCache = new Map<string, CacheEntry<unknown>>();

export function getCached<T>(key: string): T | null {
  // Check memory first (fast path)
  const memEntry = memoryCache.get(key) as CacheEntry<T> | undefined;
  if (memEntry) {
    if (Date.now() - memEntry.timestamp <= memEntry.ttl) {
      return memEntry.data;
    }
    // Expired in memory
    memoryCache.delete(key);
  }
  
  // Fall back to storage only if not in memory
  const entry = getFromStorage<T>(key);
  if (!entry) return null;
  
  if (Date.now() - entry.timestamp > entry.ttl) {
    sessionStorage.removeItem(getCacheKey(key));
    return null;
  }
  
  // Populate memory cache for next access
  memoryCache.set(key, entry);
  return entry.data;
}

export function setCache<T>(key: string, data: T, ttlMs: number): void {
  const entry: CacheEntry<T> = {
    data,
    timestamp: Date.now(),
    ttl: ttlMs,
  };
  // Always set in memory (fast)
  memoryCache.set(key, entry);
  
  // Debounce sessionStorage writes to reduce I/O
  // Only write to storage for longer TTLs (> 2 minutes)
  if (ttlMs > 2 * 60 * 1000) {
    setToStorage(key, entry);
  }
}

export function invalidateCache(pattern?: string): void {
  if (!pattern) {
    // Clear all
    memoryCache.clear();
    const keysToRemove: string[] = [];
    for (let i = 0; i < sessionStorage.length; i++) {
      const key = sessionStorage.key(i);
      if (key?.startsWith(CACHE_PREFIX)) {
        keysToRemove.push(key);
      }
    }
    keysToRemove.forEach(key => sessionStorage.removeItem(key));
    return;
  }
  
  // Clear matching pattern
  for (const key of memoryCache.keys()) {
    if (key.includes(pattern)) {
      memoryCache.delete(key);
    }
  }
  
  const keysToRemove: string[] = [];
  for (let i = 0; i < sessionStorage.length; i++) {
    const key = sessionStorage.key(i);
    if (key?.startsWith(CACHE_PREFIX) && key.includes(pattern)) {
      keysToRemove.push(key);
    }
  }
  keysToRemove.forEach(key => sessionStorage.removeItem(key));
}

interface UseCacheOptions<T> {
  /** Cache key */
  key: string;
  /** Fetcher function */
  fetcher: () => Promise<T>;
  /** Time to live in milliseconds */
  ttl?: number;
  /** Whether to enable stale-while-revalidate */
  staleWhileRevalidate?: boolean;
  /** Dependencies that trigger refetch */
  deps?: unknown[];
  /** Whether to skip fetching */
  skip?: boolean;
}

interface UseCacheResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  isStale: boolean;
  refetch: () => Promise<void>;
}

export function useCache<T>({
  key,
  fetcher,
  ttl = 5 * 60 * 1000, // 5 minutes default
  staleWhileRevalidate = true,
  deps = [],
  skip = false,
}: UseCacheOptions<T>): UseCacheResult<T> {
  const [data, setData] = useState<T | null>(() => getCached<T>(key));
  const [loading, setLoading] = useState(!getCached<T>(key));
  const [error, setError] = useState<string | null>(null);
  const [isStale, setIsStale] = useState(false);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const refetch = useCallback(async () => {
    if (skip) return;
    
    const cached = getCached<T>(key);
    
    if (staleWhileRevalidate && cached) {
      // Show stale data immediately
      setData(cached);
      setIsStale(true);
      setLoading(false);
    } else if (!cached) {
      setLoading(true);
    }
    
    try {
      const freshData = await fetcherRef.current();
      setCache(key, freshData, ttl);
      setData(freshData);
      setIsStale(false);
      setError(null);
    } catch (err) {
      // If we have stale data, keep showing it
      if (!cached) {
        setError((err as Error).message);
      }
    } finally {
      setLoading(false);
    }
  }, [key, ttl, staleWhileRevalidate, skip]);

  useEffect(() => {
    const cached = getCached<T>(key);
    if (cached) {
      setData(cached);
      setLoading(false);
      
      // Check if we should revalidate in background
      if (staleWhileRevalidate) {
        refetch();
      }
    } else {
      refetch();
    }
  }, [key, ...deps]);

  return { data, loading, error, isStale, refetch };
}

// Cache TTL presets (in milliseconds)
export const CACHE_TTL = {
  SHORT: 1 * 60 * 1000,      // 1 minute - for frequently changing data
  MEDIUM: 5 * 60 * 1000,     // 5 minutes - default
  LONG: 15 * 60 * 1000,      // 15 minutes - for stable data
  VERY_LONG: 30 * 60 * 1000, // 30 minutes - for rarely changing data
};
