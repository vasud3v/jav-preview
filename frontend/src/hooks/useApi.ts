/**
 * React hooks for API data fetching with caching support
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import { getCached, setCache, CACHE_TTL } from './useCache';

interface UseApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

export function useApi<T>(fetcher: () => Promise<T>, deps: unknown[] = []) {
  const [state, setState] = useState<UseApiState<T>>({
    data: null,
    loading: true,
    error: null,
  });

  const refetch = useCallback(async () => {
    setState(prev => ({ ...prev, loading: true, error: null }));
    try {
      const data = await fetcher();
      setState({ data, loading: false, error: null });
    } catch (err) {
      setState({ data: null, loading: false, error: (err as Error).message });
    }
  }, [fetcher]);

  useEffect(() => {
    refetch();
  }, deps);

  return { ...state, refetch };
}

interface UseCachedApiOptions {
  /** Cache key - must be unique for this data */
  cacheKey: string;
  /** Time to live in milliseconds (default: 5 minutes) */
  ttl?: number;
  /** Show stale data while revalidating (default: true) */
  staleWhileRevalidate?: boolean;
  /** Skip fetching (useful for conditional fetches) */
  skip?: boolean;
}

interface UseCachedApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  isStale: boolean;
  refetch: () => Promise<void>;
}

/**
 * Enhanced API hook with persistent caching and stale-while-revalidate
 */
export function useCachedApi<T>(
  fetcher: () => Promise<T>,
  options: UseCachedApiOptions,
  deps: unknown[] = []
): UseCachedApiResult<T> {
  const { cacheKey, ttl = CACHE_TTL.MEDIUM, staleWhileRevalidate = true, skip = false } = options;
  
  // Initialize with cached data if available
  const [data, setData] = useState<T | null>(() => getCached<T>(cacheKey));
  const [loading, setLoading] = useState(!getCached<T>(cacheKey));
  const [error, setError] = useState<string | null>(null);
  const [isStale, setIsStale] = useState(false);
  
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const isMountedRef = useRef(true);

  const refetch = useCallback(async () => {
    if (skip) return;
    
    const cached = getCached<T>(cacheKey);
    
    if (staleWhileRevalidate && cached) {
      // Show stale data immediately while fetching fresh
      setData(cached);
      setIsStale(true);
      setLoading(false);
    } else if (!cached) {
      setLoading(true);
    }
    
    try {
      const freshData = await fetcherRef.current();
      if (isMountedRef.current) {
        setCache(cacheKey, freshData, ttl);
        setData(freshData);
        setIsStale(false);
        setError(null);
        setLoading(false);
      }
    } catch (err) {
      console.error('API fetch error:', err);
      if (isMountedRef.current) {
        // If we have stale data, keep showing it with error indicator
        if (!cached) {
          setData(null);
          setError((err as Error).message || 'Failed to fetch data');
        } else {
          // Keep stale data but show error
          setError((err as Error).message || 'Failed to refresh data');
        }
        setIsStale(false);
        setLoading(false);
      }
    }
  }, [cacheKey, ttl, staleWhileRevalidate, skip]);

  useEffect(() => {
    isMountedRef.current = true;
    
    const cached = getCached<T>(cacheKey);
    if (cached) {
      setData(cached);
      setLoading(false);
      
      // Revalidate in background if enabled
      if (staleWhileRevalidate) {
        refetch();
      }
    } else if (!skip) {
      refetch();
    }
    
    return () => {
      isMountedRef.current = false;
    };
  }, [cacheKey, ...deps]);

  return { data, loading, error, isStale, refetch };
}

// Re-export cache utilities for convenience
export { getCached, setCache, invalidateCache, CACHE_TTL } from './useCache';
