/**
 * API client for backend communication with caching
 */

const API_BASE = import.meta.env.VITE_API_URL || '/api';

// ============================================
// Simple In-Memory Cache
// ============================================
interface CacheEntry<T> {
  data: T;
  timestamp: number;
  ttl: number;
}

class ApiCache {
  private cache = new Map<string, CacheEntry<unknown>>();
  private maxSize = 200;

  get<T>(key: string): T | null {
    const entry = this.cache.get(key) as CacheEntry<T> | undefined;
    if (!entry) return null;

    // Check if expired
    if (Date.now() - entry.timestamp > entry.ttl) {
      this.cache.delete(key);
      return null;
    }

    return entry.data;
  }

  set<T>(key: string, data: T, ttlMs: number = 60000): void {
    // Evict oldest if at capacity
    if (this.cache.size >= this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey) this.cache.delete(oldestKey);
    }

    this.cache.set(key, {
      data,
      timestamp: Date.now(),
      ttl: ttlMs,
    });
  }

  invalidate(pattern?: string): void {
    if (!pattern) {
      this.cache.clear();
      return;
    }
    for (const key of this.cache.keys()) {
      if (key.includes(pattern)) {
        this.cache.delete(key);
      }
    }
  }
}

const cache = new ApiCache();

// Cache TTLs in milliseconds
const TTL = {
  STATS: 60 * 1000,           // 1 minute
  VIDEO_LIST: 2 * 60 * 1000,  // 2 minutes
  VIDEO_DETAIL: 5 * 60 * 1000, // 5 minutes
  SEARCH: 3 * 60 * 1000,      // 3 minutes
  CATEGORIES: 5 * 60 * 1000,  // 5 minutes
  STUDIOS: 5 * 60 * 1000,     // 5 minutes
  CAST: 5 * 60 * 1000,        // 5 minutes
};

/**
 * Proxy external images through our backend to avoid tracking prevention
 */
export function proxyImageUrl(url: string | undefined | null): string {
  if (!url) return '';
  if (url.includes('pics.dmm.co.jp') || url.includes('dmm.co.jp')) {
    return `${API_BASE}/proxy/image?url=${encodeURIComponent(url)}`;
  }
  return url;
}

// Types
export interface VideoListItem {
  code: string;
  title: string;
  thumbnail_url: string;
  duration: string;
  release_date: string;
  studio: string;
  views: number;
  rating_avg: number;
  rating_count: number;
}

export interface VideoDetail extends VideoListItem {
  content_id: string;
  cover_url: string;
  series: string;
  description: string;
  embed_urls: string[];
  gallery_images: string[];
  categories: string[];
  cast: string[];
  cast_images: Record<string, string>;
  scraped_at: string;
  source_url: string;
  views: number;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface Stats {
  total_videos: number;
  categories_count: number;
  studios_count: number;
  cast_count: number;
  oldest_video: string | null;
  newest_video: string | null;
}

export interface Category {
  name: string;
  video_count: number;
}

export interface Studio {
  name: string;
  video_count: number;
}

export interface CastMember {
  name: string;
  video_count: number;
}

export interface CastWithImage {
  name: string;
  image_url: string | null;
  video_count: number;
}

export interface HomeFeedResponse {
  featured: VideoListItem[];
  trending: VideoListItem[];
  popular: VideoListItem[];
  top_rated: VideoListItem[];
  new_releases: VideoListItem[];
  classics: VideoListItem[];
}

export interface Comment {
  id: number;
  video_code: string;
  user_id: string;
  username: string;
  parent_id: number | null;
  content: string;
  created_at: string;
  updated_at: string | null;
  is_deleted: boolean;
  score: number;
  vote_count: number;
  user_vote: number;
  replies: Comment[];
}

// Cached fetch helper
async function fetchWithCache<T>(
  endpoint: string,
  ttl: number,
  cacheKey?: string
): Promise<T> {
  const key = cacheKey || endpoint;

  // Check cache first
  const cached = cache.get<T>(key);
  if (cached !== null) {
    return cached;
  }

  // Fetch from API
  const res = await fetch(`${API_BASE}${endpoint}`);
  if (!res.ok) {
    throw new Error(`API error: ${res.status}`);
  }

  const data = await res.json();

  // Cache the result
  cache.set(key, data, ttl);

  return data;
}

export const api = {
  // Stats
  getStats: () => fetchWithCache<Stats>('/stats', TTL.STATS),

  // Videos
  getVideos: (page = 1, pageSize = 20, sortBy = 'release_date', sortOrder = 'desc') =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos?page=${page}&page_size=${pageSize}&sort_by=${sortBy}&sort_order=${sortOrder}`,
      TTL.VIDEO_LIST,
      `videos:${page}:${pageSize}:${sortBy}:${sortOrder}`
    ),

  searchVideos: (query: string, page = 1, pageSize = 20) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/search?q=${encodeURIComponent(query)}&page=${page}&page_size=${pageSize}`,
      TTL.SEARCH,
      `search:${query.toLowerCase()}:${page}:${pageSize}`
    ),

  advancedSearch: (params: {
    q?: string;
    category?: string;
    studio?: string;
    cast?: string;
    series?: string;
    dateFrom?: string;
    dateTo?: string;
    minRating?: number;
    sortBy?: 'relevance' | 'date' | 'rating' | 'views' | 'title';
    sortOrder?: 'asc' | 'desc';
    page?: number;
    pageSize?: number;
  }) => {
    const searchParams = new URLSearchParams();
    if (params.q) searchParams.set('q', params.q);
    if (params.category) searchParams.set('category', params.category);
    if (params.studio) searchParams.set('studio', params.studio);
    if (params.cast) searchParams.set('cast', params.cast);
    if (params.series) searchParams.set('series', params.series);
    if (params.dateFrom) searchParams.set('date_from', params.dateFrom);
    if (params.dateTo) searchParams.set('date_to', params.dateTo);
    if (params.minRating !== undefined) searchParams.set('min_rating', params.minRating.toString());
    if (params.sortBy) searchParams.set('sort_by', params.sortBy);
    if (params.sortOrder) searchParams.set('sort_order', params.sortOrder);
    searchParams.set('page', (params.page || 1).toString());
    searchParams.set('page_size', (params.pageSize || 20).toString());

    const queryString = searchParams.toString();
    return fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/search/advanced?${queryString}`,
      TTL.SEARCH,
      `adv_search:${queryString}`
    );
  },

  getSearchSuggestions: async (query: string, limit = 10) => {
    if (query.length < 2) return { suggestions: [] };
    const res = await fetch(
      `${API_BASE}/videos/search/suggestions?q=${encodeURIComponent(query)}&limit=${limit}`
    );
    if (!res.ok) throw new Error('Failed to get suggestions');
    return res.json() as Promise<{
      suggestions: Array<{
        type: 'video' | 'cast' | 'studio' | 'category' | 'series';
        value: string;
        label: string;
        priority: number;
      }>;
    }>;
  },

  getSearchFacets: (query?: string) => {
    const params = query ? `?q=${encodeURIComponent(query)}` : '';
    return fetchWithCache<{
      categories: Array<{ name: string; count: number }>;
      studios: Array<{ name: string; count: number }>;
      cast: Array<{ name: string; count: number }>;
      years: Array<{ year: string; count: number }>;
    }>(`/videos/search/facets${params}`, TTL.SEARCH, `facets:${query || ''}`);
  },

  getVideo: (code: string) =>
    fetchWithCache<VideoDetail>(
      `/videos/${encodeURIComponent(code)}`,
      TTL.VIDEO_DETAIL,
      `video:${code.toUpperCase()}`
    ),

  getRelatedVideos: (code: string, userId?: string, limit = 12, strategy = 'balanced') => {
    const params = new URLSearchParams({ limit: limit.toString(), strategy });
    if (userId) params.set('user_id', userId);
    return fetchWithCache<VideoListItem[]>(
      `/videos/${encodeURIComponent(code)}/related?${params}`,
      TTL.VIDEO_LIST,
      `related:${code.toUpperCase()}:${userId || 'anon'}:${limit}:${strategy}`
    );
  },

  getDiscoverMore: async (userId: string, batch = 0, batchSize = 12, seenCodes: string[] = []) => {
    const params = new URLSearchParams({
      user_id: userId,
      batch: batch.toString(),
      batch_size: batchSize.toString(),
      seen: seenCodes.join(',')
    });
    const res = await fetch(`${API_BASE}/videos/user/discover?${params}`);
    if (!res.ok) throw new Error('Failed to get recommendations');
    return res.json() as Promise<{
      items: VideoListItem[];
      has_more: boolean;
      batch: number;
      strategy: string;
      seed_video: string;
    }>;
  },

  recordWatch: async (code: string, userId: string, duration = 0, completed = false) => {
    const params = new URLSearchParams({
      user_id: userId,
      duration: duration.toString(),
      completed: completed.toString()
    });
    try {
      const res = await fetch(
        `${API_BASE}/videos/${encodeURIComponent(code)}/watch?${params}`,
        { method: 'POST' }
      );
      if (!res.ok) return { success: false }; // Silently fail
      return res.json();
    } catch {
      return { success: false }; // Silently fail on network errors too
    }
  },

  getRandomVideoCode: async (exclude: string[] = []) => {
    const params = exclude.length > 0
      ? `?exclude=${encodeURIComponent(exclude.join(','))}`
      : '';
    const res = await fetch(`${API_BASE}/videos/random${params}`);
    if (!res.ok) throw new Error('Failed to get random video');
    return res.json() as Promise<{ code: string }>;
  },

  // Homepage Category Endpoints
  getHomeFeed: (userId: string) =>
    fetchWithCache<HomeFeedResponse>(
      `/videos/feed/home?user_id=${encodeURIComponent(userId)}`,
      TTL.VIDEO_LIST,
      `home-feed:${userId}`
    ),

  // Direct fetch version for use with useCachedApi hook
  getHomeFeedDirect: async (userId: string): Promise<HomeFeedResponse> => {
    const res = await fetch(`${API_BASE}/videos/feed/home?user_id=${encodeURIComponent(userId)}`);
    if (!res.ok) throw new Error('Failed to fetch home feed');
    return res.json();
  },

  getTrendingVideos: (page = 1, pageSize = 10) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/trending?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `trending:${page}:${pageSize}`
    ),

  getPopularVideos: (page = 1, pageSize = 10) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/popular?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `popular:${page}:${pageSize}`
    ),

  getTopRatedVideos: (page = 1, pageSize = 10) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/top-rated?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `top-rated:${page}:${pageSize}`
    ),

  getFeaturedVideos: (page = 1, pageSize = 10) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/featured?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `featured:${page}:${pageSize}`
    ),

  getNewReleases: (page = 1, pageSize = 10) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/new-releases?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `new-releases:${page}:${pageSize}`
    ),

  getClassics: (page = 1, pageSize = 10) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/classics?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `classics:${page}:${pageSize}`
    ),

  // Categories
  getCategories: () =>
    fetchWithCache<Category[]>('/categories', TTL.CATEGORIES),

  getVideosByCategory: (category: string, page = 1, pageSize = 20) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/categories/${encodeURIComponent(category)}/videos?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `cat:${category}:${page}:${pageSize}`
    ),

  // Studios
  getStudios: () =>
    fetchWithCache<Studio[]>('/studios', TTL.STUDIOS),

  getVideosByStudio: (studio: string, page = 1, pageSize = 20) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/studios/${encodeURIComponent(studio)}/videos?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `studio:${studio}:${page}:${pageSize}`
    ),

  // Series
  getSeries: () =>
    fetchWithCache<{ name: string; count: number }[]>('/series', TTL.STUDIOS),

  getVideosBySeries: (series: string, page = 1, pageSize = 20) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/series/${encodeURIComponent(series)}/videos?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `series:${series}:${page}:${pageSize}`
    ),

  // Cast
  getCast: () =>
    fetchWithCache<CastMember[]>('/cast', TTL.CAST),

  getAllCastWithImages: () =>
    fetchWithCache<CastWithImage[]>('/cast/all', TTL.CAST, 'cast:all'),

  // Direct fetch version for use with useCachedApi hook
  getAllCastWithImagesDirect: async (): Promise<CastWithImage[]> => {
    const res = await fetch(`${API_BASE}/cast/all`);
    if (!res.ok) throw new Error('Failed to fetch all cast');
    return res.json();
  },

  getFeaturedCast: (limit = 20) =>
    fetchWithCache<CastWithImage[]>(
      `/cast/featured?limit=${limit}`,
      TTL.CAST,
      `cast:featured:${limit}`
    ),

  // Direct fetch version for use with useCachedApi hook (no double caching)
  getFeaturedCastDirect: async (limit = 20): Promise<CastWithImage[]> => {
    const res = await fetch(`${API_BASE}/cast/featured?limit=${limit}`);
    if (!res.ok) throw new Error('Failed to fetch featured cast');
    return res.json();
  },

  getVideosByCast: (name: string, page = 1, pageSize = 20) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/cast/${encodeURIComponent(name)}/videos?page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `cast:${name}:${page}:${pageSize}`
    ),

  // Increment view count (no caching, invalidates video cache)
  incrementView: async (code: string) => {
    const res = await fetch(`${API_BASE}/videos/${encodeURIComponent(code)}/view`, {
      method: 'POST'
    });
    // Invalidate this video's cache
    cache.invalidate(`video:${code.toUpperCase()}`);
    return res.json();
  },

  // Rating system
  getRating: async (code: string, userId?: string) => {
    const params = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const res = await fetch(`${API_BASE}/videos/${encodeURIComponent(code)}/rating${params}`);
    if (!res.ok) throw new Error('Failed to get rating');
    return res.json() as Promise<{
      average: number;
      count: number;
      distribution: Record<number, number>;
      user_rating?: number;
    }>;
  },

  setRating: async (code: string, userId: string, rating: number) => {
    const res = await fetch(
      `${API_BASE}/videos/${encodeURIComponent(code)}/rating?rating=${rating}&user_id=${encodeURIComponent(userId)}`,
      { method: 'POST' }
    );
    if (!res.ok) throw new Error('Failed to set rating');
    return res.json();
  },

  deleteRating: async (code: string, userId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/${encodeURIComponent(code)}/rating?user_id=${encodeURIComponent(userId)}`,
      { method: 'DELETE' }
    );
    if (!res.ok) throw new Error('Failed to delete rating');
    return res.json();
  },

  // Bookmark system
  isBookmarked: async (code: string, userId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/${encodeURIComponent(code)}/bookmark?user_id=${encodeURIComponent(userId)}`
    );
    if (!res.ok) throw new Error('Failed to check bookmark');
    return res.json() as Promise<{ bookmarked: boolean }>;
  },

  addBookmark: async (code: string, userId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/${encodeURIComponent(code)}/bookmark?user_id=${encodeURIComponent(userId)}`,
      { method: 'POST' }
    );
    if (!res.ok) throw new Error('Failed to add bookmark');
    return res.json() as Promise<{ success: boolean; added: boolean }>;
  },

  removeBookmark: async (code: string, userId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/${encodeURIComponent(code)}/bookmark?user_id=${encodeURIComponent(userId)}`,
      { method: 'DELETE' }
    );
    if (!res.ok) throw new Error('Failed to remove bookmark');
    return res.json();
  },

  getBookmarks: async (userId: string, page = 1, pageSize = 20) => {
    const res = await fetch(
      `${API_BASE}/videos/user/bookmarks?user_id=${encodeURIComponent(userId)}&page=${page}&page_size=${pageSize}`
    );
    if (!res.ok) throw new Error('Failed to get bookmarks');
    return res.json() as Promise<PaginatedResponse<VideoListItem>>;
  },

  getForYou: (userId: string, page = 1, pageSize = 12) =>
    fetchWithCache<PaginatedResponse<VideoListItem>>(
      `/videos/user/for-you?user_id=${encodeURIComponent(userId)}&page=${page}&page_size=${pageSize}`,
      TTL.VIDEO_LIST,
      `for-you:${userId}:${page}:${pageSize}`
    ),

  // Direct fetch version for use with useCachedApi hook
  getForYouDirect: async (userId: string, page = 1, pageSize = 12): Promise<PaginatedResponse<VideoListItem>> => {
    const res = await fetch(
      `${API_BASE}/videos/user/for-you?user_id=${encodeURIComponent(userId)}&page=${page}&page_size=${pageSize}`
    );
    if (!res.ok) throw new Error('Failed to fetch personalized recommendations');
    return res.json();
  },

  getBookmarkCount: async (userId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/user/bookmarks/count?user_id=${encodeURIComponent(userId)}`
    );
    if (!res.ok) throw new Error('Failed to get bookmark count');
    return res.json() as Promise<{ count: number }>;
  },

  getWatchHistory: async (userId: string, page = 1, pageSize = 20) => {
    const res = await fetch(
      `${API_BASE}/videos/user/history?user_id=${encodeURIComponent(userId)}&page=${page}&page_size=${pageSize}`
    );
    if (!res.ok) throw new Error('Failed to get watch history');
    return res.json() as Promise<PaginatedResponse<VideoListItem>>;
  },

  clearWatchHistory: async (userId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/user/history?user_id=${encodeURIComponent(userId)}`,
      { method: 'DELETE' }
    );
    if (!res.ok) throw new Error('Failed to clear watch history');
    return res.json();
  },

  mergeHistory: async (fromUserId: string, toUserId: string) => {
    const res = await fetch(
      `${API_BASE}/videos/user/merge-history?from_user_id=${encodeURIComponent(fromUserId)}&to_user_id=${encodeURIComponent(toUserId)}`,
      { method: 'POST' }
    );
    if (!res.ok) throw new Error('Failed to merge history');
    return res.json() as Promise<{ merged_history: number; merged_ratings: number; total_merged: number }>;
  },

  // Comments
  getComments: async (videoCode: string, userId?: string, sort = 'best') => {
    const params = new URLSearchParams({ sort });
    if (userId) params.set('user_id', userId);
    const res = await fetch(`${API_BASE}/comments/${encodeURIComponent(videoCode)}?${params}`);
    if (!res.ok) throw new Error('Failed to get comments');
    return res.json() as Promise<{ comments: Comment[]; count: number }>;
  },

  createComment: async (videoCode: string, userId: string, content: string, username?: string, parentId?: number) => {
    const params = new URLSearchParams({ user_id: userId });
    if (username) params.set('username', username);
    const res = await fetch(`${API_BASE}/comments/${encodeURIComponent(videoCode)}?${params}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content, parent_id: parentId })
    });
    if (!res.ok) throw new Error('Failed to create comment');
    return res.json() as Promise<Comment>;
  },

  updateComment: async (commentId: number, userId: string, content: string) => {
    const res = await fetch(`${API_BASE}/comments/${commentId}?user_id=${encodeURIComponent(userId)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content })
    });
    if (!res.ok) throw new Error('Failed to update comment');
    return res.json() as Promise<Comment>;
  },

  deleteComment: async (commentId: number, userId: string) => {
    const res = await fetch(`${API_BASE}/comments/${commentId}?user_id=${encodeURIComponent(userId)}`, {
      method: 'DELETE'
    });
    if (!res.ok) throw new Error('Failed to delete comment');
    return res.json();
  },

  voteComment: async (commentId: number, userId: string, vote: number) => {
    const res = await fetch(
      `${API_BASE}/comments/${commentId}/vote?user_id=${encodeURIComponent(userId)}&vote=${vote}`,
      { method: 'POST' }
    );
    if (!res.ok) throw new Error('Failed to vote');
    return res.json() as Promise<{ comment_id: number; score: number; vote_count: number; user_vote: number }>;
  },

  // Cache management
  clearCache: () => cache.invalidate(),
};
