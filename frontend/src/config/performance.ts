/**
 * Performance configuration for the application
 */

export const PERFORMANCE_CONFIG = {
  // Image loading
  IMAGE_LAZY_LOAD_THRESHOLD: '150px', // Reduced from 200px for faster loading
  IMAGE_DECODE_ASYNC: true,           // Use async image decoding
  
  // Cache settings
  CACHE_MEMORY_ONLY_THRESHOLD: 2 * 60 * 1000, // Don't persist to sessionStorage if TTL < 2 minutes
  CACHE_MAX_MEMORY_SIZE: 200,                  // Max items in memory cache
  
  // API settings
  API_DEBOUNCE_MS: 300,               // Debounce API calls by 300ms
  API_REQUEST_DEDUP: true,            // Deduplicate concurrent identical requests
  MAX_CONCURRENT_REQUESTS: 6,         // Limit concurrent API requests
  REQUEST_BATCH_DELAY: 50,            // Batch requests within 50ms
  
  // Rendering
  VIRTUAL_SCROLL_THRESHOLD: 50,       // Use virtual scrolling for lists > 50 items
  INTERSECTION_OBSERVER_MARGIN: '50px', // Trigger intersection 50px early
  
  // Initial load optimization
  HOME_FEED_INITIAL_SIZE: 4,          // Reduced for faster initial load
  SECTION_INITIAL_SIZE: 6,            // Items per section
  
  // Video player
  VIDEO_PRELOAD: 'metadata' as const, // Only preload metadata, not full video
  VIDEO_BUFFER_AHEAD: 60,             // Buffer 60 seconds ahead
  VIDEO_BUFFER_BEHIND: 90,            // Keep 90 seconds behind
  
  // Auto-refresh
  HOME_REFRESH_COOLDOWN: 5 * 60 * 1000, // Don't auto-refresh home more than once per 5 minutes
  
  // Bundle optimization
  ROUTE_PREFETCH_DELAY: 2000,         // Prefetch route chunks after 2s idle
  
  // Performance thresholds
  LOW_FPS_THRESHOLD: 30,
  SCROLL_THROTTLE: 16,                // ~60fps
} as const;

export default PERFORMANCE_CONFIG;
