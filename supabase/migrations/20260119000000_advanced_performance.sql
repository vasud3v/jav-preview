-- Advanced Performance Optimizations
-- This migration adds comprehensive indexes and optimizations without changing functionality

-- ============================================
-- VIDEO TABLE OPTIMIZATIONS
-- ============================================

-- Composite index for Featured section (thumbnail + cover + scraped_at)
CREATE INDEX IF NOT EXISTS idx_videos_featured_quality 
ON videos(scraped_at DESC, views DESC) 
WHERE thumbnail_url IS NOT NULL AND thumbnail_url != '' 
  AND cover_url IS NOT NULL AND cover_url != '';

-- Composite index for Trending (scraped_at + views for recent popular content)
-- Note: No date filter in index - filter applied in query instead
CREATE INDEX IF NOT EXISTS idx_videos_trending 
ON videos(scraped_at DESC, views DESC);

-- Composite index for New Releases (release_date + views)
-- Note: No date filter in index - filter applied in query instead
CREATE INDEX IF NOT EXISTS idx_videos_new_releases 
ON videos(release_date DESC, views DESC);

-- Composite index for Classics (old videos + high views)
-- Note: No date filter in index - filter applied in query instead
CREATE INDEX IF NOT EXISTS idx_videos_classics 
ON videos(release_date ASC, views DESC);

-- Index for code lookups (used frequently)
CREATE INDEX IF NOT EXISTS idx_videos_code_lookup ON videos(code);

-- Index for studio filtering
CREATE INDEX IF NOT EXISTS idx_videos_studio ON videos(studio) WHERE studio IS NOT NULL;

-- ============================================
-- RATINGS TABLE OPTIMIZATIONS
-- ============================================

-- Composite index for rating aggregations (video_code + rating)
CREATE INDEX IF NOT EXISTS idx_video_ratings_aggregation 
ON video_ratings(video_code, rating);

-- Index for user rating lookups
CREATE INDEX IF NOT EXISTS idx_video_ratings_user_lookup 
ON video_ratings(user_id, video_code);

-- ============================================
-- LIKES TABLE OPTIMIZATIONS
-- ============================================

-- Composite index for like counting (video_code for grouping)
CREATE INDEX IF NOT EXISTS idx_video_likes_counting 
ON video_likes(video_code, created_at DESC);

-- ============================================
-- ANALYZE TABLES FOR QUERY PLANNER
-- ============================================

-- Update statistics for better query planning
ANALYZE videos;
ANALYZE video_ratings;
ANALYZE video_likes;
ANALYZE video_cast;
ANALYZE cast_members;
ANALYZE categories;
ANALYZE video_categories;

-- Note: VACUUM cannot run in a transaction block (migration context)
-- Run these manually if needed:
-- VACUUM ANALYZE videos;
-- VACUUM ANALYZE video_ratings;
-- VACUUM ANALYZE video_likes;

