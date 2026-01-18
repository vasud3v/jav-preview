-- Add missing indexes for performance optimization

-- Index for trending/recent queries
CREATE INDEX IF NOT EXISTS idx_videos_scraped_at ON videos(scraped_at DESC);

-- Index for popular/top-rated queries
CREATE INDEX IF NOT EXISTS idx_videos_views ON videos(views DESC);

-- Partial indexes for Featured section (checking non-empty fields)
CREATE INDEX IF NOT EXISTS idx_videos_featured_candidates 
ON videos(views DESC) 
WHERE thumbnail_url IS NOT NULL AND thumbnail_url != '' 
  AND description IS NOT NULL AND description != '';

-- Index for New Releases
CREATE INDEX IF NOT EXISTS idx_videos_release_date_desc ON videos(release_date DESC);

-- Indexes for like system (feed algorithm integration)
CREATE INDEX IF NOT EXISTS idx_video_likes_video_code ON video_likes(video_code);
CREATE INDEX IF NOT EXISTS idx_video_likes_created_at ON video_likes(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_video_likes_user_video ON video_likes(user_id, video_code);
