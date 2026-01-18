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
