-- Add video_likes table if it doesn't exist
-- This migration is idempotent and safe to run multiple times

-- Create video_likes table
CREATE TABLE IF NOT EXISTS video_likes (
    id SERIAL PRIMARY KEY,
    video_code VARCHAR(50) NOT NULL REFERENCES videos(code) ON DELETE CASCADE,
    user_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_user_video_like UNIQUE (video_code, user_id)
);

-- Create indexes for performance (IF NOT EXISTS makes it safe)
CREATE INDEX IF NOT EXISTS idx_like_video ON video_likes(video_code);
CREATE INDEX IF NOT EXISTS idx_like_user ON video_likes(user_id);
CREATE INDEX IF NOT EXISTS idx_like_created ON video_likes(created_at);

-- Enable RLS
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename = 'video_likes' 
        AND rowsecurity = true
    ) THEN
        ALTER TABLE video_likes ENABLE ROW LEVEL SECURITY;
    END IF;
END $$;

-- Drop existing policies if they exist (to avoid conflicts)
DROP POLICY IF EXISTS "Anyone can view likes" ON video_likes;
DROP POLICY IF EXISTS "Users can insert their own likes" ON video_likes;
DROP POLICY IF EXISTS "Users can delete their own likes" ON video_likes;

-- Create RLS policies
CREATE POLICY "Anyone can view likes" ON video_likes
    FOR SELECT USING (true);

CREATE POLICY "Users can insert their own likes" ON video_likes
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Users can delete their own likes" ON video_likes
    FOR DELETE USING (true);

-- Create view for like counts (optional, for analytics)
CREATE OR REPLACE VIEW video_like_counts AS
SELECT 
    video_code,
    COUNT(*) as like_count
FROM video_likes
GROUP BY video_code;

-- Grant access to the view
GRANT SELECT ON video_like_counts TO anon, authenticated;
