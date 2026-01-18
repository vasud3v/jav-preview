-- Create video_likes table for Instagram-style likes
CREATE TABLE IF NOT EXISTS video_likes (
    id SERIAL PRIMARY KEY,
    video_code VARCHAR(50) NOT NULL REFERENCES videos(code) ON DELETE CASCADE,
    user_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_user_video_like UNIQUE (video_code, user_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_like_video ON video_likes(video_code);
CREATE INDEX IF NOT EXISTS idx_like_user ON video_likes(user_id);
CREATE INDEX IF NOT EXISTS idx_like_created ON video_likes(created_at);

-- Add RLS policies for video_likes
ALTER TABLE video_likes ENABLE ROW LEVEL SECURITY;

-- Allow anyone to read likes
CREATE POLICY "Anyone can view likes" ON video_likes
    FOR SELECT USING (true);

-- Allow authenticated and anonymous users to insert their own likes
CREATE POLICY "Users can insert their own likes" ON video_likes
    FOR INSERT WITH CHECK (true);

-- Allow users to delete their own likes
CREATE POLICY "Users can delete their own likes" ON video_likes
    FOR DELETE USING (true);

-- Create a view for like counts per video (optional, for analytics)
CREATE OR REPLACE VIEW video_like_counts AS
SELECT 
    video_code,
    COUNT(*) as like_count
FROM video_likes
GROUP BY video_code;

-- Grant access to the view
GRANT SELECT ON video_like_counts TO anon, authenticated;
