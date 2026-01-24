-- Migration to add RPC functions for better performance and atomic operations

-- 1. Atomic increment for views
CREATE OR REPLACE FUNCTION increment_views(video_code VARCHAR)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE videos
  SET views = COALESCE(views, 0) + 1
  WHERE code = video_code;

  RETURN FOUND;
END;
$$;

-- 2. Efficient Most Liked Videos (Aggregation)
CREATE OR REPLACE FUNCTION get_most_liked_videos(limit_count INTEGER DEFAULT 10)
RETURNS TABLE (
    code VARCHAR,
    like_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT video_code, COUNT(*) as count
    FROM video_likes
    GROUP BY video_code
    ORDER BY count DESC
    LIMIT limit_count;
END;
$$;

-- 3. Efficient Top Rated Videos (Aggregation)
CREATE OR REPLACE FUNCTION get_top_rated_videos(min_votes INTEGER DEFAULT 5, limit_count INTEGER DEFAULT 10)
RETURNS TABLE (
    code VARCHAR,
    rating_avg NUMERIC,
    rating_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT
        video_code,
        ROUND(AVG(rating)::numeric, 1) as avg_rating,
        COUNT(*) as count
    FROM video_ratings
    GROUP BY video_code
    HAVING COUNT(*) >= min_votes
    ORDER BY avg_rating DESC, count DESC
    LIMIT limit_count;
END;
$$;
