-- Fix RLS policies to allow anonymous users to rate videos
-- Anonymous users have user_id like "anon_xxxxx" but no auth.uid()

-- Drop existing policies
DROP POLICY IF EXISTS "Users can insert own ratings" ON video_ratings;
DROP POLICY IF EXISTS "Users can update own ratings" ON video_ratings;
DROP POLICY IF EXISTS "Users can delete own ratings" ON video_ratings;

-- Create new policies that allow anonymous users
-- For INSERT: Allow if user is authenticated and matches, OR if user is anonymous (user_id starts with 'anon_')
CREATE POLICY "Users can insert own ratings" ON video_ratings 
FOR INSERT 
WITH CHECK (
    auth.uid()::text = user_id 
    OR user_id LIKE 'anon_%'
);

-- For UPDATE: Allow if user is authenticated and matches, OR if user is anonymous (user_id starts with 'anon_')
CREATE POLICY "Users can update own ratings" ON video_ratings 
FOR UPDATE 
USING (
    auth.uid()::text = user_id 
    OR user_id LIKE 'anon_%'
);

-- For DELETE: Allow if user is authenticated and matches, OR if user is anonymous (user_id starts with 'anon_')
CREATE POLICY "Users can delete own ratings" ON video_ratings 
FOR DELETE 
USING (
    auth.uid()::text = user_id 
    OR user_id LIKE 'anon_%'
);
