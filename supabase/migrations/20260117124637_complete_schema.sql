-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create categories table
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE INDEX idx_category_name ON categories(name);

-- Create cast_members table
CREATE TABLE cast_members (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) UNIQUE NOT NULL
);

CREATE INDEX idx_cast_name ON cast_members(name);

-- Create videos table
CREATE TABLE videos (
    code VARCHAR(50) PRIMARY KEY,
    content_id VARCHAR(100),
    title VARCHAR(500) NOT NULL,
    duration VARCHAR(20),
    release_date TIMESTAMP,
    thumbnail_url VARCHAR(500),
    cover_url VARCHAR(500),
    studio VARCHAR(200),
    series VARCHAR(200),
    description TEXT,
    scraped_at TIMESTAMP DEFAULT NOW(),
    source_url VARCHAR(500),
    views INTEGER DEFAULT 0,
    embed_urls JSONB DEFAULT '[]'::jsonb,
    gallery_images JSONB DEFAULT '[]'::jsonb,
    cast_images JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_video_studio ON videos(studio);
CREATE INDEX idx_video_release_date ON videos(release_date);

-- Create video_categories junction table
CREATE TABLE video_categories (
    video_code VARCHAR(50) REFERENCES videos(code) ON DELETE CASCADE,
    category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE,
    PRIMARY KEY (video_code, category_id)
);

-- Create video_cast junction table
CREATE TABLE video_cast (
    video_code VARCHAR(50) REFERENCES videos(code) ON DELETE CASCADE,
    cast_id INTEGER REFERENCES cast_members(id) ON DELETE CASCADE,
    PRIMARY KEY (video_code, cast_id)
);

-- Create video_ratings table
CREATE TABLE video_ratings (
    id SERIAL PRIMARY KEY,
    video_code VARCHAR(50) REFERENCES videos(code) ON DELETE CASCADE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (video_code, user_id)
);

CREATE INDEX idx_rating_video ON video_ratings(video_code);
CREATE INDEX idx_rating_user ON video_ratings(user_id);

-- Create video_bookmarks table
CREATE TABLE video_bookmarks (
    id SERIAL PRIMARY KEY,
    video_code VARCHAR(50) REFERENCES videos(code) ON DELETE CASCADE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (video_code, user_id)
);

CREATE INDEX idx_bookmark_video ON video_bookmarks(video_code);
CREATE INDEX idx_bookmark_user ON video_bookmarks(user_id);
CREATE INDEX idx_bookmark_user_created ON video_bookmarks(user_id, created_at);

-- Create watch_history table
CREATE TABLE watch_history (
    id SERIAL PRIMARY KEY,
    video_code VARCHAR(50) REFERENCES videos(code) ON DELETE CASCADE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    watched_at TIMESTAMP DEFAULT NOW(),
    watch_duration INTEGER DEFAULT 0,
    completed INTEGER DEFAULT 0
);

CREATE INDEX idx_watch_video ON watch_history(video_code);
CREATE INDEX idx_watch_user ON watch_history(user_id);
CREATE INDEX idx_watch_user_time ON watch_history(user_id, watched_at);

-- Create comments table
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    video_code VARCHAR(50) REFERENCES videos(code) ON DELETE CASCADE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    username VARCHAR(100),
    parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX idx_comment_video ON comments(video_code);
CREATE INDEX idx_comment_user ON comments(user_id);
CREATE INDEX idx_comment_parent ON comments(parent_id);
CREATE INDEX idx_comment_created ON comments(created_at);

-- Create comment_votes table
CREATE TABLE comment_votes (
    id SERIAL PRIMARY KEY,
    comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    vote INTEGER NOT NULL CHECK (vote IN (-1, 1)),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (comment_id, user_id)
);

CREATE INDEX idx_vote_comment ON comment_votes(comment_id);
CREATE INDEX idx_vote_user ON comment_votes(user_id);

-- Scraper state tables
CREATE TABLE scraper_progress (
    id SERIAL PRIMARY KEY,
    start_page INTEGER NOT NULL,
    end_page INTEGER NOT NULL,
    current_page INTEGER NOT NULL,
    total_videos INTEGER DEFAULT 0,
    successful_videos INTEGER DEFAULT 0,
    failed_videos INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX idx_progress_active ON scraper_progress(is_active);

CREATE TABLE scraper_completed (
    id SERIAL PRIMARY KEY,
    progress_id INTEGER REFERENCES scraper_progress(id) ON DELETE CASCADE,
    code VARCHAR(50) NOT NULL,
    completed_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (progress_id, code)
);

CREATE INDEX idx_completed_progress ON scraper_completed(progress_id);
CREATE INDEX idx_completed_code ON scraper_completed(code);

CREATE TABLE scraper_pending (
    id SERIAL PRIMARY KEY,
    progress_id INTEGER REFERENCES scraper_progress(id) ON DELETE CASCADE,
    code VARCHAR(50) NOT NULL,
    added_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (progress_id, code)
);

CREATE INDEX idx_pending_progress ON scraper_pending(progress_id);
CREATE INDEX idx_pending_code ON scraper_pending(code);

CREATE TABLE scraper_failed (
    code VARCHAR(50) PRIMARY KEY,
    error_message TEXT,
    last_attempt TIMESTAMP DEFAULT NOW(),
    attempt_count INTEGER DEFAULT 1
);

CREATE INDEX idx_failed_code ON scraper_failed(code);
CREATE INDEX idx_failed_last_attempt ON scraper_failed(last_attempt);

CREATE TABLE scraper_random_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    state BYTEA NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW(),
    CHECK (id = 1)
);

-- Enable Row Level Security on all tables
ALTER TABLE videos ENABLE ROW LEVEL SECURITY;
ALTER TABLE categories ENABLE ROW LEVEL SECURITY;
ALTER TABLE cast_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE video_categories ENABLE ROW LEVEL SECURITY;
ALTER TABLE video_cast ENABLE ROW LEVEL SECURITY;
ALTER TABLE video_ratings ENABLE ROW LEVEL SECURITY;
ALTER TABLE video_bookmarks ENABLE ROW LEVEL SECURITY;
ALTER TABLE watch_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE comment_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper_progress ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper_completed ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper_pending ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper_failed ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper_random_state ENABLE ROW LEVEL SECURITY;

-- Public read access for videos and related data
CREATE POLICY "Public videos read" ON videos FOR SELECT USING (true);
CREATE POLICY "Public categories read" ON categories FOR SELECT USING (true);
CREATE POLICY "Public cast read" ON cast_members FOR SELECT USING (true);
CREATE POLICY "Public video_categories read" ON video_categories FOR SELECT USING (true);
CREATE POLICY "Public video_cast read" ON video_cast FOR SELECT USING (true);

-- Service role can write videos (for scraper)
CREATE POLICY "Service role can insert videos" ON videos FOR INSERT WITH CHECK (true);
CREATE POLICY "Service role can update videos" ON videos FOR UPDATE USING (true);
CREATE POLICY "Service role can delete videos" ON videos FOR DELETE USING (true);

CREATE POLICY "Service role can insert categories" ON categories FOR INSERT WITH CHECK (true);
CREATE POLICY "Service role can insert cast" ON cast_members FOR INSERT WITH CHECK (true);
CREATE POLICY "Service role can insert video_categories" ON video_categories FOR INSERT WITH CHECK (true);
CREATE POLICY "Service role can insert video_cast" ON video_cast FOR INSERT WITH CHECK (true);

-- User-specific policies for ratings
CREATE POLICY "Users can view all ratings" ON video_ratings FOR SELECT USING (true);
CREATE POLICY "Users can insert own ratings" ON video_ratings FOR INSERT WITH CHECK (auth.uid()::text = user_id);
CREATE POLICY "Users can update own ratings" ON video_ratings FOR UPDATE USING (auth.uid()::text = user_id);
CREATE POLICY "Users can delete own ratings" ON video_ratings FOR DELETE USING (auth.uid()::text = user_id);

-- User-specific policies for bookmarks
CREATE POLICY "Users can view own bookmarks" ON video_bookmarks FOR SELECT USING (auth.uid()::text = user_id);
CREATE POLICY "Users can insert own bookmarks" ON video_bookmarks FOR INSERT WITH CHECK (auth.uid()::text = user_id);
CREATE POLICY "Users can delete own bookmarks" ON video_bookmarks FOR DELETE USING (auth.uid()::text = user_id);

-- User-specific policies for watch history
CREATE POLICY "Users can view own history" ON watch_history FOR SELECT USING (auth.uid()::text = user_id);
CREATE POLICY "Users can insert own history" ON watch_history FOR INSERT WITH CHECK (auth.uid()::text = user_id);
CREATE POLICY "Users can update own history" ON watch_history FOR UPDATE USING (auth.uid()::text = user_id);

-- Comment policies
CREATE POLICY "Users can view all comments" ON comments FOR SELECT USING (true);
CREATE POLICY "Authenticated users can insert comments" ON comments FOR INSERT WITH CHECK (auth.uid()::text = user_id);
CREATE POLICY "Users can update own comments" ON comments FOR UPDATE USING (auth.uid()::text = user_id);
CREATE POLICY "Users can delete own comments" ON comments FOR DELETE USING (auth.uid()::text = user_id);

-- Comment vote policies
CREATE POLICY "Users can view all votes" ON comment_votes FOR SELECT USING (true);
CREATE POLICY "Users can insert own votes" ON comment_votes FOR INSERT WITH CHECK (auth.uid()::text = user_id);
CREATE POLICY "Users can update own votes" ON comment_votes FOR UPDATE USING (auth.uid()::text = user_id);
CREATE POLICY "Users can delete own votes" ON comment_votes FOR DELETE USING (auth.uid()::text = user_id);

-- Scraper state policies (service role only)
CREATE POLICY "Service role scraper_progress" ON scraper_progress FOR ALL USING (true);
CREATE POLICY "Service role scraper_completed" ON scraper_completed FOR ALL USING (true);
CREATE POLICY "Service role scraper_pending" ON scraper_pending FOR ALL USING (true);
CREATE POLICY "Service role scraper_failed" ON scraper_failed FOR ALL USING (true);
CREATE POLICY "Service role scraper_random_state" ON scraper_random_state FOR ALL USING (true);
