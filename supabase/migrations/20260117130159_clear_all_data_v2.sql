-- Clear all data from all tables
TRUNCATE TABLE comment_votes CASCADE;
TRUNCATE TABLE comments CASCADE;
TRUNCATE TABLE watch_history CASCADE;
TRUNCATE TABLE video_bookmarks CASCADE;
TRUNCATE TABLE video_ratings CASCADE;
TRUNCATE TABLE video_cast CASCADE;
TRUNCATE TABLE video_categories CASCADE;
TRUNCATE TABLE videos CASCADE;
TRUNCATE TABLE cast_members CASCADE;
TRUNCATE TABLE categories CASCADE;
TRUNCATE TABLE scraper_failed CASCADE;
TRUNCATE TABLE scraper_pending CASCADE;
TRUNCATE TABLE scraper_completed CASCADE;
TRUNCATE TABLE scraper_progress CASCADE;
TRUNCATE TABLE scraper_random_state CASCADE;
