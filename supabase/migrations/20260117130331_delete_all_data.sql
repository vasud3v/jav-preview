-- Delete all data from all tables (CASCADE will handle foreign keys)
DELETE FROM comment_votes;
DELETE FROM comments;
DELETE FROM watch_history;
DELETE FROM video_bookmarks;
DELETE FROM video_ratings;
DELETE FROM video_cast;
DELETE FROM video_categories;
DELETE FROM videos;
DELETE FROM cast_members;
DELETE FROM categories;
DELETE FROM scraper_failed;
DELETE FROM scraper_pending;
DELETE FROM scraper_completed;
DELETE FROM scraper_progress;
DELETE FROM scraper_random_state;
