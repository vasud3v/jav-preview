-- Add a view or computed column to provide 'duration' as an alias for 'watch_duration'
-- This fixes the error: column watch_history.duration does not exist

-- Option 1: Add a generated column (PostgreSQL 12+)
-- This creates a virtual column that always returns the value of watch_duration
ALTER TABLE watch_history 
ADD COLUMN IF NOT EXISTS duration INTEGER GENERATED ALWAYS AS (watch_duration) STORED;

-- Add index on the new column for performance
CREATE INDEX IF NOT EXISTS idx_watch_duration ON watch_history(duration);
