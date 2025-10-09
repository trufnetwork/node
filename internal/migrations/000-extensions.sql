-- Sets the extension schema contexts for the migration session.
-- The '000' prefix ensures this script is executed before all other migrations.
-- Isolating the `USE` statements is crucial for idempotency, allowing migrations
-- to be re-applied safely and ensuring objects are created in the correct schema.
USE tn_cache AS tn_cache;
USE database_size AS database_size;
USE tn_utils AS tn_utils;
