-- Sets the 'tn_cache' schema context for the migration session.
-- The '000' prefix ensures this script is executed before all other migrations.
-- Isolating the `USE` statement is crucial for idempotency, allowing migrations
-- to be re-applied safely and ensuring objects are created in the correct schema.
USE tn_cache AS tn_cache;