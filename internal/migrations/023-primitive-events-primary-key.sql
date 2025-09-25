/*
 * PRIMITIVE EVENTS PRIMARY KEY MIGRATION
 *
 * Establishes the primary key constraint for the primitive_events table
 * following the schema normalization completed in migration 017.
 *
 * The primary key ensures data integrity and enables efficient operations
 * on time-series event data by providing:
 * - Unique row identification for logical replication
 * - Optimal indexing for time-based queries
 * - Proper constraint enforcement for data consistency
 *
 * This migration adds the composite primary key (stream_ref, event_time, created_at)
 * which provides both uniqueness constraints and automatic indexing.
 */

-- Add composite primary key for normalized schema
-- Ensures unique identification of time-series data points
-- Primary key automatically creates a unique index for optimal performance
ALTER TABLE primitive_events ADD CONSTRAINT primitive_events_pkey
    PRIMARY KEY (stream_ref, event_time, created_at);