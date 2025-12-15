-- MIGRATION 039: SETTLEMENT EXTENSION CONFIGURATION
--
-- Creates configuration table for automatic settlement extension (tn_settlement).
-- This extension automatically triggers settle_market() for markets that have
-- reached their settlement time and have attestation data available.
--
-- Features:
-- - Leader-only execution (uses leaderwatch)
-- - Configurable cron schedule
-- - Runtime enable/disable
-- - Batch processing with configurable limits
--
-- Configuration Usage:
-- Enable automatic settlement (every hour):
--   UPDATE settlement_config SET enabled = true WHERE id = 1;
--
-- Disable (emergency stop):
--   UPDATE settlement_config SET enabled = false WHERE id = 1;
--
-- Change schedule to every 15 minutes:
--   UPDATE settlement_config SET settlement_schedule = '0,15,30,45 * * * *' WHERE id = 1;
--
-- Adjust batch size:
--   UPDATE settlement_config SET max_markets_per_run = 50 WHERE id = 1;
--
-- Dependencies:
-- - Migration 032: settle_market() action must exist
-- - Migration 033: process_settlement() action must exist
-- - Migration 037: validate_market_collateral() action must exist
-- - leaderwatch extension: For leadership coordination
-- - tn_attestation extension: For attestation data

-- Settlement configuration table (single row with id=1)
-- Note: enabled defaults to FALSE for safety (must explicitly enable)
CREATE TABLE settlement_config (
    id INT PRIMARY KEY,
    enabled BOOLEAN DEFAULT false NOT NULL,
    settlement_schedule TEXT DEFAULT '0 * * * *' NOT NULL,
    max_markets_per_run INT DEFAULT 10 NOT NULL,
    retry_attempts INT DEFAULT 3 NOT NULL,
    updated_at INT8 DEFAULT 0 NOT NULL
);

-- Seed default configuration
-- enabled = false: Disabled by default for safety (operators must explicitly enable)
-- settlement_schedule = '0 * * * *': Check every hour (on the hour)
-- max_markets_per_run = 10: Process up to 10 markets per job run
-- retry_attempts = 3: Retry up to 3 times for transient failures
INSERT INTO settlement_config (id, enabled, settlement_schedule, max_markets_per_run, retry_attempts, updated_at)
VALUES (1, false, '0 * * * *', 10, 3, 0);

-- Add index for fast settlement candidate queries
-- This index helps the extension quickly find unsettled markets past their settlement time
CREATE INDEX idx_settlement_candidates
ON ob_queries(settled, settle_time);
