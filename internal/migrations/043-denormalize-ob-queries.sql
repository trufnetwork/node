-- Migration: Denormalize ob_queries table for discovery
-- Adds structured columns for searchable market data components.
-- Requires Phase 1 (tn_utils.decode_query_components precompile) to be deployed.

-- 1. Add new columns (nullable to accommodate legacy testnet data)
ALTER TABLE ob_queries ADD COLUMN data_provider BYTEA;
ALTER TABLE ob_queries ADD COLUMN stream_id BYTEA;
ALTER TABLE ob_queries ADD COLUMN action_id TEXT;
ALTER TABLE ob_queries ADD COLUMN query_args BYTEA;

-- 2. Add indexes for high-performance discovery
-- Enable direct stream page lookups (O(log n))
CREATE INDEX IF NOT EXISTS idx_ob_queries_stream_id ON ob_queries(stream_id);

-- Enable per-provider stream filtering
CREATE INDEX IF NOT EXISTS idx_ob_queries_provider_stream ON ob_queries(data_provider, stream_id);

-- Enable filtering by action type (e.g. price_above_threshold)
CREATE INDEX IF NOT EXISTS idx_ob_queries_action_id ON ob_queries(action_id);
