-- Stub auto_digest action for tn_digest extension bootstrap
-- No-op, deterministic, read-only; used to verify wiring and scheduler calls

CREATE OR REPLACE ACTION auto_digest() PUBLIC RETURNS TABLE(
    processed_days INT8,
    has_more BOOL,
    total_deleted_rows INT8
) {
    -- TODO: check if it's the leader node
    -- if (@leader <> @caller) {
    --     ERROR('auto_digest can only be called by the leader node');
    -- }
    RETURN SELECT 0::INT8, false::BOOL, 0::INT8;
};