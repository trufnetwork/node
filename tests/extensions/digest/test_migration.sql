-- Stub auto_digest action for tn_digest extension bootstrap
-- No-op, deterministic, read-only; used to verify wiring and scheduler calls

CREATE OR REPLACE ACTION auto_digest() PRIVATE VIEW RETURNS TABLE(
    processed_days INT8,
    has_more BOOL,
    total_deleted_rows INT8
) {
    -- TODO: check if it's the leader node
    -- if (@leader <> @caller) {
    --     ERROR('auto_digest can only be called by the leader node');
    -- }
    INSERT INTO test_digest_action (id, caller) VALUES (@height, @caller);
    RETURN SELECT 0::INT8, false::BOOL, 0::INT8;
};

-- temporary table/action just to test if the action is working without the full action implementation
CREATE TABLE IF NOT EXISTS test_digest_action(
    id INT8 PRIMARY KEY,
    caller TEXT
);

CREATE OR REPLACE ACTION get_last_digest_action() PRIVATE VIEW RETURNS TABLE(
    id INT8,
    caller TEXT
) {
    RETURN SELECT id, caller FROM test_digest_action ORDER BY id DESC LIMIT 1;
};
