/*
 * INDEX-CHANGE ATTESTATION ACTION
 *
 * Adds index_change_in_range, a single-row binary action that resolves on how
 * far a stream's index moved over an interval rather than on the value the
 * stream publishes.
 *
 * Streams that publish an index level (BLS CPI at ~335, PCE at ~131) cannot
 * back an inflation-rate market through the value actions in migration 040,
 * because those compare a level against a rate. get_index_change computes the
 * rate but returns a series, and multi-row actions are blocked from
 * attestation. This action computes the same percentage and returns one
 * boolean, so it is attestable.
 *
 * Nullable bounds cover every outcome of a bucketed market with one action id:
 *
 *   min NULL, max 1.335   -> "Below 1.335%"
 *   min 1.335, max 1.605  -> "1.335% - 1.605%"
 *   min 2.246, max NULL   -> "Above 2.246%"
 *   min 0, max NULL       -> "did the rate rise at all?"
 *
 * Bounds are half-open, [min, max), so the buckets of a market tile the number
 * line exactly once. The value actions in 040 are inclusive on both ends of
 * value_in_range, which lets a value landing exactly on an interior boundary
 * satisfy two adjacent buckets.
 *
 * Action ID:
 *   12 = index_change_in_range
 */

-- Register the action in the attestation allowlist. Must stay in step with
-- getActionIDNumber in extensions/tn_utils/precompiles.go; nothing checks that
-- the two agree.
INSERT INTO attestation_actions (action_name, action_id) VALUES ('index_change_in_range', 12)
ON CONFLICT (action_name) DO NOTHING;

-- =============================================================================
-- get_indexed_value_at: the indexed value in force at $at, refusing stale data
-- =============================================================================
--
-- Reads through get_index rather than primitive_events directly, so the number
-- this action settles on is the number get_index_change reports for the same
-- base_time. For a primitive stream the base value cancels out of the ratio and
-- the choice makes no difference; for a composed stream it does, because
-- get_index_composed weights indexed children and the ratio of composed raw
-- records is not the ratio of the composed index.
--
-- The window is one second wide on purpose. get_record carries an LOCF anchor,
-- so a read of ($at - 1, $at] also returns the last record at or before
-- $at - 1 when the window itself holds nothing. That makes an empty result
-- impossible to tell apart from a value that is years old, which is why the
-- staleness check below is an explicit comparison on event_time rather than a
-- narrower range.
--
-- Parameters:
--   $max_staleness: how far before $at the value may sit, in seconds
--
CREATE OR REPLACE ACTION get_indexed_value_at(
    $data_provider TEXT,
    $stream_id TEXT,
    $at INT8,
    $max_staleness INT8,
    $base_time INT8,
    $frozen_at INT8
) PRIVATE VIEW RETURNS (value NUMERIC(36,18)) {
    $found_time INT8 := NULL;
    $found_value NUMERIC(36,18) := NULL;

    -- get_index returns ascending event_time, so the last row of the loop is
    -- the value in force at $at.
    for $row in get_index($data_provider, $stream_id, $at - 1, $at, $frozen_at, $base_time, false) {
        $found_time := $row.event_time;
        $found_value := $row.value;
    }

    if $found_time IS NULL {
        ERROR('No data at or before ' || $at::TEXT || ' for stream ' || $stream_id);
    }

    -- Checked separately from the time, and separately from each other, because
    -- kwil's OR does not follow SQL three-valued logic. A NULL value reaching
    -- the caller would make every comparison NULL, and the bucket would resolve
    -- TRUE without anything having been compared.
    if $found_value IS NULL {
        ERROR('Null value at ' || $found_time::TEXT || ' for stream ' || $stream_id);
    }

    $oldest_allowed INT8 := $at - $max_staleness;
    if $found_time < $oldest_allowed {
        ERROR('Stream ' || $stream_id || ' has no value within ' || $max_staleness::TEXT ||
              ' seconds of ' || $at::TEXT || '. Most recent is ' || $found_time::TEXT);
    }

    RETURN $found_value;
};

-- =============================================================================
-- index_change_in_range: TRUE if the index moved by a percentage in [min, max)
-- =============================================================================
--
-- Use case: "Will US CPI inflation come in between 1.335% and 1.605%?"
--
-- Parameters:
--   $data_provider: The data provider address (0x-prefixed hex)
--   $stream_id: The stream ID (32 characters)
--   $timestamp: Unix timestamp the market settles at
--   $base_time: Base time for the index, passed through to get_index
--   $time_interval: Seconds to look back for the comparison point
--   $min_change: Lower bound in percent, inclusive; NULL for an open tail
--   $max_change: Upper bound in percent, exclusive; NULL for an open tail
--   $frozen_at: Optional frozen_at timestamp for historical queries
--
-- Returns: Single row with boolean result column
--
-- use_cache is never exposed and always passed as false, so every validator
-- computes the same result regardless of cache state. That is also why this
-- action needs no entry in the force_last_arg_false branch of migration 024.
--
CREATE OR REPLACE ACTION index_change_in_range(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $base_time INT8,
    $time_interval INT,
    $min_change NUMERIC(36, 18),
    $max_change NUMERIC(36, 18),
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE (
    result BOOLEAN
) {
    $data_provider := LOWER($data_provider);

    -- A market cannot resolve before its settlement time.
    validate_not_before_timestamp($timestamp);

    if $time_interval IS NULL {
        ERROR('time_interval is required');
    }
    if $time_interval <= 0 {
        ERROR('time_interval must be positive, got ' || $time_interval::TEXT);
    }

    -- Split rather than combined with OR: kwil's OR does not follow SQL
    -- three-valued logic, and `NULL OR NULL` here would not behave as written.
    if $min_change IS NULL {
        if $max_change IS NULL {
            ERROR('at least one of min_change or max_change is required');
        }
    }
    if $min_change IS NOT NULL {
        if $max_change IS NOT NULL {
            if $min_change >= $max_change {
                ERROR('min_change must be less than max_change');
            }
        }
    }

    $interval_seconds INT8 := ($time_interval)::INT8;
    $prior_at INT8 := $timestamp - $interval_seconds;

    -- The current anchor keeps the one-day staleness rule the 040 actions use:
    -- it is a freshness check, refusing to settle today's market on last week's
    -- value.
    $current_value NUMERIC(36,18) := get_indexed_value_at(
        $data_provider, $stream_id, $timestamp, 86400, $base_time, $frozen_at);

    -- The prior anchor is a historical lookup, where staleness means nothing.
    -- Its only job is to refuse when the stream has a hole where the comparison
    -- point belongs, so it scales with the interval asked for: a year-over-year
    -- market accepts a prior print up to a year old, a month-over-month market
    -- accepts a month. A stream too young to have a comparison point is
    -- refused rather than settled on the wrong number.
    $prior_value NUMERIC(36,18) := get_indexed_value_at(
        $data_provider, $stream_id, $prior_at, $interval_seconds, $base_time, $frozen_at);

    -- get_index_change skips a zero prior value and moves to the next row. A
    -- single-row action has no next row, so it refuses.
    if $prior_value = 0::NUMERIC(36,18) {
        ERROR('Prior value is 0 at ' || $prior_at::TEXT || '; percentage change is undefined');
    }

    -- Same arithmetic as get_index_change, so the two agree for the same
    -- (base_time, time_interval).
    $change NUMERIC(36,18) := (($current_value - $prior_value) * 100::NUMERIC(36,18)) / $prior_value;

    $in_range BOOL := true;
    if $min_change IS NOT NULL {
        if $change < $min_change {
            $in_range := false;
        }
    }
    if $max_change IS NOT NULL {
        if $change >= $max_change {
            $in_range := false;
        }
    }

    RETURN NEXT $in_range;
};
