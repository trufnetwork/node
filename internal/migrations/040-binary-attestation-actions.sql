/*
 * BINARY ATTESTATION ACTIONS
 *
 * Adds binary query actions for prediction market settlement.
 * These actions return TRUE/FALSE based on stream data comparisons,
 * enabling fully automatic settlement without manual intervention.
 *
 * Actions:
 * - price_above_threshold: Returns TRUE if value > threshold
 * - price_below_threshold: Returns TRUE if value < threshold
 * - value_in_range: Returns TRUE if min <= value <= max
 * - value_equals: Returns TRUE if value equals target (within tolerance)
 */

-- =============================================================================
-- Register new action types in attestation_actions table
-- =============================================================================
INSERT INTO attestation_actions (action_name, action_id) VALUES
    ('price_above_threshold', 6),
    ('price_below_threshold', 7),
    ('value_in_range', 8),
    ('value_equals', 9)
ON CONFLICT (action_name) DO NOTHING;

-- =============================================================================
-- price_above_threshold: Returns TRUE if value > threshold at timestamp
-- =============================================================================
--
-- Use case: "Will BTC exceed $100,000 by Dec 31?"
--
-- Parameters:
--   $data_provider: The data provider address (0x-prefixed hex)
--   $stream_id: The stream ID (32 characters)
--   $timestamp: Unix timestamp to check the value at
--   $threshold: The threshold value to compare against
--   $frozen_at: Optional frozen_at timestamp for historical queries
--
-- Returns: Single row with boolean result column
--
CREATE OR REPLACE ACTION price_above_threshold(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $threshold NUMERIC(36, 18),
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE (
    result BOOLEAN
) {
    $data_provider := LOWER($data_provider);
    $max_int8 INT8 := 9223372036854775000;
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- Validate inputs
    if $timestamp IS NULL {
        ERROR('timestamp is required');
    }

    -- Prevent premature resolution
    if @block_timestamp < $timestamp {
        ERROR('Cannot resolve market before target timestamp. Current: ' || @block_timestamp::TEXT || ', Target: ' || $timestamp::TEXT);
    }
    if $threshold IS NULL {
        ERROR('threshold is required');
    }

    -- Get stream reference using helper function (same as get_record_primitive)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    if $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Get value at or before the timestamp
    -- Look back up to 1 day (86400 seconds) to find the most recent value
    $value NUMERIC(36, 18) := NULL;
    $found BOOLEAN := FALSE;
    $lookback_start INT8 := $timestamp - 86400;

    -- Query primitive events directly using stream_ref
    for $row in SELECT pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
          AND pe.event_time <= $timestamp
          AND pe.event_time >= $lookback_start
          AND pe.created_at <= $effective_frozen_at
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    {
        $value := $row.value;
        $found := TRUE;
    }

    -- If no primitive data found, try composed stream via get_record
    if NOT $found {
        for $row in get_record($data_provider, $stream_id, $lookback_start, $timestamp, $effective_frozen_at, FALSE) {
            $value := $row.value;
            $found := TRUE;
        }
    }

    if NOT $found OR $value IS NULL {
        ERROR('No data found for stream ' || $stream_id ||
              ' at timestamp ' || $timestamp::TEXT ||
              '. Data provider: ' || $data_provider);
    }

    RETURN NEXT $value > $threshold;
};

-- =============================================================================
-- price_below_threshold: Returns TRUE if value < threshold at timestamp
-- =============================================================================
--
-- Use case: "Will unemployment rate drop below 4%?"
--
CREATE OR REPLACE ACTION price_below_threshold(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $threshold NUMERIC(36, 18),
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE (
    result BOOLEAN
) {
    $data_provider := LOWER($data_provider);
    $max_int8 INT8 := 9223372036854775000;
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- Validate inputs
    if $timestamp IS NULL {
        ERROR('timestamp is required');
    }

    -- Prevent premature resolution
    if @block_timestamp < $timestamp {
        ERROR('Cannot resolve market before target timestamp. Current: ' || @block_timestamp::TEXT || ', Target: ' || $timestamp::TEXT);
    }
    if $threshold IS NULL {
        ERROR('threshold is required');
    }

    -- Get stream reference
    $stream_ref := get_stream_id($data_provider, $stream_id);
    if $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    $value NUMERIC(36, 18) := NULL;
    $found BOOLEAN := FALSE;
    $lookback_start INT8 := $timestamp - 86400;

    -- Query primitive events directly
    for $row in SELECT pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
          AND pe.event_time <= $timestamp
          AND pe.event_time >= $lookback_start
          AND pe.created_at <= $effective_frozen_at
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    {
        $value := $row.value;
        $found := TRUE;
    }

    -- If no primitive data found, try composed stream
    if NOT $found {
        for $row in get_record($data_provider, $stream_id, $lookback_start, $timestamp, $effective_frozen_at, FALSE) {
            $value := $row.value;
            $found := TRUE;
        }
    }

    if NOT $found OR $value IS NULL {
        ERROR('No data found for stream ' || $stream_id ||
              ' at timestamp ' || $timestamp::TEXT);
    }

    RETURN NEXT $value < $threshold;
};

-- =============================================================================
-- value_in_range: Returns TRUE if min <= value <= max at timestamp
-- =============================================================================
--
-- Use case: "Will BTC stay between $90k-$110k on settlement date?"
--
CREATE OR REPLACE ACTION value_in_range(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $min_value NUMERIC(36, 18),
    $max_value NUMERIC(36, 18),
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE (
    result BOOLEAN
) {
    $data_provider := LOWER($data_provider);
    $max_int8 INT8 := 9223372036854775000;
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- Validate inputs
    if $timestamp IS NULL {
        ERROR('timestamp is required');
    }

    -- Prevent premature resolution
    if @block_timestamp < $timestamp {
        ERROR('Cannot resolve market before target timestamp. Current: ' || @block_timestamp::TEXT || ', Target: ' || $timestamp::TEXT);
    }
    if $min_value IS NULL {
        ERROR('min_value is required');
    }
    if $max_value IS NULL {
        ERROR('max_value is required');
    }
    if $min_value > $max_value {
        ERROR('Invalid range: min_value (' || $min_value::TEXT ||
              ') > max_value (' || $max_value::TEXT || ')');
    }

    -- Get stream reference
    $stream_ref := get_stream_id($data_provider, $stream_id);
    if $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    $value NUMERIC(36, 18) := NULL;
    $found BOOLEAN := FALSE;
    $lookback_start INT8 := $timestamp - 86400;

    -- Query primitive events directly
    for $row in SELECT pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
          AND pe.event_time <= $timestamp
          AND pe.event_time >= $lookback_start
          AND pe.created_at <= $effective_frozen_at
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    {
        $value := $row.value;
        $found := TRUE;
    }

    -- If no primitive data found, try composed stream
    if NOT $found {
        for $row in get_record($data_provider, $stream_id, $lookback_start, $timestamp, $effective_frozen_at, FALSE) {
            $value := $row.value;
            $found := TRUE;
        }
    }

    if NOT $found OR $value IS NULL {
        ERROR('No data found for stream ' || $stream_id ||
              ' at timestamp ' || $timestamp::TEXT);
    }

    RETURN NEXT ($value >= $min_value AND $value <= $max_value);
};

-- =============================================================================
-- value_equals: Returns TRUE if value equals target (within tolerance)
-- =============================================================================
--
-- Use case: "Will Fed rate be exactly 5.25%?" (with 0 tolerance for exact match)
--
CREATE OR REPLACE ACTION value_equals(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $target NUMERIC(36, 18),
    $tolerance NUMERIC(36, 18),
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE (
    result BOOLEAN
) {
    $data_provider := LOWER($data_provider);
    $max_int8 INT8 := 9223372036854775000;
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);
    $effective_tolerance NUMERIC(36, 18) := COALESCE($tolerance, 0::NUMERIC(36, 18));

    -- Validate inputs
    if $timestamp IS NULL {
        ERROR('timestamp is required');
    }

    -- Prevent premature resolution
    if @block_timestamp < $timestamp {
        ERROR('Cannot resolve market before target timestamp. Current: ' || @block_timestamp::TEXT || ', Target: ' || $timestamp::TEXT);
    }
    if $target IS NULL {
        ERROR('target is required');
    }
    if $effective_tolerance < 0::NUMERIC(36, 18) {
        ERROR('Tolerance cannot be negative: ' || $effective_tolerance::TEXT);
    }

    -- Get stream reference
    $stream_ref := get_stream_id($data_provider, $stream_id);
    if $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    $value NUMERIC(36, 18) := NULL;
    $found BOOLEAN := FALSE;
    $lookback_start INT8 := $timestamp - 86400;

    -- Query primitive events directly
    for $row in SELECT pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
          AND pe.event_time <= $timestamp
          AND pe.event_time >= $lookback_start
          AND pe.created_at <= $effective_frozen_at
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    {
        $value := $row.value;
        $found := TRUE;
    }

    -- If no primitive data found, try composed stream
    if NOT $found {
        for $row in get_record($data_provider, $stream_id, $lookback_start, $timestamp, $effective_frozen_at, FALSE) {
            $value := $row.value;
            $found := TRUE;
        }
    }

    if NOT $found OR $value IS NULL {
        ERROR('No data found for stream ' || $stream_id ||
              ' at timestamp ' || $timestamp::TEXT);
    }

    -- Check if value is within tolerance of target
    $diff NUMERIC(36, 18) := abs($value - $target);
    RETURN NEXT $diff <= $effective_tolerance;
};
