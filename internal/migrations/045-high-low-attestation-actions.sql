/*
 * HIGH/LOW ATTESTATION ACTIONS
 *
 * Adds get_high_value and get_low_value actions that return a single row
 * (max or min value within a bounded date range). These are designed for
 * attestation use cases where escrows need high/low price data.
 *
 * Both actions return exactly 1 row (LIMIT 1), making them safe for the
 * attestation path. The precompile validates a max 90-day date range.
 *
 * Each primitive action anchors before $from, so a range in which the stream
 * published nothing reports the value that was standing rather than no rows.
 * Without that, a stream that only publishes when its value moves answers an
 * escrow's high/low question with an empty attestation.
 *
 * Action IDs:
 *   10 = get_high_value (max value in [from, to])
 *   11 = get_low_value  (min value in [from, to])
 */

-- Register new actions in the attestation allowlist
INSERT INTO attestation_actions (action_name, action_id) VALUES ('get_high_value', 10)
ON CONFLICT (action_name) DO NOTHING;
INSERT INTO attestation_actions (action_name, action_id) VALUES ('get_low_value', 11)
ON CONFLICT (action_name) DO NOTHING;

-- =============================================================================
-- get_high_value_primitive: Returns the row with the highest value in [from, to]
-- =============================================================================
CREATE OR REPLACE ACTION get_high_value_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    if is_allowed_to_read_core($stream_ref, $lower_caller, $from, $to) == false {
        ERROR('wallet not allowed to read');
    }

    $max_int8 INT8 := 9223372036854775000;
    $effective_from INT8 := COALESCE($from, 0);
    $effective_to INT8 := COALESCE($to, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    RETURN WITH
    deduped AS (
        SELECT pe.event_time, pe.value,
            ROW_NUMBER() OVER (
                PARTITION BY pe.event_time
                ORDER BY pe.created_at DESC
            ) AS rn
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
            AND pe.event_time >= $effective_from
            AND pe.event_time <= $effective_to
            AND pe.created_at <= $effective_frozen_at
    ),
    -- A stream that did not move during [from, to] has no rows in range, but the
    -- value it published last still stood for the whole window. Reach back for it
    -- the way get_record_primitive's anchor_record does, so a quiet range reports
    -- that value instead of nothing. Strictly '<' because the range above is
    -- inclusive of $effective_from, so this cannot double-count the row at $from.
    anchor AS (
        SELECT pe.event_time, pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
            AND pe.event_time < $effective_from
            AND pe.created_at <= $effective_frozen_at
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    ),
    candidates AS (
        SELECT event_time, value FROM deduped WHERE rn = 1
        UNION ALL
        SELECT event_time, value FROM anchor
    )
    -- On a tie the earliest event_time wins, which is the anchor. That is what the
    -- composed branch below already reports: it walks get_record in event_time
    -- order and keeps the first maximum, and get_record leads with its own anchor.
    -- The two branches have to agree, so the tie is not broken in favour of the
    -- in-range row.
    SELECT event_time, value FROM candidates
    ORDER BY value DESC, event_time ASC
    LIMIT 1;
};

-- =============================================================================
-- get_low_value_primitive: Returns the row with the lowest value in [from, to]
-- =============================================================================
CREATE OR REPLACE ACTION get_low_value_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    if is_allowed_to_read_core($stream_ref, $lower_caller, $from, $to) == false {
        ERROR('wallet not allowed to read');
    }

    $max_int8 INT8 := 9223372036854775000;
    $effective_from INT8 := COALESCE($from, 0);
    $effective_to INT8 := COALESCE($to, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    RETURN WITH
    deduped AS (
        SELECT pe.event_time, pe.value,
            ROW_NUMBER() OVER (
                PARTITION BY pe.event_time
                ORDER BY pe.created_at DESC
            ) AS rn
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
            AND pe.event_time >= $effective_from
            AND pe.event_time <= $effective_to
            AND pe.created_at <= $effective_frozen_at
    ),
    -- A stream that did not move during [from, to] has no rows in range, but the
    -- value it published last still stood for the whole window. Reach back for it
    -- the way get_record_primitive's anchor_record does, so a quiet range reports
    -- that value instead of nothing. Strictly '<' because the range above is
    -- inclusive of $effective_from, so this cannot double-count the row at $from.
    anchor AS (
        SELECT pe.event_time, pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
            AND pe.event_time < $effective_from
            AND pe.created_at <= $effective_frozen_at
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    ),
    candidates AS (
        SELECT event_time, value FROM deduped WHERE rn = 1
        UNION ALL
        SELECT event_time, value FROM anchor
    )
    -- On a tie the earliest event_time wins, which is the anchor. That is what the
    -- composed branch below already reports: it walks get_record in event_time
    -- order and keeps the first maximum, and get_record leads with its own anchor.
    -- The two branches have to agree, so the tie is not broken in favour of the
    -- in-range row.
    SELECT event_time, value FROM candidates
    ORDER BY value ASC, event_time ASC
    LIMIT 1;
};

-- =============================================================================
-- get_high_value: Public facade — routes to primitive or composed
-- =============================================================================
CREATE OR REPLACE ACTION get_high_value(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider := LOWER($data_provider);
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);

    if $is_primitive {
        for $row in get_high_value_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        -- For composed streams, iterate get_record results to find max value
        $max_value NUMERIC(36,18) := NULL;
        $max_event_time INT8 := NULL;
        for $row in get_record($data_provider, $stream_id, $from, $to, $frozen_at, FALSE) {
            -- Split from the comparison on purpose. The interpreter does not
            -- implement three-valued logic: `$max_value IS NULL OR $row.value >
            -- $max_value` evaluates TRUE OR NULL to NULL on the first row, which
            -- reads as not-taken, so the combined guard never assigns and this
            -- branch returned nothing for every composed stream.
            if $max_value IS NULL {
                $max_value := $row.value;
                $max_event_time := $row.event_time;
            } ELSEIF $row.value > $max_value {
                $max_value := $row.value;
                $max_event_time := $row.event_time;
            }
        }
        if $max_value IS NOT NULL {
            RETURN NEXT $max_event_time, $max_value;
        }
    }
};

-- =============================================================================
-- get_low_value: Public facade — routes to primitive or composed
-- =============================================================================
CREATE OR REPLACE ACTION get_low_value(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PUBLIC VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider := LOWER($data_provider);
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);

    if $is_primitive {
        for $row in get_low_value_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        -- For composed streams, iterate get_record results to find min value
        $min_value NUMERIC(36,18) := NULL;
        $min_event_time INT8 := NULL;
        for $row in get_record($data_provider, $stream_id, $from, $to, $frozen_at, FALSE) {
            -- Split from the comparison on purpose. The interpreter does not
            -- implement three-valued logic: `$min_value IS NULL OR $row.value <
            -- $min_value` evaluates TRUE OR NULL to NULL on the first row, which
            -- reads as not-taken, so the combined guard never assigns and this
            -- branch returned nothing for every composed stream.
            if $min_value IS NULL {
                $min_value := $row.value;
                $min_event_time := $row.event_time;
            } ELSEIF $row.value < $min_value {
                $min_value := $row.value;
                $min_event_time := $row.event_time;
            }
        }
        if $min_value IS NOT NULL {
            RETURN NEXT $min_event_time, $min_value;
        }
    }
};
