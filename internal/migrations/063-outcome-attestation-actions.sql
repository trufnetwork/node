/*
 * OUTCOME ATTESTATION ACTIONS (ids 13 to 16)
 *
 * The actions of 040 and 055 can only answer YES or NO. When the print a
 * market waits for never arrives they error on every capture, so the market
 * never settles and its collateral stays locked (website#5250). The actions
 * here answer in one row whose value is 1 (YES), 0 (NO) or -1 (INVALID):
 *
 *   13 index_change_in_range_v2   change over time_interval in [min, max)
 *   14 price_above_threshold_v2   value > threshold
 *   15 price_below_threshold_v2   value < threshold
 *   16 value_in_range_v2          value in [min, max)
 *
 * Every one takes $timestamp (T), $window (W) and $tolerance. tolerance bounds
 * the event time of the record read; W bounds block time. Before block time T
 * every action errors with a message starting 'awaiting print', so a capture
 * never answers early, whatever the market's settle_time. From T, while no
 * record is found, it errors the same way, so the capture is retried; from
 * block time T + W on it returns INVALID instead. event_time in the row is the
 * event time of the record read, or T + W when there was none.
 *
 * Primitive streams only. A missing, deleted or composed stream reads as "no
 * record", so a market on it ends INVALID at T + W rather than hanging. Read
 * permissions are not checked, as in the 040 actions, so every requester gets
 * the same answer and only the outcome is published. Records are read straight
 * from primitive_events by stream_ref: the
 * latest created_at wins at an event time, and value DESC breaks a full tie,
 * so every node picks the same row.
 *
 * The argument checks repeat the Go checks in validate_attestation_date_range
 * (extensions/tn_utils/outcome.go), which create_market and
 * request_attestation run first. They must never be stricter than the Go
 * checks, or a market could be created whose capture always fails.
 *
 * Apply order: the binary that knows ids 13 to 16 and
 * parse_attestation_outcome goes on every node kgw or our RPC serves from
 * first. Then the new settle_market and create_market and this file go out in
 * one atomic exec-sql, never this file first:
 *
 *   - testnet: settle_market and create_market from
 *     032-order-book-actions.sql, then this file.
 *   - mainnet: settle_market from 032-order-book-actions.sql (it names no
 *     bridge, and 032.prod has no copy of it), create_market from
 *     032-order-book-actions.prod.sql, then this file. The create_market in
 *     032-order-book-actions.sql charges its fee through hoodi_tt, which
 *     mainnet does not have. The full 032 followed by the full 032.prod, then
 *     this file, is the other allowed form.
 *
 * A v2 market created before the new create_market skips its argument checks,
 * and one reaching the old settle_market is quarantined on 'unsupported
 * action_id'. Once this file is applied the binary cannot be rolled back.
 */

-- =============================================================================
-- v2_readable_primitive_ref: the stream's ref, or NULL when it cannot be read
-- =============================================================================
--
-- NULL when the stream does not exist (or was deleted) or is not primitive.
-- Nothing here raises: a stream that cannot be read is a print that never
-- arrives, and the market ends INVALID at T + W. Read permissions are not
-- checked, as in the 040 actions: a market's outcome must not depend on who
-- requests the capture.
--
CREATE OR REPLACE ACTION v2_readable_primitive_ref(
    $data_provider TEXT,
    $stream_id TEXT
) PRIVATE VIEW RETURNS (stream_ref INT) {
    $ref INT := get_stream_id(LOWER($data_provider), $stream_id);
    if $ref IS NULL {
        RETURN NULL;
    }

    $stream_type TEXT;
    for $row in SELECT stream_type FROM streams WHERE id = $ref {
        $stream_type := $row.stream_type;
    }
    if $stream_type IS NULL {
        RETURN NULL;
    }
    if $stream_type != 'primitive' {
        RETURN NULL;
    }

    RETURN $ref;
};

-- =============================================================================
-- v2_first_in: the first record with $from <= event_time <= $to
-- =============================================================================
CREATE OR REPLACE ACTION v2_first_in(
    $stream_ref INT,
    $from INT8,
    $to INT8
) PRIVATE VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    if $stream_ref IS NULL {
        RETURN;
    }
    if $from IS NULL {
        ERROR('v2_first_in: from is required');
    }
    if $to IS NULL {
        ERROR('v2_first_in: to is required');
    }

    RETURN SELECT pe.event_time, pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
          AND pe.event_time >= $from
          AND pe.event_time <= $to
        ORDER BY pe.event_time ASC, pe.created_at DESC, pe.value DESC
        LIMIT 1;
};

-- =============================================================================
-- v2_last_in: the latest record with $from <= event_time <= $to
-- =============================================================================
CREATE OR REPLACE ACTION v2_last_in(
    $stream_ref INT,
    $from INT8,
    $to INT8
) PRIVATE VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    if $stream_ref IS NULL {
        RETURN;
    }
    if $from IS NULL {
        ERROR('v2_last_in: from is required');
    }
    if $to IS NULL {
        ERROR('v2_last_in: to is required');
    }

    RETURN SELECT pe.event_time, pe.value
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
          AND pe.event_time >= $from
          AND pe.event_time <= $to
        ORDER BY pe.event_time DESC, pe.created_at DESC, pe.value DESC
        LIMIT 1;
};

-- =============================================================================
-- v2_nearest: the record nearest $target, at most $tolerance away
-- =============================================================================
--
-- Two point lookups, the latest record at or before $target and the first at
-- or after it, each only within $tolerance. The nearer one wins and the
-- earlier one wins a tie. No row when neither side has a record.
--
CREATE OR REPLACE ACTION v2_nearest(
    $stream_ref INT,
    $target INT8,
    $tolerance INT8
) PRIVATE VIEW RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    if $stream_ref IS NULL {
        RETURN;
    }
    if $target IS NULL {
        ERROR('v2_nearest: target is required');
    }
    if $tolerance IS NULL {
        ERROR('v2_nearest: tolerance is required');
    }

    $below_time INT8;
    $below_value NUMERIC(36,18);
    for $row in v2_last_in($stream_ref, $target - $tolerance, $target) {
        $below_time := $row.event_time;
        $below_value := $row.value;
    }

    $above_time INT8;
    $above_value NUMERIC(36,18);
    for $row in v2_first_in($stream_ref, $target, $target + $tolerance) {
        $above_time := $row.event_time;
        $above_value := $row.value;
    }

    if $below_time IS NULL {
        if $above_time IS NOT NULL {
            RETURN NEXT $above_time, $above_value;
        }
        RETURN;
    }
    if $above_time IS NULL {
        RETURN NEXT $below_time, $below_value;
        RETURN;
    }

    if ($above_time - $target) < ($target - $below_time) {
        RETURN NEXT $above_time, $above_value;
        RETURN;
    }
    RETURN NEXT $below_time, $below_value;
};

-- =============================================================================
-- v2_check_window_args: the checks every outcome action shares
-- =============================================================================
--
-- Bounding T, W and tolerance first keeps every sum below (T + W, target plus
-- or minus tolerance) far inside INT8: the engine adds INT8 without an
-- overflow check. 315360000 is 10 years of 365 days and 3888000 is 45 days,
-- the Go limits; 2 * 45 days keeps a nearest read inside the 90-day range rule.
--
CREATE OR REPLACE ACTION v2_check_window_args(
    $timestamp INT8,
    $window INT8,
    $tolerance INT8
) PRIVATE VIEW {
    if $timestamp IS NULL {
        ERROR('timestamp is required');
    }
    if $window IS NULL {
        ERROR('window is required');
    }
    if $tolerance IS NULL {
        ERROR('tolerance is required');
    }

    if $timestamp < 0 {
        ERROR('timestamp must not be negative, got ' || $timestamp::TEXT);
    }
    -- Go skips this check when the call carries no block time.
    if @block_timestamp > 0 {
        if $timestamp > @block_timestamp + 315360000 {
            ERROR('timestamp ' || $timestamp::TEXT || ' is more than 315360000 seconds after the block time ' ||
                  @block_timestamp::TEXT);
        }
    }

    if $window <= 0 {
        ERROR('window must be in (0, 315360000], got ' || $window::TEXT);
    }
    if $window > 315360000 {
        ERROR('window must be in (0, 315360000], got ' || $window::TEXT);
    }
    if $tolerance < 0 {
        ERROR('tolerance must be in [0, 3888000], got ' || $tolerance::TEXT);
    }
    if $tolerance > 3888000 {
        ERROR('tolerance must be in [0, 3888000], got ' || $tolerance::TEXT);
    }
    -- A print stamped anywhere in the tolerance must be able to arrive before
    -- the deadline.
    if $window < $tolerance {
        ERROR('window (' || $window::TEXT || ') must not be less than tolerance (' || $tolerance::TEXT || ')');
    }
};

-- =============================================================================
-- v2_await_deadline: wait for a missing record until T + W
-- =============================================================================
--
-- Errors while the deadline is in the future, so the capture reverts and is
-- asked for again later. Returns from the deadline on, and the caller answers
-- INVALID.
--
CREATE OR REPLACE ACTION v2_await_deadline(
    $deadline INT8,
    $missing TEXT
) PRIVATE VIEW {
    if @block_timestamp < $deadline {
        ERROR('awaiting print: ' || $missing || '; the market ends INVALID from block time ' || $deadline::TEXT);
    }
};

-- =============================================================================
-- v2_await_timestamp: answer only from block time T on
-- =============================================================================
--
-- create_market lets settle_time fall before T, and settle_market takes the
-- first capture at or after settle_time. A capture before T sees only what has
-- been written so far: a record stamped at or before T can still arrive, so an
-- older record would stand in for the value as of T. Errors until T, so the
-- capture is asked for again later. This cannot hang a market: W > 0, so the
-- deadline T + W comes after T.
--
CREATE OR REPLACE ACTION v2_await_timestamp(
    $timestamp INT8
) PRIVATE VIEW {
    if @block_timestamp < $timestamp {
        ERROR('awaiting print: block time ' || @block_timestamp::TEXT || ' is before the timestamp ' ||
              $timestamp::TEXT);
    }
};

-- =============================================================================
-- index_change_in_range_v2 (id 13): did the stream move by a percentage in
-- [min, max) over time_interval?
-- =============================================================================
--
-- current: the first record in [T, T + tolerance].
-- prior:   the record nearest current.event_time - time_interval, at most
--          tolerance away.
-- change:  (current - prior) * 100 / prior, YES when min <= change < max. A
--          NULL bound is an open tail, as in 055.
--
-- Nothing is answered before block time T. A missing current or prior record
-- waits until T + W, then is INVALID. A zero prior is INVALID from T: the
-- change is undefined, and waiting does not change it.
--
CREATE OR REPLACE ACTION index_change_in_range_v2(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $window INT8,
    $tolerance INT8,
    $time_interval INT8,
    $min_change NUMERIC(36, 18),
    $max_change NUMERIC(36, 18)
) PUBLIC VIEW RETURNS TABLE (
    event_time INT8,
    value NUMERIC(36, 18)
) {
    v2_check_window_args($timestamp, $window, $tolerance);

    if $time_interval IS NULL {
        ERROR('time_interval is required');
    }
    if $time_interval <= 0 {
        ERROR('time_interval must be in (0, 315360000], got ' || $time_interval::TEXT);
    }
    if $time_interval > 315360000 {
        ERROR('time_interval must be in (0, 315360000], got ' || $time_interval::TEXT);
    }
    -- Keeps the prior's window clear of the current record's window.
    if 2 * $tolerance >= $time_interval {
        ERROR('time_interval (' || $time_interval::TEXT || ') must be more than twice tolerance (' ||
              $tolerance::TEXT || ')');
    }

    -- Split rather than joined with OR: kwil's OR does not follow SQL
    -- three-valued logic.
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

    -- The forward window is empty before T unless a record is stamped ahead of
    -- its time; waiting keeps that record from settling the market early.
    v2_await_timestamp($timestamp);

    $yes NUMERIC(36, 18) := 1::NUMERIC(36, 18);
    $no NUMERIC(36, 18) := 0::NUMERIC(36, 18);
    $invalid NUMERIC(36, 18) := '-1'::NUMERIC(36, 18);
    $deadline INT8 := $timestamp + $window;
    $current_to INT8 := $timestamp + $tolerance;

    $ref INT := v2_readable_primitive_ref($data_provider, $stream_id);

    $current_time INT8;
    $current_value NUMERIC(36, 18);
    for $row in v2_first_in($ref, $timestamp, $current_to) {
        $current_time := $row.event_time;
        $current_value := $row.value;
    }

    if $current_time IS NULL {
        v2_await_deadline($deadline, 'no readable record in [' || $timestamp::TEXT || ', ' || $current_to::TEXT || ']');
        RETURN NEXT $deadline, $invalid;
        RETURN;
    }

    $prior_target INT8 := $current_time - $time_interval;
    $prior_time INT8;
    $prior_value NUMERIC(36, 18);
    for $row in v2_nearest($ref, $prior_target, $tolerance) {
        $prior_time := $row.event_time;
        $prior_value := $row.value;
    }

    -- A history backfill can still be writing the prior, so it waits too.
    if $prior_time IS NULL {
        v2_await_deadline($deadline, 'no prior record within ' || $tolerance::TEXT || ' seconds of ' ||
                          $prior_target::TEXT);
        RETURN NEXT $current_time, $invalid;
        RETURN;
    }

    if $prior_value = 0::NUMERIC(36, 18) {
        RETURN NEXT $current_time, $invalid;
        RETURN;
    }

    -- Compared without dividing, in a wider type: (current - prior) * 100 and
    -- bound * prior both fit NUMERIC(78,36) exactly for any two
    -- NUMERIC(36,18) values, where the same sums in NUMERIC(36,18) overflow.
    -- Dividing by a negative prior flips each comparison.
    $current_wide NUMERIC(78, 36) := $current_value::NUMERIC(78, 36);
    $prior_wide NUMERIC(78, 36) := $prior_value::NUMERIC(78, 36);
    $move NUMERIC(78, 36) := ($current_wide - $prior_wide) * 100::NUMERIC(78, 36);
    $prior_positive BOOL := $prior_wide > 0::NUMERIC(78, 36);

    $in_range BOOL := true;
    if $min_change IS NOT NULL {
        $min_scaled NUMERIC(78, 36) := $min_change::NUMERIC(78, 36) * $prior_wide;
        if $prior_positive {
            if $move < $min_scaled {
                $in_range := false;
            }
        } else {
            if $move > $min_scaled {
                $in_range := false;
            }
        }
    }
    if $max_change IS NOT NULL {
        $max_scaled NUMERIC(78, 36) := $max_change::NUMERIC(78, 36) * $prior_wide;
        if $prior_positive {
            if $move >= $max_scaled {
                $in_range := false;
            }
        } else {
            if $move <= $max_scaled {
                $in_range := false;
            }
        }
    }

    if $in_range {
        RETURN NEXT $current_time, $yes;
        RETURN;
    }
    RETURN NEXT $current_time, $no;
};

-- =============================================================================
-- price_above_threshold_v2 (id 14): was the value as of T above threshold?
-- =============================================================================
--
-- The value as of T is the latest record in [T - tolerance, T], the meaning
-- the 040 actions have, without their fallback to an older record. Like the
-- 040 actions, it waits for block time T first. No record waits until T + W,
-- then is INVALID.
--
CREATE OR REPLACE ACTION price_above_threshold_v2(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $window INT8,
    $tolerance INT8,
    $threshold NUMERIC(36, 18)
) PUBLIC VIEW RETURNS TABLE (
    event_time INT8,
    value NUMERIC(36, 18)
) {
    v2_check_window_args($timestamp, $window, $tolerance);
    -- A NULL threshold would make the comparison NULL and resolve NO.
    if $threshold IS NULL {
        ERROR('threshold is required');
    }
    v2_await_timestamp($timestamp);

    $yes NUMERIC(36, 18) := 1::NUMERIC(36, 18);
    $no NUMERIC(36, 18) := 0::NUMERIC(36, 18);
    $invalid NUMERIC(36, 18) := '-1'::NUMERIC(36, 18);
    $deadline INT8 := $timestamp + $window;

    -- Event times below 0 cannot be encoded in the attestation.
    $current_from INT8 := $timestamp - $tolerance;
    if $current_from < 0 {
        $current_from := 0;
    }

    $ref INT := v2_readable_primitive_ref($data_provider, $stream_id);

    $current_time INT8;
    $current_value NUMERIC(36, 18);
    for $row in v2_last_in($ref, $current_from, $timestamp) {
        $current_time := $row.event_time;
        $current_value := $row.value;
    }

    if $current_time IS NULL {
        v2_await_deadline($deadline, 'no readable record in [' || $current_from::TEXT || ', ' || $timestamp::TEXT || ']');
        RETURN NEXT $deadline, $invalid;
        RETURN;
    }

    if $current_value > $threshold {
        RETURN NEXT $current_time, $yes;
        RETURN;
    }
    RETURN NEXT $current_time, $no;
};

-- =============================================================================
-- price_below_threshold_v2 (id 15): was the value as of T below threshold?
-- =============================================================================
--
-- Same selection, waiting and INVALID rule as price_above_threshold_v2.
--
CREATE OR REPLACE ACTION price_below_threshold_v2(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $window INT8,
    $tolerance INT8,
    $threshold NUMERIC(36, 18)
) PUBLIC VIEW RETURNS TABLE (
    event_time INT8,
    value NUMERIC(36, 18)
) {
    v2_check_window_args($timestamp, $window, $tolerance);
    -- A NULL threshold would make the comparison NULL and resolve NO.
    if $threshold IS NULL {
        ERROR('threshold is required');
    }
    v2_await_timestamp($timestamp);

    $yes NUMERIC(36, 18) := 1::NUMERIC(36, 18);
    $no NUMERIC(36, 18) := 0::NUMERIC(36, 18);
    $invalid NUMERIC(36, 18) := '-1'::NUMERIC(36, 18);
    $deadline INT8 := $timestamp + $window;

    -- Event times below 0 cannot be encoded in the attestation.
    $current_from INT8 := $timestamp - $tolerance;
    if $current_from < 0 {
        $current_from := 0;
    }

    $ref INT := v2_readable_primitive_ref($data_provider, $stream_id);

    $current_time INT8;
    $current_value NUMERIC(36, 18);
    for $row in v2_last_in($ref, $current_from, $timestamp) {
        $current_time := $row.event_time;
        $current_value := $row.value;
    }

    if $current_time IS NULL {
        v2_await_deadline($deadline, 'no readable record in [' || $current_from::TEXT || ', ' || $timestamp::TEXT || ']');
        RETURN NEXT $deadline, $invalid;
        RETURN;
    }

    if $current_value < $threshold {
        RETURN NEXT $current_time, $yes;
        RETURN;
    }
    RETURN NEXT $current_time, $no;
};

-- =============================================================================
-- value_in_range_v2 (id 16): was the value as of T in [min, max)?
-- =============================================================================
--
-- Same selection, waiting and INVALID rule as price_above_threshold_v2. The
-- range is half-open with NULL tails, as in 055, so neighbouring buckets never
-- both win; 040's value_in_range is inclusive at both ends.
--
CREATE OR REPLACE ACTION value_in_range_v2(
    $data_provider TEXT,
    $stream_id TEXT,
    $timestamp INT8,
    $window INT8,
    $tolerance INT8,
    $min_value NUMERIC(36, 18),
    $max_value NUMERIC(36, 18)
) PUBLIC VIEW RETURNS TABLE (
    event_time INT8,
    value NUMERIC(36, 18)
) {
    v2_check_window_args($timestamp, $window, $tolerance);

    -- Split rather than joined with OR: kwil's OR does not follow SQL
    -- three-valued logic.
    if $min_value IS NULL {
        if $max_value IS NULL {
            ERROR('at least one of min_value or max_value is required');
        }
    }
    if $min_value IS NOT NULL {
        if $max_value IS NOT NULL {
            if $min_value >= $max_value {
                ERROR('min_value must be less than max_value');
            }
        }
    }
    v2_await_timestamp($timestamp);

    $yes NUMERIC(36, 18) := 1::NUMERIC(36, 18);
    $no NUMERIC(36, 18) := 0::NUMERIC(36, 18);
    $invalid NUMERIC(36, 18) := '-1'::NUMERIC(36, 18);
    $deadline INT8 := $timestamp + $window;

    -- Event times below 0 cannot be encoded in the attestation.
    $current_from INT8 := $timestamp - $tolerance;
    if $current_from < 0 {
        $current_from := 0;
    }

    $ref INT := v2_readable_primitive_ref($data_provider, $stream_id);

    $current_time INT8;
    $current_value NUMERIC(36, 18);
    for $row in v2_last_in($ref, $current_from, $timestamp) {
        $current_time := $row.event_time;
        $current_value := $row.value;
    }

    if $current_time IS NULL {
        v2_await_deadline($deadline, 'no readable record in [' || $current_from::TEXT || ', ' || $timestamp::TEXT || ']');
        RETURN NEXT $deadline, $invalid;
        RETURN;
    }

    $in_range BOOL := true;
    if $min_value IS NOT NULL {
        if $current_value < $min_value {
            $in_range := false;
        }
    }
    if $max_value IS NOT NULL {
        if $current_value >= $max_value {
            $in_range := false;
        }
    }

    if $in_range {
        RETURN NEXT $current_time, $yes;
        RETURN;
    }
    RETURN NEXT $current_time, $no;
};

-- =============================================================================
-- process_invalid_settlement: close a market that resolved INVALID
-- =============================================================================
--
-- Called by settle_market after its collateral checks, in place of
-- process_settlement. Nobody wins: every share, holding or ask, YES or NO, is
-- paid $0.50, and every bid gets back what it locked. No fee is taken, so the
-- data provider, validators and LPs get nothing, and the LP samples of the
-- market are deleted.
--
-- Parity (YES shares = NO shares) makes the share total YES * U, so the vault
-- releases exactly what this market owes. That is exact while U % 100 = 0,
-- which holds for every bridge.
--
-- Rows get ids from MAX(id) + 1 in the order written, so both loops are
-- ordered by the ob_positions key.
--
CREATE OR REPLACE ACTION process_invalid_settlement(
    $query_id INT
) PRIVATE {
    $bridge TEXT;
    for $row in SELECT bridge FROM ob_queries WHERE id = $query_id {
        $bridge := $row.bridge;
    }

    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);

    -- One 'settled' event per position. Price is the cents paid per share: 50
    -- for a share, the bid's own price for a bid.
    for $evt_row in
        SELECT participant_id, outcome, price, amount
        FROM ob_positions
        WHERE query_id = $query_id
        ORDER BY participant_id, outcome, price, amount
    {
        $evt_pid INT := $evt_row.participant_id;
        $evt_out BOOL := $evt_row.outcome;
        $evt_raw_price INT := $evt_row.price;
        $evt_amount INT8 := $evt_row.amount;
        $evt_price INT := 50;
        if $evt_raw_price < 0 {
            $evt_price := -$evt_raw_price;
        }
        ob_record_order_event($query_id, $evt_pid, 'settled', $evt_out, $evt_price, $evt_amount, NULL);
    }

    -- Each payout keeps the position's own outcome: ob_batch_unlock_collateral
    -- refuses a NULL one. The wallet is built in the query, since SQL
    -- functions cannot run in the body of a loop over a query.
    $pids INT[];
    $wallet_hexes TEXT[];
    $amounts NUMERIC(78, 0)[];
    $outcomes BOOL[];

    for $pay_row in
        SELECT pos.participant_id,
               '0x' || encode(p.wallet_address, 'hex') AS wallet_hex,
               pos.outcome,
               CASE
                   WHEN pos.price >= 0 THEN ((pos.amount::NUMERIC(78, 0) * $units_per_dollar * 50::NUMERIC(78, 0)) / 100::NUMERIC(78, 0))
                   ELSE ((pos.amount::NUMERIC(78, 0) * (-pos.price)::NUMERIC(78, 0) * $units_per_dollar) / 100::NUMERIC(78, 0))
               END AS pay
        FROM ob_positions pos
        JOIN ob_participants p ON pos.participant_id = p.id
        WHERE pos.query_id = $query_id
        ORDER BY pos.participant_id, pos.outcome, pos.price
    {
        if $pay_row.pay > 0::NUMERIC(78, 0) {
            $pids := array_append($pids, $pay_row.participant_id);
            $wallet_hexes := array_append($wallet_hexes, $pay_row.wallet_hex);
            $amounts := array_append($amounts, $pay_row.pay);
            $outcomes := array_append($outcomes, $pay_row.outcome);
        }
    }

    DELETE FROM ob_positions WHERE query_id = $query_id;
    DELETE FROM ob_rewards WHERE query_id = $query_id;

    if COALESCE(array_length($pids), 0) > 0 {
        ob_batch_unlock_collateral($query_id, $bridge, $pids, $wallet_hexes, $amounts, $outcomes);
    }
};

-- =============================================================================
-- Register the actions in the attestation allowlist
-- =============================================================================
--
-- The ids must match getActionIDNumber in extensions/tn_utils/precompiles.go:
-- create_market hashes a market with the Go id and request_attestation hashes
-- a capture with this table's id. ON CONFLICT skips a name that is already
-- present, so the SELECT after it fails the whole file unless each name maps
-- to its expected id.
INSERT INTO attestation_actions (action_name, action_id) VALUES
    ('index_change_in_range_v2', 13),
    ('price_above_threshold_v2', 14),
    ('price_below_threshold_v2', 15),
    ('value_in_range_v2', 16)
ON CONFLICT (action_name) DO NOTHING;

SELECT error('attestation_actions must map index_change_in_range_v2, price_above_threshold_v2, price_below_threshold_v2 and value_in_range_v2 to ids 13, 14, 15 and 16')
FROM (
    SELECT COUNT(*) AS matched
    FROM attestation_actions
    WHERE (action_name = 'index_change_in_range_v2' AND action_id = 13)
       OR (action_name = 'price_above_threshold_v2' AND action_id = 14)
       OR (action_name = 'price_below_threshold_v2' AND action_id = 15)
       OR (action_name = 'value_in_range_v2' AND action_id = 16)
) AS registered
WHERE registered.matched != 4;
