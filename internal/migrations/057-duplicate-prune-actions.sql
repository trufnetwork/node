/*
 * DUPLICATE PRUNE ACTIONS MIGRATION
 *
 * Removes records that restate the value already standing at their event time.
 *
 * Reads carry the last observation forward: a query for a time with no record of
 * its own resolves to the newest record at or before it. So in a run of equal
 * consecutive values only the first is load-bearing -- every later one answers
 * exactly what its predecessor already answered. Deleting them leaves the value
 * at every point in time unchanged, which is the whole safety argument.
 *
 * A record is deleted only when all seven of these hold:
 *
 *   1. Its value equals the value of the nearest surviving earlier record.
 *      "Surviving" is what collapses a run of k equal values to its first rather
 *      than dropping every other one. Written here as `value = LAG(value)`, which
 *      picks the same set: inside a run every row but the first has an equal
 *      predecessor, and afterwards no two neighbours are equal, so one pass
 *      reaches a fixpoint.
 *   2. It is older than the retention window.
 *   3. It is not the stream's first record -- the anchor every read falls back
 *      to. LAG is NULL there, so the null guard covers this.
 *   4a. It is not the stream's newest record by event_time, so "the current
 *      value" stays honest for a stream that has gone quiet.
 *   4b. It does not hold the stream's greatest truflation_created_at. The
 *      Truflation provider derives its fetch watermark from that maximum: delete
 *      the row holding it and the watermark walks backwards, the provider
 *      re-publishes what was just deleted, and the next pass deletes it again.
 *      4a does not cover this -- a late backfill can hold the largest
 *      truflation_created_at at an old event_time.
 *   5. Every row at that event time goes with it, and every marker in its day.
 *      A shadowed revision left behind would resurrect the value at a lower
 *      created_at. Markers are cleared per day rather than per event time -- see
 *      step 3 for why.
 *   6. Its predecessor sits in the same retention-window-wide bucket, so a flat
 *      run keeps one record per window instead of one in total. Without this an
 *      anchor can end up years older than the point it answers for, and
 *      get_indexed_value_at (055) rejects such an anchor outright rather than
 *      carrying it forward -- a market comparing across an interval would ERROR
 *      where it used to settle. Bucketing by the retention window covers every
 *      interval: one that fits inside the window never reads pruned history, and
 *      one that exceeds it allows a staleness at least a window wide.
 *   7. Neither it nor its predecessor is an event time with two rows sharing its
 *      winning created_at. The readers order by created_at alone, so which value
 *      they resolve to there is the planner's choice; deleting on a guess would
 *      make that choice permanent.
 *
 * WHAT IS UNCHANGED is one thing only, and it is the important one: the value a
 * read resolves to at any time, for primitive and composed streams alike.
 *
 * WHAT CHANGES is wider than "fewer points on a chart", and worth reading before
 * anyone turns `enabled` on:
 *
 *   - A range read returns fewer points: the same step function, fewer vertices.
 *   - Every anchored read reports the anchor's own event_time, and pruning moves
 *     that back to the head of the run. The value is right; the timestamp beside
 *     it is older. This covers get_record's first row, get_last_record, and
 *     get_high_value / get_low_value whenever the extremum is the anchor.
 *   - get_first_record is NOT anchored. It is a forward scan from $after, so
 *     pruning the record it would have returned moves its answer to the next
 *     survivor -- a different VALUE, not just a different timestamp. It is an
 *     attestable action.
 *   - get_daily_ohlc reports the OHLC of whatever is left of a day. The rule is
 *     scoped to event times, not days, so it can take some of a day's records
 *     and leave others; a day that loses any record loses its markers too and
 *     answers from the raw branch, and a day pruned to nothing answers NULL.
 *   - A frozen_at replay of a pruned window no longer reproduces the pruned rows.
 *   - index_change_in_range refuses rather than settles when the anchor it finds
 *     is older than the staleness window it was given (055's get_indexed_value_at
 *     compares the anchor's own event_time). Pruning moves that anchor back, so a
 *     market comparing across an interval on a stream that stayed flat through it
 *     would ERROR where it used to settle FALSE. Nothing on the network is
 *     pruned until an operator turns `enabled` on, and this is the reason not to
 *     do that yet.
 *
 * Modelled on the digest actions in 020: same leader gate, same capped delete
 * with a cap + 1 probe so the caller can resume, same scheduler. What differs is
 * the unit of work. Digest keys on (stream, day) and drains a queue; being a
 * duplicate is a property of a whole stream, so this walks a cursor over
 * streams.id instead.
 */

/**
 * get_duplicate_prune_config: the scheduler's view of the configuration.
 *
 * `enabled` and `prune_schedule` are read here and nowhere else in this file. The
 * actions below are leader-gated and do what they are told, so that an operator
 * can still drain by hand through a signed exec-sql while the scheduled sweep is
 * off — the same division digest draws between `digest_config` and `auto_digest`.
 */
CREATE OR REPLACE ACTION get_duplicate_prune_config()
PUBLIC VIEW RETURNS (
    enabled BOOL,
    retention_days INT,
    prune_schedule TEXT,
    last_stream_ref INT
) {
    for $row in SELECT enabled, retention_days, prune_schedule, last_stream_ref
                FROM duplicate_prune_config WHERE id = 1
    {
        RETURN $row.enabled, $row.retention_days, $row.prune_schedule, $row.last_stream_ref;
    }
    -- Migration 056 seeds the row, so this only answers a network that somehow
    -- lost it. The values are the schema's own defaults.
    RETURN false, 30, '0 */6 * * *', 0;
};

/**
 * batch_prune_duplicates: prune one batch of streams.
 *
 * $delete_cap bounds EVENT TIMES, not rows. Rule 5 makes an event time atomic --
 * every revision at it goes together -- so a cap on rows could not be honoured
 * without splitting one, and primitive_events has no primary key to address a
 * single row by.
 */
CREATE OR REPLACE ACTION batch_prune_duplicates(
    $stream_refs INT[],
    $retention_seconds INT8,
    $delete_cap INT DEFAULT 10000
) PUBLIC RETURNS TABLE(
    deleted_event_times INT,
    deleted_rows INT,
    has_more_to_delete BOOL
) {
    check_leader_authorization();

    if $retention_seconds IS NULL {
        ERROR('retention_seconds must not be NULL');
    }
    if $retention_seconds < 1 {
        ERROR('retention_seconds must be a positive integer (minimum 1), got: ' || $retention_seconds::TEXT);
    }
    if $delete_cap IS NULL {
        ERROR('delete_cap must not be NULL');
    }
    if $delete_cap < 1 {
        ERROR('delete_cap must be a positive integer (minimum 1), got: ' || $delete_cap::TEXT);
    }

    if COALESCE(array_length($stream_refs), 0) = 0 {
        RETURN 0, 0, false;
    }

    $cutoff INT8 := @block_timestamp - $retention_seconds;

    $probe_count INT := 0;
    $deleted_event_times INT := 0;
    $deleted_rows INT := 0;
    $has_more_to_delete BOOL := false;

    -- Step 1: how many event times qualify, and is there a leftover.
    for $result in
    WITH targets AS (
        SELECT r.stream_ref
        FROM UNNEST($stream_refs) AS r(stream_ref)
    ),
    effective AS (
        -- The record a read resolves to at each event time: the greatest
        -- created_at. The value tiebreak is for consensus rather than for
        -- correctness -- primitive_events has carried no primary key since
        -- migration 017, so two rows can share (stream_ref, event_time,
        -- created_at), and every node has to pick the same one.
        SELECT pe.stream_ref, pe.event_time, pe.value,
               ROW_NUMBER() OVER (PARTITION BY pe.stream_ref, pe.event_time
                                  ORDER BY pe.created_at DESC, pe.value DESC) AS rn,
               -- Rule 4b, read as two windows over this same scan rather than as a
               -- separate aggregate and join. The whole rule then costs one pass
               -- over the batch's records instead of three.
               MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref, pe.event_time) AS event_watermark,
               MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref) AS stream_watermark,
               -- How many rows share the winning created_at (counting a NOT NULL
               -- column, because kwil rejects COUNT(*) as a window function).
               -- More than one and
               -- there is no single record a read resolves to: the readers order
               -- by created_at alone, so which value they answer with is the
               -- planner's choice. Deleting on a guess would make that choice
               -- permanent, so an event time like that is left alone.
               COUNT(pe.value) OVER (PARTITION BY pe.stream_ref, pe.event_time, pe.created_at) AS peers
        FROM primitive_events pe
        JOIN targets t ON t.stream_ref = pe.stream_ref
    ),
    series AS (
        SELECT stream_ref, event_time, value, event_watermark, stream_watermark, peers,
               LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_value,
               LAG(event_time) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_event_time,
               LAG(peers) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_peers,
               ROW_NUMBER() OVER (PARTITION BY stream_ref ORDER BY event_time DESC) AS rn_newest
        FROM effective
        WHERE rn = 1
    ),
    watermark_times AS (
        -- The event times holding their stream's greatest truflation_created_at.
        -- A stream with none anywhere has a NULL stream_watermark and selects
        -- nothing here, which is right: it has no Truflation watermark to protect.
        SELECT DISTINCT stream_ref, event_time
        FROM effective
        WHERE stream_watermark IS NOT NULL
          AND event_watermark = stream_watermark
    ),
    deletable AS (
        SELECT s.stream_ref, s.event_time
        FROM series s
        LEFT JOIN watermark_times wt ON wt.stream_ref = s.stream_ref
                                    AND wt.event_time = s.event_time
        WHERE s.previous_value IS NOT NULL
          AND s.value = s.previous_value
          AND s.event_time < $cutoff
          AND s.rn_newest > 1
          AND s.peers = 1
          AND s.previous_peers = 1
          -- Rule 6: keep one record per retention window. Without this a flat run
          -- collapses to its head, and an anchor can end up years older than the
          -- point it answers for -- which get_indexed_value_at (055) rejects
          -- outright rather than carrying forward. Bucketing by the retention
          -- window is what makes that impossible: a market whose interval fits
          -- inside the window never reads pruned history at all, and one whose
          -- interval exceeds it allows a staleness at least that large.
          AND (s.event_time / $retention_seconds) = (s.previous_event_time / $retention_seconds)
          AND wt.stream_ref IS NULL
    ),
    probe AS (
        -- cap + 1, so a full page tells us something was left over. Same trick
        -- batch_digest uses.
        SELECT stream_ref, event_time FROM deletable
        ORDER BY stream_ref ASC, event_time ASC
        LIMIT $delete_cap + 1
    )
    SELECT COUNT(*) AS probe_count FROM probe
    {
        $probe_count := $result.probe_count;
    }

    $has_more_to_delete := $probe_count > $delete_cap;
    $deleted_event_times := LEAST($delete_cap, $probe_count);

    if $deleted_event_times > 0 {
        -- Step 2: count what is about to go, before it goes. Events and markers
        -- together, the way batch_digest reports its own total.
        for $result in
        WITH targets AS (
            SELECT r.stream_ref
            FROM UNNEST($stream_refs) AS r(stream_ref)
        ),
        effective AS (
            -- The record a read resolves to at each event time: the greatest
            -- created_at. The value tiebreak is for consensus rather than for
            -- correctness -- primitive_events has carried no primary key since
            -- migration 017, so two rows can share (stream_ref, event_time,
            -- created_at), and every node has to pick the same one.
            SELECT pe.stream_ref, pe.event_time, pe.value,
                   ROW_NUMBER() OVER (PARTITION BY pe.stream_ref, pe.event_time
                                      ORDER BY pe.created_at DESC, pe.value DESC) AS rn,
                   -- Rule 4b, read as two windows over this same scan rather than as a
                   -- separate aggregate and join. The whole rule then costs one pass
                   -- over the batch's records instead of three.
                   MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref, pe.event_time) AS event_watermark,
                   MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref) AS stream_watermark,
                   -- How many rows share the winning created_at (counting a NOT NULL
               -- column, because kwil rejects COUNT(*) as a window function).
               -- More than one and
                   -- there is no single record a read resolves to: the readers order
                   -- by created_at alone, so which value they answer with is the
                   -- planner's choice. Deleting on a guess would make that choice
                   -- permanent, so an event time like that is left alone.
                   COUNT(pe.value) OVER (PARTITION BY pe.stream_ref, pe.event_time, pe.created_at) AS peers
            FROM primitive_events pe
            JOIN targets t ON t.stream_ref = pe.stream_ref
        ),
        series AS (
            SELECT stream_ref, event_time, value, event_watermark, stream_watermark, peers,
                   LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_value,
                   LAG(event_time) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_event_time,
                   LAG(peers) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_peers,
                   ROW_NUMBER() OVER (PARTITION BY stream_ref ORDER BY event_time DESC) AS rn_newest
            FROM effective
            WHERE rn = 1
        ),
        watermark_times AS (
            -- The event times holding their stream's greatest truflation_created_at.
            -- A stream with none anywhere has a NULL stream_watermark and selects
            -- nothing here, which is right: it has no Truflation watermark to protect.
            SELECT DISTINCT stream_ref, event_time
            FROM effective
            WHERE stream_watermark IS NOT NULL
              AND event_watermark = stream_watermark
        ),
        deletable AS (
            SELECT s.stream_ref, s.event_time
            FROM series s
            LEFT JOIN watermark_times wt ON wt.stream_ref = s.stream_ref
                                        AND wt.event_time = s.event_time
            WHERE s.previous_value IS NOT NULL
              AND s.value = s.previous_value
              AND s.event_time < $cutoff
              AND s.rn_newest > 1
              AND s.peers = 1
              AND s.previous_peers = 1
              -- Rule 6: keep one record per retention window. Without this a flat run
              -- collapses to its head, and an anchor can end up years older than the
              -- point it answers for -- which get_indexed_value_at (055) rejects
              -- outright rather than carrying forward. Bucketing by the retention
              -- window is what makes that impossible: a market whose interval fits
              -- inside the window never reads pruned history at all, and one whose
              -- interval exceeds it allows a staleness at least that large.
              AND (s.event_time / $retention_seconds) = (s.previous_event_time / $retention_seconds)
              AND wt.stream_ref IS NULL
        ),
        chosen AS (
            SELECT stream_ref, event_time FROM deletable
            ORDER BY stream_ref ASC, event_time ASC
            LIMIT $delete_cap
        ),
        deleted_row_probe AS (
            SELECT pe.stream_ref, pe.event_time
            FROM primitive_events pe
            JOIN chosen c ON c.stream_ref = pe.stream_ref AND c.event_time = pe.event_time
            UNION ALL
            SELECT DISTINCT pet.stream_ref, pet.event_time
            FROM primitive_event_type pet
            JOIN chosen c ON c.stream_ref = pet.stream_ref
                         AND pet.event_time >= (c.event_time / 86400) * 86400
                         AND pet.event_time <  (c.event_time / 86400) * 86400 + 86400
        )
        SELECT COUNT(*) AS row_count FROM deleted_row_probe
        {
            $deleted_rows := $result.row_count;
        }

        -- Step 3: markers first. `chosen` is derived from primitive_events, so
        -- deleting the events first would leave this statement recomputing an
        -- empty set and orphaning every marker it was meant to remove.
        --
        -- A whole day's markers go, not just the ones at the chosen event times.
        -- get_daily_ohlc treats a day as digested if ANY surviving marker still
        -- joins a live record, and then reads each of open/high/low/close from
        -- its own marker bit -- so taking one marked record out of a digested day
        -- and leaving the rest would answer NULL for that role beside three real
        -- values, which reads as corruption rather than as absence. Clearing the
        -- day drops it back to the raw branch, which recomputes from whatever
        -- survives. Nothing re-marks it: digest only writes markers for days in
        -- pending_prune_days, and the pruner never enqueues one.
        WITH targets AS (
            SELECT r.stream_ref
            FROM UNNEST($stream_refs) AS r(stream_ref)
        ),
        effective AS (
            -- The record a read resolves to at each event time: the greatest
            -- created_at. The value tiebreak is for consensus rather than for
            -- correctness -- primitive_events has carried no primary key since
            -- migration 017, so two rows can share (stream_ref, event_time,
            -- created_at), and every node has to pick the same one.
            SELECT pe.stream_ref, pe.event_time, pe.value,
                   ROW_NUMBER() OVER (PARTITION BY pe.stream_ref, pe.event_time
                                      ORDER BY pe.created_at DESC, pe.value DESC) AS rn,
                   -- Rule 4b, read as two windows over this same scan rather than as a
                   -- separate aggregate and join. The whole rule then costs one pass
                   -- over the batch's records instead of three.
                   MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref, pe.event_time) AS event_watermark,
                   MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref) AS stream_watermark,
                   -- How many rows share the winning created_at (counting a NOT NULL
               -- column, because kwil rejects COUNT(*) as a window function).
               -- More than one and
                   -- there is no single record a read resolves to: the readers order
                   -- by created_at alone, so which value they answer with is the
                   -- planner's choice. Deleting on a guess would make that choice
                   -- permanent, so an event time like that is left alone.
                   COUNT(pe.value) OVER (PARTITION BY pe.stream_ref, pe.event_time, pe.created_at) AS peers
            FROM primitive_events pe
            JOIN targets t ON t.stream_ref = pe.stream_ref
        ),
        series AS (
            SELECT stream_ref, event_time, value, event_watermark, stream_watermark, peers,
                   LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_value,
                   LAG(event_time) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_event_time,
                   LAG(peers) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_peers,
                   ROW_NUMBER() OVER (PARTITION BY stream_ref ORDER BY event_time DESC) AS rn_newest
            FROM effective
            WHERE rn = 1
        ),
        watermark_times AS (
            -- The event times holding their stream's greatest truflation_created_at.
            -- A stream with none anywhere has a NULL stream_watermark and selects
            -- nothing here, which is right: it has no Truflation watermark to protect.
            SELECT DISTINCT stream_ref, event_time
            FROM effective
            WHERE stream_watermark IS NOT NULL
              AND event_watermark = stream_watermark
        ),
        deletable AS (
            SELECT s.stream_ref, s.event_time
            FROM series s
            LEFT JOIN watermark_times wt ON wt.stream_ref = s.stream_ref
                                        AND wt.event_time = s.event_time
            WHERE s.previous_value IS NOT NULL
              AND s.value = s.previous_value
              AND s.event_time < $cutoff
              AND s.rn_newest > 1
              AND s.peers = 1
              AND s.previous_peers = 1
              -- Rule 6: keep one record per retention window. Without this a flat run
              -- collapses to its head, and an anchor can end up years older than the
              -- point it answers for -- which get_indexed_value_at (055) rejects
              -- outright rather than carrying forward. Bucketing by the retention
              -- window is what makes that impossible: a market whose interval fits
              -- inside the window never reads pruned history at all, and one whose
              -- interval exceeds it allows a staleness at least that large.
              AND (s.event_time / $retention_seconds) = (s.previous_event_time / $retention_seconds)
              AND wt.stream_ref IS NULL
        ),
        chosen AS (
            SELECT stream_ref, event_time FROM deletable
            ORDER BY stream_ref ASC, event_time ASC
            LIMIT $delete_cap
        )
        DELETE FROM primitive_event_type
        WHERE EXISTS (
            SELECT 1 FROM chosen c
            WHERE c.stream_ref = primitive_event_type.stream_ref
              AND primitive_event_type.event_time >= (c.event_time / 86400) * 86400
              AND primitive_event_type.event_time <  (c.event_time / 86400) * 86400 + 86400
        );

        -- Step 4: every row at those event times, shadowed revisions included.
        WITH targets AS (
            SELECT r.stream_ref
            FROM UNNEST($stream_refs) AS r(stream_ref)
        ),
        effective AS (
            -- The record a read resolves to at each event time: the greatest
            -- created_at. The value tiebreak is for consensus rather than for
            -- correctness -- primitive_events has carried no primary key since
            -- migration 017, so two rows can share (stream_ref, event_time,
            -- created_at), and every node has to pick the same one.
            SELECT pe.stream_ref, pe.event_time, pe.value,
                   ROW_NUMBER() OVER (PARTITION BY pe.stream_ref, pe.event_time
                                      ORDER BY pe.created_at DESC, pe.value DESC) AS rn,
                   -- Rule 4b, read as two windows over this same scan rather than as a
                   -- separate aggregate and join. The whole rule then costs one pass
                   -- over the batch's records instead of three.
                   MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref, pe.event_time) AS event_watermark,
                   MAX(pe.truflation_created_at) OVER (PARTITION BY pe.stream_ref) AS stream_watermark,
                   -- How many rows share the winning created_at (counting a NOT NULL
               -- column, because kwil rejects COUNT(*) as a window function).
               -- More than one and
                   -- there is no single record a read resolves to: the readers order
                   -- by created_at alone, so which value they answer with is the
                   -- planner's choice. Deleting on a guess would make that choice
                   -- permanent, so an event time like that is left alone.
                   COUNT(pe.value) OVER (PARTITION BY pe.stream_ref, pe.event_time, pe.created_at) AS peers
            FROM primitive_events pe
            JOIN targets t ON t.stream_ref = pe.stream_ref
        ),
        series AS (
            SELECT stream_ref, event_time, value, event_watermark, stream_watermark, peers,
                   LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_value,
                   LAG(event_time) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_event_time,
                   LAG(peers) OVER (PARTITION BY stream_ref ORDER BY event_time ASC) AS previous_peers,
                   ROW_NUMBER() OVER (PARTITION BY stream_ref ORDER BY event_time DESC) AS rn_newest
            FROM effective
            WHERE rn = 1
        ),
        watermark_times AS (
            -- The event times holding their stream's greatest truflation_created_at.
            -- A stream with none anywhere has a NULL stream_watermark and selects
            -- nothing here, which is right: it has no Truflation watermark to protect.
            SELECT DISTINCT stream_ref, event_time
            FROM effective
            WHERE stream_watermark IS NOT NULL
              AND event_watermark = stream_watermark
        ),
        deletable AS (
            SELECT s.stream_ref, s.event_time
            FROM series s
            LEFT JOIN watermark_times wt ON wt.stream_ref = s.stream_ref
                                        AND wt.event_time = s.event_time
            WHERE s.previous_value IS NOT NULL
              AND s.value = s.previous_value
              AND s.event_time < $cutoff
              AND s.rn_newest > 1
              AND s.peers = 1
              AND s.previous_peers = 1
              -- Rule 6: keep one record per retention window. Without this a flat run
              -- collapses to its head, and an anchor can end up years older than the
              -- point it answers for -- which get_indexed_value_at (055) rejects
              -- outright rather than carrying forward. Bucketing by the retention
              -- window is what makes that impossible: a market whose interval fits
              -- inside the window never reads pruned history at all, and one whose
              -- interval exceeds it allows a staleness at least that large.
              AND (s.event_time / $retention_seconds) = (s.previous_event_time / $retention_seconds)
              AND wt.stream_ref IS NULL
        ),
        chosen AS (
            SELECT stream_ref, event_time FROM deletable
            ORDER BY stream_ref ASC, event_time ASC
            LIMIT $delete_cap
        )
        DELETE FROM primitive_events
        WHERE EXISTS (
            SELECT 1 FROM chosen c
            WHERE c.stream_ref = primitive_events.stream_ref
              AND c.event_time = primitive_events.event_time
        );
    }

    RETURN $deleted_event_times, $deleted_rows, $has_more_to_delete;
};

/**
 * auto_prune_duplicates: take the next slice of the sweep and prune it.
 *
 * The sweep is cyclic, so has_more_to_delete means "the cursor has not reached
 * the end of a pass", not "there is definitely something left to delete". A
 * caller drains with its own bounded loop, the way the digest scheduler does.
 *
 * A NULL $retention_days reads the configured value, so an operator has one place
 * to set it while a manual drain can still override it. `enabled` is deliberately
 * not read here: these actions are leader-gated and do what they are told, so a
 * hand-paced drain through a signed exec-sql stays possible while the scheduled
 * sweep is off.
 */
CREATE OR REPLACE ACTION auto_prune_duplicates(
    $delete_cap INT DEFAULT 10000,
    $stream_batch_size INT DEFAULT 100,
    $retention_days INT DEFAULT NULL
) PUBLIC RETURNS TABLE(
    swept_streams INT,
    deleted_event_times INT,
    deleted_rows INT,
    has_more_to_delete BOOL
) {
    check_leader_authorization();

    if $stream_batch_size IS NULL {
        ERROR('stream_batch_size must not be NULL');
    }
    if $stream_batch_size < 1 {
        ERROR('stream_batch_size must be a positive integer (minimum 1), got: ' || $stream_batch_size::TEXT);
    }
    -- Checked here as well as in batch_prune_duplicates, because the no-streams
    -- path below returns before ever calling it and a bad cap would look accepted.
    if $delete_cap IS NULL {
        ERROR('delete_cap must not be NULL');
    }
    if $delete_cap < 1 {
        ERROR('delete_cap must be a positive integer (minimum 1), got: ' || $delete_cap::TEXT);
    }

    $cursor INT := 0;
    $effective_retention_days INT;
    if $retention_days IS NOT NULL {
        $effective_retention_days := $retention_days;
    }
    for $config in SELECT last_stream_ref, retention_days FROM duplicate_prune_config WHERE id = 1 {
        $cursor := $config.last_stream_ref;
        if $effective_retention_days IS NULL {
            $effective_retention_days := $config.retention_days;
        }
    }

    if $effective_retention_days IS NULL {
        ERROR('retention_days is not set: pass it explicitly or seed duplicate_prune_config');
    }
    if $effective_retention_days < 1 {
        ERROR('retention_days must be a positive integer (minimum 1), got: ' || $effective_retention_days::TEXT);
    }

    -- Composed streams hold no primitive_events, so they are skipped rather than
    -- swept over. There is no (stream_type, id) index, so the type filter is
    -- applied inside the id range rather than the other way round.
    $stream_refs INT[];
    $new_cursor INT := $cursor;
    for $batch in
    SELECT ARRAY_AGG(id ORDER BY id ASC) AS refs, MAX(id) AS max_id
    FROM (
        SELECT id FROM streams
        WHERE id > $cursor AND stream_type = 'primitive'
        ORDER BY id ASC
        LIMIT $stream_batch_size
    ) s
    {
        $stream_refs := $batch.refs;
        $new_cursor := $batch.max_id;
    }

    -- End of a pass: start the next one from the beginning.
    if COALESCE(array_length($stream_refs), 0) = 0 {
        $cursor := 0;
        for $batch in
        SELECT ARRAY_AGG(id ORDER BY id ASC) AS refs, MAX(id) AS max_id
        FROM (
            SELECT id FROM streams
            WHERE stream_type = 'primitive'
            ORDER BY id ASC
            LIMIT $stream_batch_size
        ) s
        {
            $stream_refs := $batch.refs;
            $new_cursor := $batch.max_id;
        }
    }

    if COALESCE(array_length($stream_refs), 0) = 0 {
        -- No primitive streams on this network at all.
        UPDATE duplicate_prune_config
        SET last_stream_ref = 0, updated_at_height = @height
        WHERE id = 1;
        emit_auto_prune_notice(0, 0, 0, false);
        RETURN 0, 0, 0, false;
    }

    $swept_streams INT := array_length($stream_refs);
    $deleted_event_times INT := 0;
    $deleted_rows INT := 0;
    $batch_has_more BOOL := false;
    $retention_seconds INT8 := $effective_retention_days::INT8 * 86400;

    for $result in batch_prune_duplicates($stream_refs, $retention_seconds, $delete_cap) {
        $deleted_event_times := $result.deleted_event_times;
        $deleted_rows := $result.deleted_rows;
        $batch_has_more := $result.has_more_to_delete;
    }

    -- A capped batch leaves these streams unfinished, so the cursor stays put and
    -- the next call finishes them before moving on.
    $next_cursor INT := $new_cursor;
    if $batch_has_more {
        $next_cursor := $cursor;
    }

    UPDATE duplicate_prune_config
    SET last_stream_ref = $next_cursor, updated_at_height = @height
    WHERE id = 1;

    $has_more BOOL := false;
    if $batch_has_more {
        $has_more := true;
    }
    for $remaining in
    SELECT COUNT(*) AS streams_left
    FROM (
        SELECT id FROM streams
        WHERE id > $next_cursor AND stream_type = 'primitive'
        LIMIT 1
    ) s
    {
        if $remaining.streams_left > 0 {
            $has_more := true;
        }
    }

    emit_auto_prune_notice($swept_streams, $deleted_event_times, $deleted_rows, $has_more);
    RETURN $swept_streams, $deleted_event_times, $deleted_rows, $has_more;
};

-- Private helper emitting a structured NOTICE so a scheduler broadcasting this
-- action can read the outcome out of the transaction log. An action's return
-- value is not otherwise visible to an SDK caller.
CREATE OR REPLACE ACTION emit_auto_prune_notice(
    $swept_streams INT,
    $deleted_event_times INT,
    $deleted_rows INT,
    $has_more BOOL
) PRIVATE VIEW {
    $has_more_text := 'false';
    if $has_more {
        $has_more_text := 'true';
    }
    NOTICE('auto_prune_duplicates:' ||
        '{"swept_streams":' || $swept_streams::TEXT ||
        ',"deleted_event_times":' || $deleted_event_times::TEXT ||
        ',"deleted_rows":' || $deleted_rows::TEXT ||
        ',"has_more_to_delete":' || $has_more_text || '}');
};
