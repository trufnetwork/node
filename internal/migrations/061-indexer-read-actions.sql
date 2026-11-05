/*
 * MIGRATION 061: READ ACTIONS FOR INDEXERS
 *
 * Public views an indexer pages through to mirror the order book and the fee
 * ledgers by calling actions rather than sending ad-hoc SQL. Each one reads
 * tables that earlier migrations create and changes none of them:
 *
 *   list_markets_after             ob_queries (030)
 *   get_open_interest_by_bridge    ob_queries, ob_positions (030)
 *   get_market_positions           ob_positions, ob_participants (030)
 *   list_markets_with_positions    ob_positions (030)
 *   list_fee_distributions         ob_fee_distributions (036), ob_queries
 *   get_latest_fee_distribution    ob_fee_distributions (036), ob_queries
 *   list_fee_distribution_details  ob_fee_distribution_details (036)
 *   list_net_impacts               ob_net_impacts (043), ob_participants
 *   list_order_events              ob_order_events (044), ob_participants
 *   get_order_book_max_ids         ob_net_impacts (043), ob_order_events (044)
 *   list_transaction_events        transaction_events (026)
 *
 * Paging. Each paged action returns rows in the order of a key and takes the
 * key of the last row already read as its cursor, so the next call starts
 * right after that row. A cursor never skips a row, wherever a page boundary
 * falls. A NULL in a cursor sorts before every value: an all-NULL cursor
 * starts at the first row, and a cursor whose later parts are NULL starts at
 * the first row that matches its earlier parts. The ids these tables generate
 * start at 1.
 *
 * Page size. $limit is between 1 and 1000, and NULL means 1000. A larger value
 * is refused rather than cut down: a caller that reads "fewer rows than I
 * asked for" as "no rows left" would otherwise stop at the cap and believe it
 * had read everything.
 *
 * Every cursor walks a primary key or an existing index; list_order_events
 * walks the block_height index and sorts the events of each block by id.
 * get_open_interest_by_bridge reads every market and every open position, and
 * list_markets_with_positions every open position: settlement deletes a
 * market's positions, so that is the open book, not its history.
 *
 * References no bridge namespace, so there is no dev/prod twin.
 */

-- =============================================================================
-- list_markets_after: markets in id order, after a cursor.
-- =============================================================================
/**
 * Every column of ob_queries. settled, winning_outcome and settled_at change
 * when a market settles, so a reader that wants those changes walks the table
 * again from the start rather than reading only past its last id.
 *
 * Usage:
 *   kwil-cli call-action list_markets_after int:null int:1000
 */
CREATE OR REPLACE ACTION list_markets_after(
    $after_id INT,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    id INT,
    hash BYTEA,
    query_components BYTEA,
    settle_time INT8,
    settled BOOL,
    winning_outcome BOOL,
    settled_at INT8,
    created_at INT8,
    creator BYTEA,
    max_spread INT,
    min_order_size INT8,
    bridge TEXT
) {
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }
    if $after_id IS NULL { $after_id := 0; }

    RETURN SELECT id, hash, query_components, settle_time, settled, winning_outcome,
                  settled_at, created_at, creator, max_spread, min_order_size, bridge
    FROM ob_queries
    WHERE id > $after_id
    ORDER BY id ASC
    LIMIT $limit;
};

-- =============================================================================
-- get_open_interest_by_bridge: contracts outstanding on each bridge.
-- =============================================================================
/**
 * Counted on the node, so the answer is one row per bridge however large the
 * book grows.
 *
 * A contract is a YES/NO pair, so counting the YES side counts each contract
 * once. price tells holdings (0) and asks (> 0), which are shares that exist,
 * from bids (< 0), which are collateral locked for shares that do not exist
 * yet; bids are left out. Settled markets count nothing.
 *
 * Grouped by bridge because each bridge's collateral has its own unit, so the
 * totals are never added across bridges. The settled test sits inside the
 * CASE rather than in a WHERE clause, so a bridge whose markets have all
 * settled still returns a row, of zero, instead of no row.
 *
 * Usage:
 *   kwil-cli call-action get_open_interest_by_bridge
 */
CREATE OR REPLACE ACTION get_open_interest_by_bridge()
PUBLIC VIEW RETURNS TABLE(
    bridge TEXT,
    open_interest INT8,
    markets_with_oi INT
) {
    RETURN SELECT
        q.bridge,
        COALESCE(SUM(CASE WHEN NOT q.settled AND p.outcome AND p.price >= 0
                          THEN p.amount END)::INT8, 0::INT8) AS open_interest,
        COUNT(DISTINCT CASE WHEN NOT q.settled AND p.outcome AND p.price >= 0
                            THEN p.query_id END)::INT AS markets_with_oi
    FROM ob_queries q
    LEFT JOIN ob_positions p ON p.query_id = q.id
    GROUP BY q.bridge
    ORDER BY q.bridge ASC;
};

-- =============================================================================
-- get_market_positions: one market's positions with each holder's wallet.
-- =============================================================================
/**
 * Holdings (price 0), asks (price > 0) and bids (price < 0), in primary key
 * order. Read to the end, this is the market's whole book: the rows
 * get_order_book returns for both outcomes, plus holdings.
 *
 * The cursor is the last row's (participant_id, outcome, price), the primary
 * key of ob_positions after query_id. One holder can have rows on both
 * outcomes and at several prices, so a cursor on the participant alone would
 * skip the rest of a holder that a page boundary splits. FALSE sorts before
 * TRUE. ($p, NULL, NULL) starts at the first row of participant $p, and
 * ($p, $o, NULL) at its first row on outcome $o.
 *
 * Usage:
 *   kwil-cli call-action get_market_positions int:1 int:null bool:null int:null int:1000
 */
CREATE OR REPLACE ACTION get_market_positions(
    $query_id INT,
    $after_participant_id INT,
    $after_outcome BOOL,
    $after_price INT,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    participant_id INT,
    wallet_address BYTEA,
    outcome BOOL,
    price INT,
    amount INT8
) {
    if $query_id IS NULL { ERROR('query_id is required'); }
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }
    if $after_participant_id IS NULL { $after_participant_id := 0; }

    RETURN SELECT p.participant_id, part.wallet_address, p.outcome, p.price, p.amount
    FROM ob_positions p
    LEFT JOIN ob_participants part ON part.id = p.participant_id
    WHERE p.query_id = $query_id
      AND p.participant_id >= $after_participant_id
      AND (p.participant_id > $after_participant_id
           OR $after_outcome IS NULL
           OR p.outcome > $after_outcome
           OR (p.outcome = $after_outcome
               AND ($after_price IS NULL OR p.price > $after_price)))
    ORDER BY p.participant_id ASC, p.outcome ASC, p.price ASC
    LIMIT $limit;
};

-- =============================================================================
-- list_markets_with_positions: ids of the markets that hold a position.
-- =============================================================================
/**
 * Ascending. Settlement deletes a market's positions, so every market listed
 * is one that has not settled.
 *
 * Usage:
 *   kwil-cli call-action list_markets_with_positions
 */
CREATE OR REPLACE ACTION list_markets_with_positions()
PUBLIC VIEW RETURNS TABLE(
    query_id INT
) {
    RETURN SELECT DISTINCT query_id
    FROM ob_positions
    ORDER BY query_id ASC;
};

-- =============================================================================
-- list_fee_distributions: fee distributions in id order, after a cursor.
-- =============================================================================
/**
 * Each distribution with its market's bridge. A distribution is written once
 * and never changed, so a reader can keep its last id and read only what
 * follows.
 *
 * Usage:
 *   kwil-cli call-action list_fee_distributions int:0 int:500
 */
CREATE OR REPLACE ACTION list_fee_distributions(
    $after_id INT,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    id INT,
    query_id INT,
    bridge TEXT,
    total_fees_distributed NUMERIC(78, 0),
    total_dp_fees NUMERIC(78, 0),
    total_validator_fees NUMERIC(78, 0),
    total_lp_count INT,
    block_count INT,
    distributed_at INT8
) {
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }
    if $after_id IS NULL { $after_id := 0; }

    RETURN SELECT d.id, d.query_id, q.bridge, d.total_fees_distributed, d.total_dp_fees,
                  d.total_validator_fees, d.total_lp_count, d.block_count, d.distributed_at
    FROM ob_fee_distributions d
    LEFT JOIN ob_queries q ON q.id = d.query_id
    WHERE d.id > $after_id
    ORDER BY d.id ASC
    LIMIT $limit;
};

-- =============================================================================
-- get_latest_fee_distribution: the distribution with the highest id.
-- =============================================================================
/**
 * The columns of list_fee_distributions, for the highest id, or no row when
 * there is no distribution. The id and the rest of its row arrive in one
 * answer: behind a gateway two separate reads can reach two different nodes,
 * and a reader checking the id needs the row of the node that reported it.
 *
 * Usage:
 *   kwil-cli call-action get_latest_fee_distribution
 */
CREATE OR REPLACE ACTION get_latest_fee_distribution()
PUBLIC VIEW RETURNS TABLE(
    id INT,
    query_id INT,
    bridge TEXT,
    total_fees_distributed NUMERIC(78, 0),
    total_dp_fees NUMERIC(78, 0),
    total_validator_fees NUMERIC(78, 0),
    total_lp_count INT,
    block_count INT,
    distributed_at INT8
) {
    RETURN SELECT d.id, d.query_id, q.bridge, d.total_fees_distributed, d.total_dp_fees,
                  d.total_validator_fees, d.total_lp_count, d.block_count, d.distributed_at
    FROM ob_fee_distributions d
    LEFT JOIN ob_queries q ON q.id = d.query_id
    ORDER BY d.id DESC
    LIMIT 1;
};

-- =============================================================================
-- list_fee_distribution_details: per-wallet rewards of each distribution.
-- =============================================================================
/**
 * In (distribution_id, participant_id) order, with the distribution's market,
 * time and sampled block count. wallet_address is the copy the detail row
 * stored when it was written.
 *
 * The cursor is the last row's (distribution_id, participant_id). One
 * distribution can hold more rows than a page, so a cursor on the
 * distribution id alone would skip the rest of a distribution that a page
 * boundary splits. ($d, NULL) starts at the first row of distribution $d.
 *
 * Usage:
 *   kwil-cli call-action list_fee_distribution_details int:null int:null int:1000
 */
CREATE OR REPLACE ACTION list_fee_distribution_details(
    $after_distribution_id INT,
    $after_participant_id INT,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    distribution_id INT,
    query_id INT,
    participant_id INT,
    wallet_address BYTEA,
    total_reward_percent NUMERIC(10, 2),
    reward_amount NUMERIC(78, 0),
    distributed_at INT8,
    block_count INT
) {
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }
    if $after_distribution_id IS NULL { $after_distribution_id := 0; }

    RETURN SELECT det.distribution_id, d.query_id, det.participant_id, det.wallet_address,
                  det.total_reward_percent, det.reward_amount, d.distributed_at, d.block_count
    FROM ob_fee_distribution_details det
    JOIN ob_fee_distributions d ON d.id = det.distribution_id
    WHERE det.distribution_id >= $after_distribution_id
      AND (det.distribution_id > $after_distribution_id
           OR $after_participant_id IS NULL
           OR det.participant_id > $after_participant_id)
    ORDER BY det.distribution_id ASC, det.participant_id ASC
    LIMIT $limit;
};

-- =============================================================================
-- list_net_impacts: net position and collateral changes in id order.
-- =============================================================================
/**
 * Rows of ob_net_impacts with each participant's wallet. collateral_change is
 * a magnitude and is_negative its sign.
 *
 * Usage:
 *   kwil-cli call-action list_net_impacts int:0 int:1000
 */
CREATE OR REPLACE ACTION list_net_impacts(
    $after_id INT,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    id INT,
    tx_hash BYTEA,
    query_id INT,
    participant_id INT,
    wallet_address BYTEA,
    outcome BOOL,
    shares_change INT8,
    collateral_change NUMERIC(78, 0),
    is_negative BOOL,
    timestamp INT8
) {
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }
    if $after_id IS NULL { $after_id := 0; }

    RETURN SELECT n.id, n.tx_hash, n.query_id, n.participant_id, part.wallet_address,
                  n.outcome, n.shares_change, n.collateral_change, n.is_negative, n.timestamp
    FROM ob_net_impacts n
    LEFT JOIN ob_participants part ON part.id = n.participant_id
    WHERE n.id > $after_id
    ORDER BY n.id ASC
    LIMIT $limit;
};

-- =============================================================================
-- list_order_events: order events in block order, after a cursor.
-- =============================================================================
/**
 * Rows of ob_order_events with the acting participant's wallet, in
 * (block_height, id) order. The node trims events older than a retention
 * window (044), so it holds only recent ones.
 *
 * The cursor is the last row's (block_height, id). Ids are MAX(id) + 1, so a
 * trim that removes every event makes them start again at 1, and new events
 * can then take ids at or below a reader's cursor. A call sees only committed
 * blocks, so every event recorded after a page was read is in a later block
 * than that page's last row, and a cursor that leads with the block height
 * reads it whatever its id. An id alone names an event only until such a
 * restart; (block_height, id) names it for good. ($h, NULL) starts at the
 * first event of block $h.
 *
 * Usage:
 *   kwil-cli call-action list_order_events int:null int:null int:1000
 */
CREATE OR REPLACE ACTION list_order_events(
    $after_height INT8,
    $after_id INT8,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    id INT8,
    tx_hash BYTEA,
    query_id INT,
    participant_id INT,
    wallet_address BYTEA,
    event_type TEXT,
    outcome BOOL,
    price INT,
    amount INT8,
    counterparty_id INT,
    block_height INT8,
    block_timestamp INT8
) {
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }
    -- Block heights are never negative, so -1 starts before every event.
    if $after_height IS NULL { $after_height := -1; }

    RETURN SELECT e.id, e.tx_hash, e.query_id, e.participant_id, part.wallet_address,
                  e.event_type, e.outcome, e.price, e.amount, e.counterparty_id,
                  e.block_height, e.block_timestamp
    FROM ob_order_events e
    LEFT JOIN ob_participants part ON part.id = e.participant_id
    WHERE e.block_height >= $after_height
      AND (e.block_height > $after_height
           OR $after_id IS NULL
           OR e.id > $after_id)
    ORDER BY e.block_height ASC, e.id ASC
    LIMIT $limit;
};

-- =============================================================================
-- get_order_book_max_ids: the highest net impact id and the newest order event.
-- =============================================================================
/**
 * max_net_impact_id is the highest net impact id. max_order_event_height and
 * max_order_event_id are the block height and id of the newest order event,
 * the last row list_order_events returns. Each is NULL when its table is
 * empty.
 *
 * A reader compares them with its cursors before paging with
 * list_net_impacts and list_order_events. There are order events to read when
 * max_order_event_height is above the cursor's height, or equal to it with
 * max_order_event_id above the cursor's id. The id alone cannot tell: after a
 * trim restarts the ids, the newest id can be below, at or above the cursor's
 * id while the reader has read none of the events.
 *
 * Usage:
 *   kwil-cli call-action get_order_book_max_ids
 */
CREATE OR REPLACE ACTION get_order_book_max_ids()
PUBLIC VIEW RETURNS (
    max_net_impact_id INT,
    max_order_event_height INT8,
    max_order_event_id INT8
) {
    $max_net_impact_id INT;
    for $n in SELECT MAX(id)::INT AS max_id FROM ob_net_impacts {
        $max_net_impact_id := $n.max_id;
    }

    $max_order_event_height INT8;
    $max_order_event_id INT8;
    for $e in SELECT block_height, id FROM ob_order_events
              ORDER BY block_height DESC, id DESC
              LIMIT 1 {
        $max_order_event_height := $e.block_height;
        $max_order_event_id := $e.id;
    }

    RETURN $max_net_impact_id, $max_order_event_height, $max_order_event_id;
};

-- =============================================================================
-- list_transaction_events: the transaction fee ledger in block order.
-- =============================================================================
/**
 * Rows of transaction_events in (block_height, tx_id) order, up to and
 * including $to_height; NULL $to_height has no upper bound.
 *
 * The cursor is the last row's (block_height, tx_id). ($h, NULL) starts at the
 * first row of block $h, so ($lo, NULL, $hi) reads the blocks lo to hi, both
 * included.
 *
 * The node trims old insertRecords rows (method_id 2) after a retention window
 * (052), so their older history is no longer here to read.
 *
 * Usage (blocks 1200 to 5000, first page):
 *   kwil-cli call-action list_transaction_events int:1200 text:null int:5000 int:500
 */
CREATE OR REPLACE ACTION list_transaction_events(
    $after_height INT8,
    $after_tx_id TEXT,
    $to_height INT8,
    $limit INT
) PUBLIC VIEW RETURNS TABLE(
    tx_id TEXT,
    block_height INT8,
    method_id INT,
    caller TEXT,
    fee_amount NUMERIC(78, 0),
    fee_recipient TEXT,
    metadata TEXT
) {
    if $limit IS NULL { $limit := 1000; }
    if $limit < 1 OR $limit > 1000 { ERROR('limit must be between 1 and 1000'); }

    -- Block heights are never negative, so -1 starts before every row. Both
    -- bounds stay concrete numbers so the read is a range on the
    -- (block_height, tx_id) index.
    if $after_height IS NULL { $after_height := -1; }
    if $to_height IS NULL { $to_height := 9223372036854775807; }

    RETURN SELECT tx_id, block_height, method_id, caller, fee_amount, fee_recipient, metadata
    FROM transaction_events
    WHERE block_height >= $after_height
      AND block_height <= $to_height
      AND (block_height > $after_height
           OR $after_tx_id IS NULL
           OR tx_id > $after_tx_id)
    ORDER BY block_height ASC, tx_id ASC
    LIMIT $limit;
};
