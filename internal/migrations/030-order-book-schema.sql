/*
 * ORDER BOOK SCHEMA MIGRATION
 *
 * Creates the database schema for the prediction market order book:
 * - ob_queries: Market definitions with settlement parameters
 * - ob_participants: Wallet address to ID mapping
 * - ob_positions: Order book and holdings
 */

-- =============================================================================
-- ob_queries: Market definitions
-- =============================================================================
-- Each row represents a prediction market with settlement parameters.
-- Markets are identified by a unique SHA256 hash of the attestation query.
--
-- ID generation uses MAX(id) + 1 pattern (no sequences in Kuneiform).
-- Note: This is safe in Kwil because transactions within a block are processed
-- sequentially by the consensus engine, not concurrently. The UNIQUE constraint
-- on hash also creates an implicit index for fast lookups.
CREATE TABLE IF NOT EXISTS ob_queries (
    id INT PRIMARY KEY,
    hash BYTEA NOT NULL UNIQUE,
    settle_time INT8 NOT NULL,
    settled BOOLEAN DEFAULT false NOT NULL,
    winning_outcome BOOLEAN,
    settled_at INT8,
    max_spread INT NOT NULL DEFAULT 5,
    min_order_size INT8 NOT NULL DEFAULT 20,
    created_at INT8 NOT NULL,
    creator BYTEA NOT NULL,
    bridge TEXT NOT NULL,

    CONSTRAINT chk_ob_queries_max_spread CHECK (max_spread >= 1 AND max_spread <= 50),
    CONSTRAINT chk_ob_queries_min_order CHECK (min_order_size >= 1)
);

-- =============================================================================
-- ob_participants: Wallet lookup table
-- =============================================================================
-- Maps wallet addresses to compact integer IDs for efficient FK references.
-- One row per unique user who participates in any market.
-- The UNIQUE constraint on wallet_address prevents duplicates and creates an
-- implicit index. ID generation uses MAX(id) + 1 pattern (safe in Kwil due to
-- sequential transaction processing within blocks).
CREATE TABLE IF NOT EXISTS ob_participants (
    id INT PRIMARY KEY,
    wallet_address BYTEA NOT NULL UNIQUE
);

-- =============================================================================
-- ob_positions: Order book and holdings
-- =============================================================================
-- Unified table for orders and holdings. Price semantics:
-- - price = 0: Holding (shares owned, not listed for sale)
-- - price > 0: Sell order at that price (1-99 cents)
-- - price < 0: Buy order at that price (1-99 cents, stored as negative)
CREATE TABLE IF NOT EXISTS ob_positions (
    query_id INT NOT NULL,
    participant_id INT NOT NULL,
    outcome BOOLEAN NOT NULL,
    price INT NOT NULL,
    amount INT8 NOT NULL,
    last_updated INT8 NOT NULL,

    PRIMARY KEY (query_id, participant_id, outcome, price),
    FOREIGN KEY (query_id) REFERENCES ob_queries(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES ob_participants(id) ON DELETE CASCADE,
    CONSTRAINT chk_ob_positions_price CHECK (price >= -99 AND price <= 99),
    CONSTRAINT chk_ob_positions_amount CHECK (amount > 0)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================
-- Note: ob_participants.wallet_address already has implicit index from UNIQUE constraint
-- Note: ob_queries.hash already has implicit index from UNIQUE constraint

-- Order book queries: find best orders for matching (by query, outcome, price, FIFO)
CREATE INDEX IF NOT EXISTS idx_ob_pos_order_match
    ON ob_positions(query_id, outcome, price, last_updated);

-- User portfolio queries: find all positions for a participant
CREATE INDEX IF NOT EXISTS idx_ob_positions_participant
    ON ob_positions(participant_id);

-- Settlement queries: find markets by settle_time
CREATE INDEX IF NOT EXISTS idx_ob_queries_settle_time
    ON ob_queries(settle_time);

-- Settled status index for filtering active/settled markets
CREATE INDEX IF NOT EXISTS idx_ob_queries_settled
    ON ob_queries(settled);

-- =============================================================================
-- Transaction method registration
-- =============================================================================

-- Register new transaction method for createMarket
INSERT INTO transaction_methods (method_id, name) VALUES
    (8, 'createMarket')
ON CONFLICT (method_id) DO NOTHING;
