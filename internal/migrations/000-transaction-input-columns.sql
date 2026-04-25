/* ============================================================================
 * Transaction Tracking Columns
 * ============================================================================
 * Adds tx_id columns to the four data tables so transaction-effect tracking
 * actions (insertRecords, deployStream, setTaxonomies, setMetadata, and the
 * get_transaction_* read paths in 028-transaction-input-actions.sql) can map
 * a row back to the tx that produced it.
 *
 * Why this is its own file (not part of 000-initial-data.sql):
 * 000-initial-data.sql contains the original CREATE TABLE / CREATE INDEX
 * statements written against the un-normalized schema. Migration 017 dropped
 * `data_provider` from taxonomies/metadata/primitive_events, so re-running
 * 000-initial-data.sql now hits "column data_provider does not exist" partway
 * through and rolls back the whole multi-statement tx — including any
 * additions tacked onto the end. Splitting the tx_id additions into their
 * own file keeps them landable on existing nodes (mainnet) without depending
 * on 000-initial-data.sql succeeding end-to-end. Every statement here is
 * IF NOT EXISTS so re-runs are no-ops.
 *
 * See 0MainnetPredictionMarket/9MainnetTxIdRecovery-2026-04-25.md for the
 * incident this was extracted to fix.
 */

-- streams: trace which deployStream tx created each stream row
ALTER TABLE streams ADD COLUMN IF NOT EXISTS tx_id TEXT;
CREATE INDEX IF NOT EXISTS streams_tx_id_idx ON streams (tx_id);

-- primitive_events: trace which insertRecords tx wrote each data point
ALTER TABLE primitive_events ADD COLUMN IF NOT EXISTS tx_id TEXT;
CREATE INDEX IF NOT EXISTS pe_tx_id_idx ON primitive_events (tx_id);

-- taxonomies: trace which setTaxonomies tx introduced each taxonomy row
ALTER TABLE taxonomies ADD COLUMN IF NOT EXISTS tx_id TEXT;
CREATE INDEX IF NOT EXISTS tax_tx_id_idx ON taxonomies (tx_id);

-- metadata: trace which setMetadata tx wrote each key/value
ALTER TABLE metadata ADD COLUMN IF NOT EXISTS tx_id TEXT;
CREATE INDEX IF NOT EXISTS meta_tx_id_idx ON metadata (tx_id);
