-- This file exists to manually enable the ERC20 bootstrap process for our tests

USE kwil_erc20_meta as kwil_erc20_meta;

-- ordered-sync is provisioned by its genesis hook in node runtime. The test harness
-- does not run genesis, so we create the namespace and schema explicitly for tests.
CREATE NAMESPACE IF NOT EXISTS kwil_ordered_sync;
SET CURRENT NAMESPACE TO kwil_ordered_sync;

CREATE TABLE IF NOT EXISTS topics (
    id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    resolve_func TEXT NOT NULL,
    last_processed_point int8
);

CREATE TABLE IF NOT EXISTS pending_data (
    point int8,
    topic_id UUID REFERENCES topics(id) ON UPDATE CASCADE ON DELETE CASCADE,
    previous_point int8,
    data bytea not null,
    PRIMARY KEY (point, topic_id)
);

CREATE TABLE IF NOT EXISTS meta(
    version int8 PRIMARY KEY
);