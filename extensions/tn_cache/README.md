# TN Cache Extension

The TN_Cache extension provides a node-local caching layer for TRUF.NETWORK specific queries, optimizing expensive read operations without affecting network consensus.

## Configuration

Enable and configure the extension in your `config.toml` file:

```toml
[extensions.tn_cache]
# Enable or disable the extension
enabled = true

# Stream definitions in JSON format
streams = '''
[
  {
    "data_provider": "0x1234567890abcdef",
    "stream_id": "st1234",
    "cron_schedule": "0 * * * *",  # Hourly refresh
    "from": 1719849600             # Optional: Only cache data after this timestamp
  },
  {
    "data_provider": "0xabcdef1234567890",
    "stream_id": "st5678",
    "cron_schedule": "0 0 * * *"   # Daily refresh at midnight
  }
]
'''
```

## Features

- **Configurable Caching**: Define which streams to cache and on what schedule
- **Isolated from Consensus**: Uses PostgreSQL schemas that are excluded from block hashing
- **Background Refresh**: Automatically refreshes cache data on configurable schedules
- **Graceful Handling**: Safely enables/disables without affecting node operation

## Implementation Details

The extension creates its own private schema for storing cached data, which is isolated from the consensus state through Kwil-DB's schema filtering mechanism.

### Cache Schema

```sql
-- Private schema for the extension
CREATE SCHEMA IF NOT EXISTS ext_tn_cache;

-- Track configured streams and their refresh policies
CREATE TABLE IF NOT EXISTS ext_tn_cache.cached_streams (
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    from INT8,
    last_refreshed TIMESTAMPTZ,
    cron_schedule TEXT,
    PRIMARY KEY (data_provider, stream_id)
);

-- Store the actual cached event data
CREATE TABLE IF NOT EXISTS ext_tn_cache.cached_events (
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    event_time INT8 NOT NULL,
    value NUMERIC(36, 18) NOT NULL,
    PRIMARY KEY (data_provider, stream_id, event_time)
);
```

## SQL Functions

The extension registers custom SQL functions to allow actions to use the cache:

- `tn_cache.is_enabled()`: Checks if caching is enabled on this node
- `tn_cache.has_cached_data(data_provider, stream_id, from)`: Checks if data is available in the cache
- `tn_cache.get_cached_data(data_provider, stream_id, from, to)`: Retrieves cached data

## Usage in SQL Actions

SQL actions can use the cache like this:

```sql
CREATE OR REPLACE ACTION get_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $use_cache BOOLEAN DEFAULT false
) PRIVATE VIEW RETURNS TABLE(...) {
    -- Check for cached data if requested
    if $use_cache and tn_cache.is_enabled() {
        if tn_cache.has_cached_data($data_provider, $stream_id, $from, $to) {
            NOTICE('{"cache_hit": true}');
            return SELECT * FROM tn_cache.get_cached_data($data_provider, $stream_id, $from, $to);
        } else {
            NOTICE('{"cache_hit": false}');
        }
    }

    -- Fall back to original computation if cache not used or unavailable
    RETURN WITH RECURSIVE
    SELECT ...;
};
``` 