# TN Cache Extension

The TN_Cache extension provides a node-local caching layer for TRUF.NETWORK specific queries, optimizing expensive read operations without affecting network consensus.

## Configuration

Enable and configure the extension in your node's `config.toml` file.

```toml
[extensions.tn_cache]
# Enable or disable the extension.
enabled = true

# Optional: Schedule for re-resolving wildcards and IncludeChildren (default: daily at midnight)
# Set to empty string to disable automatic re-resolution
resolution_schedule = "0 0 0 * * *"  # Daily at midnight UTC (seconds minutes hours day month weekday)
# resolution_schedule = "0 0 * * * *"  # Hourly (for rapidly changing environments)
# resolution_schedule = ""            # Disable automatic resolution

# Optional: Maximum block age to consider node synced (default: "1h")
# Cache operations pause when node is syncing or block age exceeds this threshold
max_block_age = "1h"   # Examples: "30m", "45s", "2h"
# max_block_age = "-1"  # Disable sync checking

# Optional: Path to a CSV file containing streams to cache.
# The path is relative to the node's root directory.
streams_csv_file = "cache_streams.csv"

# Optional: Stream definitions in JSON format.
# Note: Either use 'streams_inline' OR 'streams_csv_file', not both.
streams_inline = '''
[
  {
    "data_provider": "0x1234567890abcdef1234567890abcdef12345678",
    "stream_id": "st123456789012345678901234567890",
    "cron_schedule": "0 0 * * * *",  # Hourly refresh (seconds minutes hours day month weekday)
    "from": 1719849600,            # Optional: Only cache data after this timestamp
    "include_children": true       # Optional: Include children of composed streams (default: false)
  },
  {
    "data_provider": "0x9876543210fedcba9876543210fedcba98765432",
    "stream_id": "*",
    "cron_schedule": "0 0 0 * * *",  # Daily at midnight
    "include_children": false
  }
]
'''
```

### Configuration Options

-   **`enabled`**: A boolean (`true` or `false`) to enable or disable the extension.
-   **`resolution_schedule`**: (Optional) A cron expression with seconds that defines when to re-resolve wildcards and IncludeChildren directives. Uses 6-field format: `second minute hour day month weekday`. Default is `0 0 0 * * *` (daily at midnight UTC). Set to empty string to disable automatic re-resolution.
-   **`max_block_age`**: (Optional) Maximum age of the latest block to consider the node synced. Default is `"1h"`. Accepts duration strings like `"30m"`, `"45s"`, `"2h"`. Set to `"-1"` to disable sync checking. When the node is syncing or block age exceeds this threshold, cache refresh operations are paused.
-   **`streams_csv_file`**: (Optional) A path to a CSV file containing a list of streams to cache. The file must have columns for `data_provider`, `stream_id`, `cron_schedule`, and optional `from` and `include_children` columns.
-   **`streams_inline`**: (Optional) A JSON-formatted string containing an array of stream objects to cache.

**Note**: `streams_csv_file` and `streams_inline` are **mutually exclusive**. You must use either inline JSON or CSV file configuration, not both. The extension will error if both are provided.

### Stream Definition Fields

Each stream, whether in the JSON string or CSV file, can have the following fields:

-   **`data_provider`**: (Required) The data provider's Ethereum address.
-   **`stream_id`**: (Required) The ID of the stream. You can use `*` as a wildcard to cache all streams for a given data provider.
-   **`cron_schedule`**: (Required) A cron expression with seconds (6 fields: second minute hour day month weekday, e.g., `0 0 * * * *` for hourly) that defines how often the cache should be refreshed. This field is required in both JSON and CSV configurations.
-   **`from`**: (Optional) A Unix timestamp. If provided, the cache will only store data points with a timestamp greater than or equal to this value.
-   **`include_children`**: (Optional) A boolean (default: `false`). When `true`, children of composed streams are included in caching. This is useful for hierarchical stream structures where you want to cache not only the parent stream but also its child components.

### CSV File Format

Your CSV file should look like this:

```csv
data_provider,stream_id,cron_schedule,from,include_children
0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 0 * * * *,1719849600,true
0x9876543210fedcba9876543210fedcba98765432,*,0 0 0 * * *,1719936000,false
0xabcdefabcdefabcdefabcdefabcdefabcdefabcd,stcomposedstream123,0 0 */6 * * *,,true
```

### Configuration Validation

The extension enforces that `streams_csv_file` and `streams_inline` are mutually exclusive. If both are provided, the extension will fail to start with a clear error message.

## Features

- **Configurable Caching**: Define which streams to cache and on what schedule
- **Dynamic Resolution**: Automatically detects new streams matching wildcards and new children of composed streams
- **Isolated from Consensus**: Uses PostgreSQL schemas that are excluded from block hashing
- **Background Refresh**: Automatically refreshes cache data on configurable schedules
- **Graceful Handling**: Safely enables/disables without affecting node operation

## Implementation Details

The extension creates its own private schema for storing cached data, which is isolated from the consensus state through Kwil-DB's schema filtering mechanism.

### Dynamic Resolution

When using wildcards (`*`) or `include_children`, the extension performs dynamic resolution to handle changes in the stream landscape:

1. **Initial Resolution**: At startup, wildcards and IncludeChildren directives are resolved to concrete streams
2. **Periodic Re-resolution**: Based on `resolution_schedule` (default: daily), the extension re-resolves:
   - Wildcard patterns to find new streams
   - IncludeChildren to detect new child streams
3. **Atomic Updates**: The `cached_streams` table is updated atomically to ensure no data gaps

This ensures that:
- New streams created after startup are automatically cached if they match a wildcard
- New children added to composed streams are automatically included
- Deleted streams are cleaned up from the cache

### Cache Schema

```sql
-- Private schema for the extension
CREATE SCHEMA IF NOT EXISTS ext_tn_cache;

-- Track configured streams and their refresh policies
CREATE TABLE IF NOT EXISTS ext_tn_cache.cached_streams (
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    from_timestamp INT8,
    last_refreshed INT8,
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
- `tn_cache.has_cached_data(data_provider, stream_id, from, to)`: Checks if the cache can answer a query
- `tn_cache.get_cached_data(data_provider, stream_id, from, to)`: Retrieves cached data
- `tn_cache.get_cached_last_before(data_provider, stream_id, before)`: Gets the most recent record before a timestamp
- `tn_cache.get_cached_first_after(data_provider, stream_id, after)`: Gets the earliest record after a timestamp

All cache methods follow TRUF.NETWORK query conventions for how `from` and `to` parameters behave (including NULL handling and anchor records).

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

-- Example using last_before
CREATE OR REPLACE ACTION get_last_value_before(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $use_cache BOOLEAN DEFAULT false
) PRIVATE VIEW RETURNS TABLE(event_time INT8, value NUMERIC(36,18)) {
    if $use_cache and tn_cache.is_enabled() {
        for $row in SELECT * FROM tn_cache.get_cached_last_before($data_provider, $stream_id, $before) {
            return next $row.event_time, $row.value;
        }
        return;
    }
    
    -- Fall back to querying primitive_events directly
    SELECT event_time, value FROM primitive_events
    WHERE data_provider = $data_provider 
      AND stream_id = $stream_id
      AND event_time < COALESCE($before, 9223372036854775000)
    ORDER BY event_time DESC
    LIMIT 1;
};
``` 