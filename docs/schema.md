# Database Schema Documentation

## Overview
This database schema manages data streams with taxonomies, events, and metadata tracking.

![Schema](/docs/images/schema.png)

## Tables

### `data_providers`
Whitelisted data providers.

| Field | Type | Description |
|-------|------|-------------|
| `id` | PK | Unique identifier |
| `address` | | Provider |
| `created_at` | | Record creation timestamp |

### `streams`
Data streams from various providers.

| Field | Type | Description |
|-------|------|-------------|
| `id` | PK | Unique identifier |
| `data_provider_id` | FK | Links to data_providers |
| `stream_id` | | Stream identifier |
| `stream_type` | | Stream classification |
| `created_at` | | Record creation timestamp |

### `primitive_events`
Time-series event data for streams.

| Field | Type | Description |
|-------|------|-------------|
| `event_time` | PK | Event occurrence time |
| `created_at` | PK | Record insertion time |
| `stream_ref` | PK,FK | Links to streams |
| `value` | | Event value |
| `truflation_created_at` | | External system timestamp |

### `taxonomies`
Hierarchical classification system for composed streams.

| Field | Type | Description |
|-------|------|-------------|
| `taxonomy_id` | PK | Unique identifier |
| `stream_ref` | FK | Reference to parent stream |
| `child_stream_ref` | FK | Reference to child stream |
| `weight` | | Weight/Importance value |
| `created_at` | | Record creation timestamp |
| `disabled_at` | | Soft deletion timestamp |
| `group_sequence` | | Ordering within group |
| `start_time` | | When taxonomy becomes active |

### `metadata`
Flexible key-value storage for additional stream properties.

| Field | Type | Description |
|-------|------|-------------|
| `row_id` | PK | Unique identifier |
| `stream_ref` | FK | Links to streams |
| `metadata_key` | | Property name |
| `value_i` | | Integer value |
| `value_f` | | Float value |
| `value_b` | | Boolean value |
| `value_s` | | String value |
| `value_ref` | | Reference value |
| `created_at` | | Record creation timestamp |
| `disabled_at` | | Soft deletion timestamp |

#### Reserved metadata keys

The `metadata_key` column is open for stream-owner use, but a fixed set
of reserved keys drive built-in actions:

| Key | Value column | Set by | Read by |
|-----|--------------|--------|---------|
| `stream_owner` | `value_ref` | `create_stream`, `transfer_stream_ownership` | `is_stream_owner` |
| `read_visibility` | `value_i` | `create_stream`, `set_read_visibility` | `is_allowed_to_read_*` |
| `compose_visibility` | `value_i` | `set_compose_visibility` | composed-query gates |
| `allow_read_wallet` | `value_ref` | `allow_read_wallet` | `is_allowed_to_read_*` |
| `allow_compose_stream` | `value_ref` | `allow_compose_stream` | composed-query gates |
| `readonly_key` | `value_s` | `create_stream` | `insert_metadata`, `disable_metadata` |
| `type` | `value_s` | `create_stream` | informational |
| `default_base_time` | `value_i` | SDK `set_default_base_time` | index-base lookup |
| `allow_zeros` | `value_b` | `create_stream($allow_zeros=TRUE)`, `set_allow_zeros(true)` | `insert_records`, `helper_enqueue_prune_days` (default FALSE: zeros dropped on insert) |