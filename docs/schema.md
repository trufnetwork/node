# Database Schema Documentation

## Overview
This database schema manages data streams with taxonomies, events, and metadata tracking.

![Schema](/docs/images/schema.png)

## Tables

### `data_providers`
External data sources and their connection details.

| Field | Type | Description |
|-------|------|-------------|
| `id` | PK | Unique identifier |
| `address` | | Provider |
| `created_at` | | Record creation timestamp |

### `streams`
Core data streams from various providers.

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
Hierarchical classification system for organizing streams.

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