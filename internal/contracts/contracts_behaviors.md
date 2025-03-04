## Description

Currently, our users have a set of expectations on how the streams should behave. These expectations are split into the following categories:

- Authorization
- Data Querying
- Data Insertion
- Composition

This document lists the behaviors that must have automated tests to ensure they are met.

## Authorization

- [AUTH01] Stream ownership is clearly defined and can be transferred to another valid wallet.
- [AUTH02] The stream owner can control which wallets are allowed to read from the stream.
- [AUTH03] The stream owner can control which wallets are allowed to insert data into the stream.
- [AUTH04] The stream owner can control which streams are allowed to compose from the stream.
- [AUTH05] Stream owners are able to delete their streams and all associated data.

## Data Querying

- [PRIMITIVE01][PRIMITIVE04] Authorized users (owner and whitelisted wallets) can query records over a specified date range.
- [PRIMITIVE05] Authorized users (owner and whitelisted wallets) can query index value which is a normalized index computed from the raw data overspecified date range.
- [PRIMITIVE06] Authorized users (owner and whitelisted wallets) can query percentage changes of an index overspecified date range.
- [COMMON04] Users can query metadata, enabled or not, to retrieve configuration details of the stream.
- [PRIMITIVE07] Authorized users can query earliest available record for a stream.
- [COMMON04] All metadata values are publicly available.
- [PRIMITIVE07] If a point in time is queried, but there's no available data for that point, the closest available data in the past is returned.
- [PRIMITIVE08] Only one data point per date is returned from query (the latest inserted one)

## Data Insertion

- [PRIMITIVE01][PRIMITIVE02][COMPOSED01][COMPOSED02] Authorized wallets can insert new data records (e.g., primitive events) with associated timestamps and values.
    Note: truf-data-provider primitive has external_created_at field.
- [COMMON01] The stream owner can insert metadata that configures stream behavior. I.e. allow_read_wallet.
- [COMMON02][PRIMITIVE03][COMPOSED03] Some stream metadata are read-only and only set once created (e.g. stream_type, or other properties that are set only on special actions such as ownership transfer)
- [COMMON03] All metadata records are immutable, and can only be disabled but never deleted.
- [x] Data records are immutable. They can't be disabled or deleted. (records can't be disabled by design, no need to test)
- [COMPOSED04] Taxonomy definitions are immutable. But they can be disabled (only the whole version and not a single child definition)
- [PRIMITIVE04] A base date for a stream can be set by parameters. If not set, the stream will use the first record date as base date.


## Composition & Aggregation

- A composed stream aggregates data from multiple child streams (which may be either primitive or composed).
- Each child stream’s contribution is weighted, and these weights can vary over time.
- Taxonomies define the mapping of child streams, including a period of validity for each weight. (start_date and end_date, otherwise not set)
- The composed stream provides aggregated records and index value based on the weighted contributions of its children.
- If a child stream doesn't have data for the given date (including last available data), the composed stream will not count it's weight for that date.
- For a single taxonomy version, there can't be duplicated child stream definitions.
- Only 1 taxonomy version can be active in a point in time.

## Other

- All referenced addresses must be lowercased and valid EVM addresses starting with `0x`.
- stream ids must respect the following regex: `^st[a-z0-9]{30}$` and be unique by each stream owner.
