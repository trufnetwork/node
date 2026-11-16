# tn_utils Extension

Reusable precompiles that support attestation and other deterministic workflows.

## Methods

### `call_dispatch(action_name TEXT, args_bytes BYTEA) -> BYTEA`
- Dispatches to another action while preserving the current engine context.
- Returns the canonical, deterministic encoding of the target action's result rows.
- Runs only `PUBLIC VIEW` actions in the `main` namespace (`dispatch_guard.go` lists the height this applies from on each network).

### `validate_query_stream(data_provider BYTEA, stream_id BYTEA, args_bytes BYTEA)`
- Errors unless `stream_id` is 32 bytes, `args_bytes` decodes, and its first two args are `data_provider` (0x-prefixed hex, either letter case) and `stream_id` (exact).
- `create_market` and `request_attestation` call it, so the stream a query is for is the stream its action reads.

### `validate_attestation_date_range(action_id INT, args_bytes BYTEA)`
- Errors when an attestation action's args are refused before dispatch: ids 1 to 3 always, the range of ids 10 and 11, the interval of id 12.
- For the outcome actions (ids 13 to 16) it checks the arg count, the types (TEXT, INT8, `NUMERIC(36,18)`), the NULLs and the bounds of `timestamp`, `window`, `tolerance` and `time_interval`. `request_attestation` and `create_market` both call it, so a market whose query could never run is refused when it is created.

### `parse_attestation_outcome(result_canonical BYTEA) -> INT`
- Reads the result of an outcome action from an attestation's `result_canonical`: `1` YES, `0` NO, `-1` INVALID.
- Accepts only ids 13 to 16, and only a payload with exactly one value of `1e18`, `0` or `-1e18`. A malformed header fails with `invalid result_canonical`, another id with `unsupported action_id`, and a bad payload with `outcome action result`.
- `parse_attestation_boolean` keeps refusing these ids.

### `bytea_join(chunks BYTEA[], delimiter BYTEA) -> BYTEA`
- Concatenates an array of `BYTEA` chunks, inserting the provided delimiter between entries.
- Treats `NULL` or empty chunks as empty segments.

### `bytea_length_prefix(chunk BYTEA) -> BYTEA`
- Returns a 4-byte big-endian length prefix followed by the original chunk (treats `NULL` as zero-length).

### `bytea_length_prefix_many(chunks BYTEA[]) -> BYTEA[]`
- Applies the same length-prefix transformation to each entry, returning a new `BYTEA[]`.

### `encode_uint8(value INT) -> BYTEA`
### `encode_uint16(value INT) -> BYTEA`
### `encode_uint32(value INT) -> BYTEA`
### `encode_uint64(value INT) -> BYTEA`
- Encode unsigned integers to big-endian byte arrays with the specified width.

## Usage

```sql
USE tn_utils AS util;

-- Call another action
$result_bytes := util.call_dispatch('get_record', $args_bytes);

-- Join canonical payload chunks
$payload := util.bytea_join(util.bytea_length_prefix_many(ARRAY[
    $version_bytes,
    $algo_bytes,
    $result_bytes
]), E'');
```

The Go package lives at `github.com/trufnetwork/node/extensions/tn_utils`.
Import it to register the utilities precompile bundle on node startup:

```go
import "github.com/trufnetwork/node/extensions/tn_utils"

func init() {
    tn_utils.InitializeExtension()
}
```
