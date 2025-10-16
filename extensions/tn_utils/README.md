# tn_utils Extension

Reusable precompiles that support attestation and other deterministic workflows.

## Methods

### `call_dispatch(action_name TEXT, args_bytes BYTEA) -> BYTEA`
- Dispatches to another action while preserving the current engine context.
- Returns the canonical, deterministic encoding of the target action's result rows.

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
