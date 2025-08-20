/**
 * =====================================================================================
 *                                  UTILITY ACTIONS
 * =====================================================================================
 * This file contains a collection of generic, reusable private helper actions.
 * These actions perform common validation and sanitization tasks that can be
 * composed to build more complex public actions.
 * =====================================================================================
 */

/**
 * helper_split_string: A private helper to split a string by the first occurrence of a delimiter.
 *
 * Input:
 * - $input_string: The string to split.
 * - $delimiter: The delimiter to split by.
 *
 * Output:
 * - Two variables, $part1 and $part2, containing the two parts of the string.
 *   Returns NULL if the input string is NULL.
 *
 * Errors:
 * - If the delimiter is not found, or if the string starts or ends with the delimiter,
 *   which would result in one of the parts being empty.
 */
CREATE OR REPLACE ACTION helper_split_string(
    $input_string TEXT,
    $delimiter TEXT
) PRIVATE VIEW RETURNS (part1 TEXT, part2 TEXT) {
    $part1 TEXT;
    $part2 TEXT;

    IF $input_string IS NULL {
        RETURN NULL, NULL;
    }

    $delim_pos INT := position($delimiter, $input_string);

    -- Error if delimiter not found, or if it's at the very beginning or end.
    -- This ensures both parts of the split are non-empty.
    IF $delim_pos <= 1 OR $delim_pos >= length($input_string) - length($delimiter) + 1 {
        ERROR('Invalid string format. Delimiter "' || $delimiter || '" must be present and not at the ends of: ' || $input_string);
    }

    $part1 := substring($input_string, 1, $delim_pos - 1);
    $part2 := substring($input_string, $delim_pos + length($delimiter));

    RETURN $part1, $part2;
};

/**
 * helper_sanitize_wallets: Validates and sanitizes an array of wallet addresses.
 */
CREATE OR REPLACE ACTION helper_sanitize_wallets($wallets TEXT[]) PRIVATE VIEW RETURNS (sanitized_wallets TEXT[]) {
    FOR $i in 1..array_length($wallets) {
        IF NOT check_ethereum_address($wallets[$i]) {
            ERROR('Invalid wallet address in array at index ' || $i::TEXT);
        }
        $wallets[$i] := LOWER($wallets[$i]);
    }
    RETURN $wallets;
};

/**
 * helper_lowercase_array: Converts an entire TEXT array to lowercase in a single operation.
 * This avoids expensive for-loops that create thousands of roundtrips.
 * 
 * Input:
 * - $input_array: Array of text strings to convert to lowercase
 * 
 * Output:
 * - Array of lowercase text strings
 */
CREATE OR REPLACE ACTION helper_lowercase_array(
    $input_array TEXT[]
) PRIVATE VIEW RETURNS (lowercase_array TEXT[]) {
    -- Use WITH RECURSIVE to process entire array in single SQL operation
    -- This avoids the expensive for-loop roundtrips
    for $row in WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($input_array)
    ),
    array_holder AS (
        SELECT $input_array AS original_array
    ),
    unnested_results AS (
        SELECT 
            idx,
            LOWER(array_holder.original_array[idx]) AS lowercase_element
        FROM indexes
        JOIN array_holder ON 1=1
    ),
    build_array AS (
        SELECT 1 AS current_idx, ARRAY[]::TEXT[] AS result
        UNION ALL
        SELECT 
            ba.current_idx + 1,
            array_append(ba.result, ur.lowercase_element)
        FROM build_array ba
        JOIN unnested_results ur ON ur.idx = ba.current_idx
        WHERE ba.current_idx <= array_length($input_array)
    )
    SELECT result AS lowercase_array
    FROM build_array
    WHERE current_idx = array_length($input_array) + 1 {
        RETURN $row.lowercase_array;
    }
};


CREATE OR REPLACE ACTION helper_check_cache(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8
) PRIVATE VIEW RETURNS (cache_hit BOOL) {
    $is_caching_enabled BOOL := tn_cache.is_enabled();
    $cache_hit := false;

    if $is_caching_enabled {
        $has_cached_data BOOL := false;
        $cache_refreshed_at_timestamp INT8;
        $cache_height INT8;
        $has_cached_data, $cache_refreshed_at_timestamp, $cache_height := tn_cache.has_cached_data($data_provider, $stream_id, $from, $to);
        
        if $has_cached_data {
            -- Cache hit - get most recent cached data, show height to users
            NOTICE('{"cache_hit": true, "cache_height": ' || $cache_height::TEXT || ', "cache_refreshed_at_timestamp": ' || $cache_refreshed_at_timestamp::TEXT || '}');
            $cache_hit := true;
        } else {
            -- Cache miss - log and fallback to original logic
            NOTICE('{"cache_hit": false}');
        }
    } else {
        NOTICE('{"cache_disabled": true}');
    }
    RETURN $cache_hit;
};

-- Enqueue pending prune days for a batch of records, filtering out zero values
CREATE OR REPLACE ACTION helper_enqueue_prune_days(
    $stream_refs INT[],
    $event_times INT8[],
    $values NUMERIC(36,18)[]
) PRIVATE {
    IF array_length($event_times) IS NULL OR array_length($event_times) = 0 {
        RETURN;
    }

    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes WHERE idx < array_length($event_times)
    ),
    array_holder AS (
        SELECT $stream_refs AS stream_refs, $event_times AS event_times, $values AS values_array
    )
    INSERT INTO pending_prune_days (stream_ref, day_index)
    SELECT DISTINCT 
        array_holder.stream_refs[indexes.idx] AS stream_ref,
        (array_holder.event_times[indexes.idx] / 86400)::INT AS day_index
    FROM indexes
    JOIN array_holder ON 1=1
    WHERE array_holder.stream_refs[indexes.idx] IS NOT NULL
      AND array_holder.event_times[indexes.idx] >= 0::INT8
      AND array_holder.values_array[indexes.idx] != 0::NUMERIC(36,18)
    ON CONFLICT (stream_ref, day_index) DO NOTHING;
};