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
 * Uses unnest with ordinality to preserve order and avoid expensive for-loops.
 */
CREATE OR REPLACE ACTION helper_sanitize_wallets($wallets TEXT[]) PRIVATE VIEW RETURNS (sanitized_wallets TEXT[]) {
    -- Handle empty array
    IF array_length($wallets) = 0 {
        RETURN $wallets;
    }

    -- Validate each wallet address first
    FOR $i in 1..array_length($wallets) {
        IF NOT check_ethereum_address($wallets[$i]) {
            ERROR('Invalid wallet address at index ' || $i::TEXT);
        }
    }

    -- Then sanitize all at once using unnest with ordinality for efficiency
    for $result in WITH sanitized AS (
        SELECT
            ord,
            LOWER(wallet) AS sanitized_wallet
        FROM unnest($wallets) WITH ORDINALITY AS u(wallet, ord)
    )
    SELECT array_agg(sanitized_wallet ORDER BY ord) AS sanitized_wallets
    FROM sanitized {
        RETURN $result.sanitized_wallets;
    }
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
    $lowercase_array TEXT[];

    -- handle NULL or empty array
    IF $input_array IS NULL OR array_length($input_array) = 0 {
        RETURN $input_array;
    }

    -- Use unnest with ordinality to preserve input array order
    RETURN SELECT array_agg(lowered ORDER BY ord) FROM (
        SELECT
            LOWER(item) AS lowered,
            ord
        FROM unnest($input_array) WITH ORDINALITY AS u(item, ord)
    ) t;
};


CREATE OR REPLACE ACTION helper_check_cache(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $base_time INT8
) PRIVATE VIEW RETURNS (cache_hit BOOL) {
    $is_caching_enabled BOOL := tn_cache.is_enabled();
    $cache_hit := false;

    if $is_caching_enabled {
        $has_cached_data BOOL := false;
        $cache_refreshed_at_timestamp INT8;
        $cache_height INT8;
        $has_cached_data, $cache_refreshed_at_timestamp, $cache_height := tn_cache.has_cached_data($data_provider, $stream_id, $from, $to, $base_time);
        
        if $has_cached_data {
            -- Cache hit - get most recent cached data, show height to users
            NOTICE('{"cache_hit": true, "cache_height": ' || $cache_height::TEXT || ', "cache_refreshed_at_timestamp": ' || $cache_refreshed_at_timestamp::TEXT || ', "base_time": ' || COALESCE($base_time, -1)::TEXT || '}');
            $cache_hit := true;
        } else {
            -- Cache miss - log and fallback to original logic
            NOTICE('{"cache_hit": false, "base_time": ' || COALESCE($base_time, -1)::TEXT || '}');
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
    IF COALESCE(array_length($event_times), 0) = 0 {  
         RETURN;  
     }  
    IF COALESCE(array_length($stream_refs), 0) != COALESCE(array_length($event_times), 0)  
       OR COALESCE(array_length($event_times), 0) != COALESCE(array_length($values), 0) {  
        ERROR('helper_enqueue_prune_days: array lengths mismatch');  
    } 

    INSERT INTO pending_prune_days (stream_ref, day_index)
    SELECT DISTINCT
        t.stream_ref,
        (t.event_time / 86400)::INT AS day_index
    FROM UNNEST($stream_refs, $event_times, $values) AS t(stream_ref, event_time, value)
    WHERE t.stream_ref IS NOT NULL
      AND t.event_time >= 0::INT8
      AND t.value != 0::NUMERIC(36,18)
    ON CONFLICT (stream_ref, day_index) DO NOTHING;
};
