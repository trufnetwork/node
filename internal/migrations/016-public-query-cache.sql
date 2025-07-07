-- Load the TN Cache extension
USE ext_tn_cache AS ext_tn_cache;

/**
 * get_record: Public facade for retrieving time series data with cache support.
 * Routes to primitive or composed implementation based on stream type.
 * Uses TN Cache extension when available and enabled.
 */
CREATE OR REPLACE ACTION get_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    
    -- Check if cache is enabled and frozen_at is null (frozen queries bypass cache)
    $cache_enabled BOOL := false;
    if $frozen_at IS NULL {
        for $row in ext_tn_cache.is_enabled() {
            $cache_enabled := $row.enabled;
            break;
        }
    }
    
    if $cache_enabled {
        -- Check if we have cached data for this request
        $has_cache BOOL := false;
        for $row in ext_tn_cache.has_cached_data($data_provider, $stream_id, $from, $to) {
            $has_cache := $row.has_data;
            break;
        }
        
        if $has_cache {
            -- Cache hit - return cached data
            NOTICE('Cache hit for stream ' || $data_provider || ':' || $stream_id || ' from ' || $from::TEXT || ' to ' || $to::TEXT);
            for $row in ext_tn_cache.get_cached_data($data_provider, $stream_id, $from, $to) {
                RETURN NEXT $row.event_time, $row.value;
            }
            return;
        } else {
            -- Cache miss - log and fallback to original logic
            NOTICE('Cache miss for stream ' || $data_provider || ':' || $stream_id || ' from ' || $from::TEXT || ' to ' || $to::TEXT);
        }
    }
    
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        for $row in get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in get_record_composed($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};