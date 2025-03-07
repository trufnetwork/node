-- Public action that routes to the correct internal action based on stream type
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

-- Public action that routes to the correct internal action for last record before timestamp
CREATE OR REPLACE ACTION get_last_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        RETURN get_last_record_primitive($data_provider, $stream_id, $before, $frozen_at);
    } else {
        RETURN get_last_record_composed($data_provider, $stream_id, $before, $frozen_at);
    }
};

-- Public action that routes to the correct internal action for first record
CREATE OR REPLACE ACTION get_first_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        RETURN get_first_record_primitive($data_provider, $stream_id, $after, $frozen_at);
    } else {
        RETURN get_first_record_composed($data_provider, $stream_id, $after, $frozen_at);
    }
};

-- Public action that routes to the correct internal action for base value
CREATE OR REPLACE ACTION get_base_value(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PUBLIC view returns (value NUMERIC(36,18)) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        return get_base_value_primitive($data_provider, $stream_id, $base_time, $frozen_at);
    } else {
        return get_base_value_composed($data_provider, $stream_id, $base_time, $frozen_at);
    }
};

-- Public action that routes to the correct internal action for index calculation
CREATE OR REPLACE ACTION get_index(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        RETURN get_index_primitive($data_provider, $stream_id, $from, $to, $frozen_at, $base_time);
    } else {
        RETURN get_index_composed($data_provider, $stream_id, $from, $to, $frozen_at, $base_time);
    }
};
