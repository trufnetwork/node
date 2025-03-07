-- get_record returns the value of the composed stream for a given date range
CREATE OR REPLACE ACTION get_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};

-- get_last_record returns the last record before the given timestamp
CREATE OR REPLACE ACTION get_last_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};

-- get_first_record returns the first record of the composed stream
CREATE OR REPLACE ACTION get_first_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};

-- get_base_value returns the first nearest value of the composed stream before the given timestamp
CREATE OR REPLACE ACTION get_base_value_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PRIVATE view returns (value NUMERIC(36,18)) {
    ERROR('Composed stream query implementation is missing');
};

-- get_index calculation is ((current_composed/base_composed)*100)
CREATE OR REPLACE ACTION get_index_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};
