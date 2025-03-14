/**
 * get_record_composed: Placeholder for composed stream data retrieval.
 * Will aggregate child stream values based on taxonomy weights.
 */
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

/**
 * get_last_record_composed: Placeholder for finding last record in composed stream.
 * Will determine the last record based on child stream values and weights.
 */
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

/**
 * get_first_record_composed: Placeholder for finding first record in composed stream.
 * Will determine the first record based on child stream values and weights.
 */
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

/**
 * get_base_value_composed: Placeholder for finding base value in composed stream.
 * Will calculate base value from child streams at the specified time.
 */
CREATE OR REPLACE ACTION get_base_value_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PRIVATE view returns (value NUMERIC(36,18)) {
    ERROR('Composed stream query implementation is missing');
};

/**
 * get_index_composed: Placeholder for index calculation in composed streams.
 * Will calculate index values using the formula: (current_value/base_value)*100
 */
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
