package tn_local

// CreateStreamRequest is the JSON-RPC request for local.create_stream.
type CreateStreamRequest struct {
	DataProvider string `json:"data_provider"`
	StreamID     string `json:"stream_id"`
	StreamType   string `json:"stream_type"` // "primitive" or "composed"
}

// CreateStreamResponse is the JSON-RPC response for local.create_stream.
type CreateStreamResponse struct{}

// InsertRecordsRequest is the JSON-RPC request for local.insert_records.
// Mirrors the consensus insert_records($data_provider TEXT[], $stream_id TEXT[],
// $event_time INT8[], $value NUMERIC(36,18)[]) signature — parallel arrays.
type InsertRecordsRequest struct {
	DataProvider []string `json:"data_provider"`
	StreamID     []string `json:"stream_id"`
	EventTime    []int64  `json:"event_time"`
	Value        []string `json:"value"`
}

// InsertRecordsResponse is the JSON-RPC response for local.insert_records.
// Mirrors consensus which returns nothing.
type InsertRecordsResponse struct{}

// InsertTaxonomyRequest is the JSON-RPC request for local.insert_taxonomy.
// Mirrors the consensus insert_taxonomy($data_provider, $stream_id,
// $child_data_providers TEXT[], $child_stream_ids TEXT[], $weights NUMERIC(36,18)[],
// $start_date INT) signature — parallel arrays for children.
type InsertTaxonomyRequest struct {
	DataProvider       string   `json:"data_provider"`
	StreamID           string   `json:"stream_id"`
	ChildDataProviders []string `json:"child_data_providers"`
	ChildStreamIDs     []string `json:"child_stream_ids"`
	Weights            []string `json:"weights"`
	StartDate          int64    `json:"start_date"`
}

// InsertTaxonomyResponse is the JSON-RPC response for local.insert_taxonomy.
type InsertTaxonomyResponse struct{}

// GetRecordRequest is the JSON-RPC request for local.get_record.
type GetRecordRequest struct {
	DataProvider string `json:"data_provider"`
	StreamID     string `json:"stream_id"`
	FromTime     *int64 `json:"from_time,omitempty"`
	ToTime       *int64 `json:"to_time,omitempty"`
}

// GetRecordResponse is the JSON-RPC response for local.get_record.
type GetRecordResponse struct {
	Records []RecordOutput `json:"records"`
}

// RecordOutput is a single record in a response.
type RecordOutput struct {
	EventTime int64  `json:"event_time"`
	Value     string `json:"value"`
	CreatedAt int64  `json:"created_at"`
}

// GetIndexRequest is the JSON-RPC request for local.get_index.
type GetIndexRequest struct {
	DataProvider string `json:"data_provider"`
	StreamID     string `json:"stream_id"`
	FromTime     *int64 `json:"from_time,omitempty"`
	ToTime       *int64 `json:"to_time,omitempty"`
	BaseTime     *int64 `json:"base_time,omitempty"`
}

// GetIndexResponse is the JSON-RPC response for local.get_index.
type GetIndexResponse struct {
	Records []IndexOutput `json:"records"`
}

// IndexOutput is a single index value in a response.
type IndexOutput struct {
	EventTime int64  `json:"event_time"`
	Value     string `json:"value"`
}

// ListStreamsRequest is the JSON-RPC request for local.list_streams.
type ListStreamsRequest struct{}

// ListStreamsResponse is the JSON-RPC response for local.list_streams.
type ListStreamsResponse struct {
	Streams []StreamInfo `json:"streams"`
}

// StreamInfo describes a local stream.
type StreamInfo struct {
	DataProvider string `json:"data_provider"`
	StreamID     string `json:"stream_id"`
	StreamType   string `json:"stream_type"`
	CreatedAt    int64  `json:"created_at"`
}
