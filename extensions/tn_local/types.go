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
type InsertRecordsRequest struct {
	DataProvider string        `json:"data_provider"`
	StreamID     string        `json:"stream_id"`
	Records      []RecordInput `json:"records"`
}

// RecordInput is a single record to insert.
type RecordInput struct {
	EventTime int64  `json:"event_time"`
	Value     string `json:"value"`
}

// InsertRecordsResponse is the JSON-RPC response for local.insert_records.
type InsertRecordsResponse struct {
	Count int `json:"count"`
}

// InsertTaxonomyRequest is the JSON-RPC request for local.insert_taxonomy.
type InsertTaxonomyRequest struct {
	DataProvider      string `json:"data_provider"`
	StreamID          string `json:"stream_id"`
	ChildDataProvider string `json:"child_data_provider"`
	ChildStreamID     string `json:"child_stream_id"`
	Weight            string `json:"weight"`
	StartTime         int64  `json:"start_time"`
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
