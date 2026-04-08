package tn_local

// Request types intentionally do NOT carry a data_provider field. The server
// derives the data_provider from the node operator's secp256k1 key (see
// Extension.nodeAddress) — local streams are always owned by the node they
// live on. Including data_provider on the wire would invite impersonation
// since the admin RPC server has no per-request identity.
//
// Response types DO keep data_provider fields when the corresponding consensus
// action returns one (e.g. list_streams), so callers can swap local/on-chain
// without reshaping records. The value is always equal to the node's own
// address — redundant but mirrored for compatibility.

// CreateStreamRequest is the JSON-RPC request for local.create_stream.
type CreateStreamRequest struct {
	StreamID   string `json:"stream_id"`
	StreamType string `json:"stream_type"` // "primitive" or "composed"
}

// CreateStreamResponse is the JSON-RPC response for local.create_stream.
type CreateStreamResponse struct{}

// InsertRecordsRequest is the JSON-RPC request for local.insert_records.
// Mirrors the consensus insert_records signature minus data_provider —
// parallel arrays for stream_id, event_time, and value. Each record may
// target a different stream, but all streams are owned by the node.
type InsertRecordsRequest struct {
	StreamID  []string `json:"stream_id"`
	EventTime []int64  `json:"event_time"`
	Value     []string `json:"value"`
}

// InsertRecordsResponse is the JSON-RPC response for local.insert_records.
// Mirrors consensus which returns nothing.
type InsertRecordsResponse struct{}

// InsertTaxonomyRequest is the JSON-RPC request for local.insert_taxonomy.
// Mirrors the consensus insert_taxonomy signature minus data_provider /
// child_data_providers — children are always local to the same node.
type InsertTaxonomyRequest struct {
	StreamID       string   `json:"stream_id"`
	ChildStreamIDs []string `json:"child_stream_ids"`
	Weights        []string `json:"weights"`
	StartDate      int64    `json:"start_date"`
}

// InsertTaxonomyResponse is the JSON-RPC response for local.insert_taxonomy.
type InsertTaxonomyResponse struct{}

// GetRecordRequest is the JSON-RPC request for local.get_record.
type GetRecordRequest struct {
	StreamID string `json:"stream_id"`
	FromTime *int64 `json:"from_time,omitempty"`
	ToTime   *int64 `json:"to_time,omitempty"`
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
	StreamID string `json:"stream_id"`
	FromTime *int64 `json:"from_time,omitempty"`
	ToTime   *int64 `json:"to_time,omitempty"`
	BaseTime *int64 `json:"base_time,omitempty"`
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

// StreamInfo describes a local stream. DataProvider is always equal to the
// node's own address — kept for parity with the consensus list_streams action
// so that on-chain and local clients can use the same response shape.
type StreamInfo struct {
	DataProvider string `json:"data_provider"`
	StreamID     string `json:"stream_id"`
	StreamType   string `json:"stream_type"`
	CreatedAt    int64  `json:"created_at"`
}
