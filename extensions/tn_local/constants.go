package tn_local

const (
	// SchemaName is the PostgreSQL schema for local storage tables.
	// Not prefixed with "ds_" so it's excluded from consensus block hashing.
	SchemaName = "ext_tn_local"

	// ServiceName is the JSON-RPC service name registered on the admin server.
	ServiceName = "local"
)

// JSON-RPC method names. These are part of the wire format that SDKs hash
// into the auth canonical payload — never rename without bumping AuthVersion
// (and coordinating SDK + server updates).
const (
	MethodCreateStream    = "local.create_stream"
	MethodInsertRecords   = "local.insert_records"
	MethodInsertTaxonomy  = "local.insert_taxonomy"
	MethodDeleteStream    = "local.delete_stream"
	MethodDisableTaxonomy = "local.disable_taxonomy"
	MethodGetRecord       = "local.get_record"
	MethodGetIndex        = "local.get_index"
	MethodListStreams     = "local.list_streams"
)
