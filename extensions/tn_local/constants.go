package tn_local

const (
	// SchemaName is the PostgreSQL schema for local storage tables.
	// Not prefixed with "ds_" so it's excluded from consensus block hashing.
	SchemaName = "ext_tn_local"

	// ServiceName is the JSON-RPC service name registered on the admin server.
	ServiceName = "local"
)
