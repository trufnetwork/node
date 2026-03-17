package tn_local

import (
	"context"
	"encoding/json"

	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
	rpcserver "github.com/trufnetwork/kwil-db/node/services/jsonrpc"
)

// Name implements rpcserver.Svc.
func (ext *Extension) Name() string { return ServiceName }

// Methods implements rpcserver.Svc.
func (ext *Extension) Methods() map[jsonrpc.Method]rpcserver.MethodDef {
	return map[jsonrpc.Method]rpcserver.MethodDef{
		"local.create_stream":   rpcserver.MakeMethodDef(ext.CreateStream, "create a local stream", ""),
		"local.insert_records":  rpcserver.MakeMethodDef(ext.InsertRecords, "insert records into local stream", "count"),
		"local.insert_taxonomy": rpcserver.MakeMethodDef(ext.InsertTaxonomy, "add taxonomy to local composed stream", ""),
		"local.get_record":      rpcserver.MakeMethodDef(ext.GetRecord, "query local stream records", "records"),
		"local.get_index":       rpcserver.MakeMethodDef(ext.GetIndex, "query local stream index", "records"),
		"local.list_streams":    rpcserver.MakeMethodDef(ext.ListStreams, "list all local streams", "streams"),
	}
}

// Health implements rpcserver.Svc.
func (ext *Extension) Health(ctx context.Context) (json.RawMessage, bool) {
	enabled := ext.isEnabled.Load()
	resp, _ := json.Marshal(struct{ Enabled bool }{enabled})
	return resp, enabled
}
