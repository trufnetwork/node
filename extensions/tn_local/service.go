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
		MethodCreateStream:    rpcserver.MakeMethodDef(ext.CreateStream, "create a local stream", ""),
		MethodInsertRecords:   rpcserver.MakeMethodDef(ext.InsertRecords, "insert records into local stream", "count"),
		MethodInsertTaxonomy:  rpcserver.MakeMethodDef(ext.InsertTaxonomy, "add taxonomy to local composed stream", ""),
		MethodDeleteStream:    rpcserver.MakeMethodDef(ext.DeleteStream, "delete a local stream and all its data", ""),
		MethodDisableTaxonomy: rpcserver.MakeMethodDef(ext.DisableTaxonomy, "disable a taxonomy group on a local composed stream", ""),
		MethodGetRecord:       rpcserver.MakeMethodDef(ext.GetRecord, "query local stream records", "records"),
		MethodGetIndex:        rpcserver.MakeMethodDef(ext.GetIndex, "query local stream index", "records"),
		MethodListStreams:     rpcserver.MakeMethodDef(ext.ListStreams, "list all local streams", "streams"),
	}
}

// Health implements rpcserver.Svc.
func (ext *Extension) Health(ctx context.Context) (json.RawMessage, bool) {
	enabled := ext.isEnabled.Load()
	resp, _ := json.Marshal(struct{ Enabled bool }{enabled})
	return resp, enabled
}
