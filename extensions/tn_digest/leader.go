package tn_digest

import (
	"bytes"

	"github.com/trufnetwork/kwil-db/common"
)

// isCurrentLeader compares the chain leader in block context with this node's identity.
func isCurrentLeader(app *common.App, block *common.BlockContext) bool {
	if block == nil || block.ChainContext == nil || block.ChainContext.NetworkParameters == nil || app.Service == nil || app.Service.Identity == nil {
		return false
	}
	leaderBytes := block.ChainContext.NetworkParameters.Leader.Bytes()
	if len(leaderBytes) == 0 {
		// warn that leader is not set
		app.Service.Logger.Warn("leader is not set")
		return false
	}
	return bytes.Equal(leaderBytes, app.Service.Identity)
}
