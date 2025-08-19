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
	leaderPk := block.ChainContext.NetworkParameters.Leader
	if leaderPk.PublicKey == nil {
		return false
	}
	return bytes.Equal(leaderPk.Bytes(), app.Service.Identity)
}
