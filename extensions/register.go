package extensions

import (
	"github.com/trufnetwork/node/extensions/database-size"
	"github.com/trufnetwork/node/extensions/leaderwatch"
	"github.com/trufnetwork/node/extensions/tn_attestation"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/extensions/tn_digest"
	"github.com/trufnetwork/node/extensions/tn_vacuum"
)

func init() {
	leaderwatch.InitializeExtension()
	tn_cache.InitializeExtension()
	tn_digest.InitializeExtension()
	tn_vacuum.InitializeExtension()
	tn_attestation.InitializeExtension()
	database_size.InitializeExtension()
}
