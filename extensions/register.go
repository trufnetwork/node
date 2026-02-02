package extensions

import (
	database_size "github.com/trufnetwork/node/extensions/database-size"
	"github.com/trufnetwork/node/extensions/leaderwatch"
	"github.com/trufnetwork/node/extensions/tn_attestation"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/extensions/tn_digest"
	"github.com/trufnetwork/node/extensions/tn_lp_rewards"
	"github.com/trufnetwork/node/extensions/tn_settlement"
	"github.com/trufnetwork/node/extensions/tn_vacuum"
	"github.com/trufnetwork/node/extensions/tn_utils"
)

func init() {
	leaderwatch.InitializeExtension()
	tn_utils.InitializeExtension()
	tn_cache.InitializeExtension()
	tn_digest.InitializeExtension()
	tn_settlement.InitializeExtension()
	tn_lp_rewards.InitializeExtension()
	tn_vacuum.InitializeExtension()
	tn_attestation.InitializeExtension()
	database_size.InitializeExtension()
}
