package tn_utils

import (
	"math"

	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	pggenerate "github.com/trufnetwork/kwil-db/node/engine/pg_generate"
)

// engineUpgradeHeights holds, for each chain ID, the first block that runs two
// engine rules from kwil-db: blocks generate their SQL with
// pggenerate.GenerateSQL instead of pggenerate.GenerateLegacySQL, and calls that
// precompile methods make through their engine are limited by the method's
// namespace and access modifiers. Blocks below the height run as before, so
// every node of a chain must run a release with this map before the chain
// reaches its height. A chain that is not listed keeps the earlier rules at
// every height.
var engineUpgradeHeights = map[string]int64{
	"tn-v2.1":    3115000,  // mainnet
	"testnet-v1": 15500000, // testnet
}

func init() {
	pggenerate.SetUpgradeHeights(func(chainID string) int64 {
		if height, ok := engineUpgradeHeights[chainID]; ok {
			return height
		}
		return math.MaxInt64
	})
	for chainID, height := range engineUpgradeHeights {
		precompiles.SetNestedCallChecksHeight(chainID, height)
	}
}
