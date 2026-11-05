package tn_utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	pggenerate "github.com/trufnetwork/kwil-db/node/engine/pg_generate"
)

// The package's init sets both engine heights from engineUpgradeHeights.
func TestEngineUpgradeHeights(t *testing.T) {
	require.Len(t, engineUpgradeHeights, 2)
	for chainID, height := range engineUpgradeHeights {
		require.True(t, pggenerate.LegacyAt(chainID, height-1), chainID)
		require.False(t, pggenerate.LegacyAt(chainID, height), chainID)

		got, ok := precompiles.NestedCallChecksHeight(chainID)
		require.True(t, ok, chainID)
		require.Equal(t, height, got, chainID)
	}

	// A chain that is not listed keeps the legacy generator and no nested-call
	// checks at every height.
	require.True(t, pggenerate.LegacyAt("some-other-chain", math.MaxInt64-1))
	_, ok := precompiles.NestedCallChecksHeight("some-other-chain")
	require.False(t, ok)
}
