package validator_set

import (
	"fmt"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/require"

	kwilnetwork "github.com/trufnetwork/node/infra/lib/kwil-network"
	kwilnetworkpeer "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
)

// Each node's rendered config.toml caps a user RPC request at 10 s, below the
// 15 s timeout common in clients, so a slow call is cut before they give up.
func TestRenderedConfigCapsRPCTimeout(t *testing.T) {
	keys := []kwilnetwork.NodeKeys{
		{KeyType: "secp256k1", PublicKeyHex: "02" + strings.Repeat("11", 32)},
		{KeyType: "secp256k1", PublicKeyHex: "03" + strings.Repeat("22", 32)},
	}
	peers := make([]kwilnetworkpeer.TNPeer, len(keys))
	for i, key := range keys {
		peers[i] = kwilnetworkpeer.TNPeer{
			NodeId:         key.NodeId(),
			Address:        jsii.String(fmt.Sprintf("node-%d.test.sub", i+1)),
			NodeHexAddress: key.PublicKeyHex,
		}
	}

	for i := range peers {
		rendered := populateAndRenderValues(nil, i, &ValidatorSetProps{}, peers[i], peers, keys)

		var cfg struct {
			RPC struct {
				Timeout string `toml:"timeout"`
			} `toml:"rpc"`
		}
		_, err := toml.Decode(rendered.String(), &cfg)
		require.NoError(t, err)
		require.Equal(t, "10s", cfg.RPC.Timeout, "node %d", i)
	}
}
