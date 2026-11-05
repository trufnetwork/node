package kwil_network_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kwilnet "github.com/trufnetwork/node/infra/lib/kwil-network"
)

// Placeholder keys. They only need the right shape.
var (
	pubKey0 = "02" + strings.Repeat("ab", 32)
	pubKey1 = "03" + strings.Repeat("CD", 32)
	edKey   = strings.Repeat("ef", 32)
)

func TestParseNodePublicKeys(t *testing.T) {
	keys, err := kwilnet.ParseNodePublicKeys(pubKey0 + "#secp256k1, " + pubKey1 + "," + edKey + "#ed25519")
	require.NoError(t, err)
	require.Equal(t, []kwilnet.NodeKeys{
		{KeyType: "secp256k1", PublicKeyHex: pubKey0},
		{KeyType: "secp256k1", PublicKeyHex: strings.ToLower(pubKey1)},
		{KeyType: "ed25519", PublicKeyHex: edKey},
	}, keys)
	require.Equal(t, pubKey0+"#secp256k1", keys[0].NodeId())
}

func TestParseNodePublicKeysRejectsBadInput(t *testing.T) {
	// 32 bytes of hex: the length of a secp256k1 private key, not a public key.
	notAPublicKey := strings.Repeat("7f", 32)

	for name, raw := range map[string]string{
		"empty list":              "",
		"empty entry":             pubKey0 + ",,",
		"private key length":      notAPublicKey,
		"private key with type":   notAPublicKey + "#secp256k1",
		"not hex":                 "02" + strings.Repeat("zz", 32),
		"unknown key type":        pubKey0 + "#rsa",
		"uncompressed key prefix": "04" + strings.Repeat("ab", 32),
		"duplicate":               pubKey0 + "," + pubKey0,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := kwilnet.ParseNodePublicKeys(raw)
			require.Error(t, err)
			// Errors never echo the value.
			require.NotContains(t, err.Error(), strings.Repeat("7f", 8))
			require.NotContains(t, err.Error(), strings.Repeat("ab", 8))
		})
	}
}
