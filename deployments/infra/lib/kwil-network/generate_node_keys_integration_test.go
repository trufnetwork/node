package kwil_network_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kwilnet "github.com/trufnetwork/node/infra/lib/kwil-network"
)

func TestGenerateNodeKeys_Integration(t *testing.T) {
	kwildPath := findKwild(t)
	t.Setenv("KWILD_CLI_PATH", kwildPath)

	scope := minimalConstruct()
	keys := kwilnet.GenerateNodeKeys(scope)

	// Check fields based on the updated NodeKeys struct
	require.NotEmpty(t, keys.KeyType, "key_type should not be empty")
	require.Equal(t, "secp256k1", keys.KeyType, "expected key_type to be secp256k1") // Assuming secp256k1 for now

	require.NotEmpty(t, keys.PrivateKeyHex, "private_key_text (PrivateKeyHex) should not be empty")
	_, err := hex.DecodeString(keys.PrivateKeyHex)
	require.NoError(t, err, "private_key_text (PrivateKeyHex) should be valid hex")

	require.NotEmpty(t, keys.PublicKeyHex, "public_key_hex (PublicKeyHex) should not be empty")
	_, err = hex.DecodeString(keys.PublicKeyHex)
	require.NoError(t, err, "public_key_hex (PublicKeyHex) should be valid hex")

	require.NotEmpty(t, keys.NodeId, "node_id should not be empty")
	// Example check: NodeId should contain the PublicKeyCometizedHex
	require.Contains(t, keys.NodeId, keys.PublicKeyHex, "node_id should contain the public_key_hex part")
	require.Contains(t, keys.NodeId, "#"+keys.KeyType, "node_id should contain the key type suffix")

	require.NotEmpty(t, keys.Address, "user_address (Address) should not be empty")
	// Basic check for Ethereum-like address format (0x prefix, 40 hex chars)
	require.True(t, strings.HasPrefix(keys.Address, "0x"), "user_address (Address) should start with 0x")
	require.Len(t, keys.Address, 42, "user_address (Address) should be 42 characters long (0x + 40 hex)")
	_, err = hex.DecodeString(keys.Address[2:]) // Decode hex part after 0x
	require.NoError(t, err, "user_address (Address) hex part should be valid hex")
}
