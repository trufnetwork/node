package kwil_network

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// NodeKeys is the public identity of a validator node. Private keys are not
// part of synthesis: each node reads its own key from SSM at boot.
type NodeKeys struct {
	KeyType      string
	PublicKeyHex string
}

// NodeId returns the kwild node ID, <public key hex>#<key type>.
func (k NodeKeys) NodeId() string {
	return k.PublicKeyHex + "#" + k.KeyType
}

const defaultKeyType = "secp256k1"

// publicKeyHexLen is the hex length of a public key for each key type: 33
// bytes for a compressed secp256k1 key, 32 bytes for an ed25519 key.
var publicKeyHexLen = map[string]int{
	"secp256k1": 66,
	"ed25519":   64,
}

// ParseNodePublicKeys parses a comma-separated list of node IDs in node order
// (entry 0 is node-1). Each entry is <public key hex>#<key type>, as printed by
// `kwild key info`, or a bare secp256k1 public key. Errors name entries by
// position only and never echo the value.
func ParseNodePublicKeys(raw string) ([]NodeKeys, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("no node public keys given")
	}

	var keys []NodeKeys
	seen := map[string]int{}
	for i, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("entry %d is empty", i)
		}

		keyHex, keyType, hasType := strings.Cut(entry, "#")
		if !hasType {
			keyType = defaultKeyType
		}
		keyHex = strings.ToLower(keyHex)

		wantLen, ok := publicKeyHexLen[keyType]
		if !ok {
			return nil, fmt.Errorf("entry %d: unsupported key type (want secp256k1 or ed25519)", i)
		}
		if len(keyHex) != wantLen {
			return nil, fmt.Errorf("entry %d: a %s public key has %d hex characters, got %d", i, keyType, wantLen, len(keyHex))
		}
		if _, err := hex.DecodeString(keyHex); err != nil {
			return nil, fmt.Errorf("entry %d: public key is not valid hex", i)
		}
		if keyType == "secp256k1" && !strings.HasPrefix(keyHex, "02") && !strings.HasPrefix(keyHex, "03") {
			return nil, fmt.Errorf("entry %d: not a compressed secp256k1 public key", i)
		}
		if first, dup := seen[keyHex]; dup {
			return nil, fmt.Errorf("entries %d and %d are the same key", first, i)
		}
		seen[keyHex] = i

		keys = append(keys, NodeKeys{KeyType: keyType, PublicKeyHex: keyHex})
	}
	return keys, nil
}
