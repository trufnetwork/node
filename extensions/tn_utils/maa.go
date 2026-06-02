package tn_utils

// Modular Agent Address (MAA) derivation precompiles.
//
// Two pure, deterministic functions back the MAA rule store (migration 048):
//
//   tn_utils.compute_rules_hash(fee_mode, fee_bps, fee_flat, bridge, namespaces[], actions[], body_hashes[])
//       -> keccak256(RULES_PREIMAGE)                                    (32 bytes)
//   tn_utils.derive_maa_address(restricted, unrestricted, rules_hash, salt)
//       -> keccak256(ADDRESS_PREIMAGE)[12:32]                           (20 bytes)
//
// The exact byte layout is frozen in 0GoalModularAgentAddresses/5RulesHash-Preimage-Spec.md and
// MUST stay byte-identical to the SDK implementations (a mismatch sends funds to the wrong address).
// keccak256 here is Ethereum/legacy Keccak (go-ethereum crypto.Keccak256), NOT NIST SHA3-256.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

const (
	maaRulesVersion byte = 0x01 // RULES_PREIMAGE leading version byte (doc 5 §1)
	maaAddrVersion  byte = 0x01 // ADDRESS_PREIMAGE leading version byte (doc 5 §2)
)

// ---------------------------------------------------------------------------
// derive_maa_address
// ---------------------------------------------------------------------------

func deriveMAAAddressMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "derive_maa_address",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("restricted", types.ByteaType, false),
			precompiles.NewPrecompileValue("unrestricted", types.ByteaType, false),
			precompiles.NewPrecompileValue("rules_hash", types.ByteaType, false),
			precompiles.NewPrecompileValue("salt", types.ByteaType, true), // nullable: empty salt allowed
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("maa_address", types.ByteaType, false),
			},
		},
		Handler: deriveMAAAddressHandler,
	}
}

func deriveMAAAddressHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	restricted, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return fmt.Errorf("restricted: %w", err)
	}
	unrestricted, err := toByteSliceAllowNil(inputs[1])
	if err != nil {
		return fmt.Errorf("unrestricted: %w", err)
	}
	rulesHash, err := toByteSliceAllowNil(inputs[2])
	if err != nil {
		return fmt.Errorf("rules_hash: %w", err)
	}
	salt, err := toByteSliceAllowNil(inputs[3])
	if err != nil {
		return fmt.Errorf("salt: %w", err)
	}

	addr, err := deriveMAAAddress(restricted, unrestricted, rulesHash, salt)
	if err != nil {
		return err
	}
	return resultFn([]any{addr})
}

// deriveMAAAddress builds the canonical ADDRESS_PREIMAGE (doc 5 §2) and returns the low 20 bytes
// of keccak256(preimage) — the Ethereum-style MAA address.
func deriveMAAAddress(restricted, unrestricted, rulesHash, salt []byte) ([]byte, error) {
	if len(restricted) != 20 {
		return nil, fmt.Errorf("restricted must be 20 bytes, got %d", len(restricted))
	}
	if len(unrestricted) != 20 {
		return nil, fmt.Errorf("unrestricted must be 20 bytes, got %d", len(unrestricted))
	}
	if len(rulesHash) != 32 {
		return nil, fmt.Errorf("rules_hash must be 32 bytes, got %d", len(rulesHash))
	}

	// ADDRESS_PREIMAGE = version ‖ restricted ‖ unrestricted ‖ rules_hash ‖ salt   (salt last/variable)
	var buf bytes.Buffer
	buf.WriteByte(maaAddrVersion)
	buf.Write(restricted)
	buf.Write(unrestricted)
	buf.Write(rulesHash)
	buf.Write(salt)

	full := ethcrypto.Keccak256(buf.Bytes()) // 32 bytes
	out := make([]byte, 20)
	copy(out, full[12:32]) // low 20 bytes
	return out, nil
}

// ---------------------------------------------------------------------------
// compute_rules_hash
// ---------------------------------------------------------------------------

func computeRulesHashMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "compute_rules_hash",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("fee_mode", types.TextType, false),
			precompiles.NewPrecompileValue("fee_bps", types.IntType, false),
			precompiles.NewPrecompileValue("fee_flat", types.TextType, false), // decimal string of base units
			precompiles.NewPrecompileValue("bridge", types.TextType, false),
			precompiles.NewPrecompileValue("namespaces", types.TextArrayType, false),
			precompiles.NewPrecompileValue("actions", types.TextArrayType, false),
			precompiles.NewPrecompileValue("body_hashes", types.ByteaArrayType, true), // nullable elements
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("rules_hash", types.ByteaType, false),
			},
		},
		Handler: computeRulesHashHandler,
	}
}

type maaAllowEntry struct {
	namespace string
	action    string
	bodyHash  []byte // nil/empty = none, else 32 bytes
}

func computeRulesHashHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	feeMode, err := toStr(inputs[0])
	if err != nil {
		return fmt.Errorf("fee_mode: %w", err)
	}
	feeBps, err := toInt64(inputs[1])
	if err != nil {
		return fmt.Errorf("fee_bps: %w", err)
	}
	feeFlatStr, err := toStr(inputs[2])
	if err != nil {
		return fmt.Errorf("fee_flat: %w", err)
	}
	bridge, err := toStr(inputs[3])
	if err != nil {
		return fmt.Errorf("bridge: %w", err)
	}
	namespaces, err := toStringSliceArray(inputs[4])
	if err != nil {
		return fmt.Errorf("namespaces: %w", err)
	}
	actions, err := toStringSliceArray(inputs[5])
	if err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	var bodyHashes [][]byte
	if inputs[6] != nil {
		bodyHashes, err = toByteSliceArray(inputs[6])
		if err != nil {
			return fmt.Errorf("body_hashes: %w", err)
		}
	}
	// Empty allow-list with a NULL body_hashes array → treat as zero-length to match namespaces/actions.
	if len(bodyHashes) == 0 && len(namespaces) > 0 {
		bodyHashes = make([][]byte, len(namespaces))
	}

	if len(namespaces) != len(actions) || len(namespaces) != len(bodyHashes) {
		return fmt.Errorf("namespaces/actions/body_hashes must be equal length (%d/%d/%d)",
			len(namespaces), len(actions), len(bodyHashes))
	}

	hash, err := computeRulesHash(feeMode, feeBps, feeFlatStr, bridge, namespaces, actions, bodyHashes)
	if err != nil {
		return err
	}
	return resultFn([]any{hash})
}

// computeRulesHash builds the canonical RULES_PREIMAGE (doc 5 §1) and returns keccak256(preimage).
func computeRulesHash(feeMode string, feeBps int64, feeFlatStr, bridge string, namespaces, actions []string, bodyHashes [][]byte) ([]byte, error) {
	// Defensive: the three allow-list slices are indexed in lockstep below. The on-chain handler
	// already equalizes them, but guard the pure function so a direct caller gets an error instead
	// of an index-out-of-range panic or a silently-truncated hash.
	if len(namespaces) != len(actions) || len(namespaces) != len(bodyHashes) {
		return nil, fmt.Errorf("namespaces/actions/body_hashes must be equal length (%d/%d/%d)",
			len(namespaces), len(actions), len(bodyHashes))
	}

	var b bytes.Buffer

	b.WriteByte(maaRulesVersion)

	switch feeMode {
	case "bps":
		b.WriteByte(0x00)
	case "flat":
		b.WriteByte(0x01)
	default:
		return nil, fmt.Errorf("fee_mode must be 'bps' or 'flat', got %q", feeMode)
	}

	if feeBps < 0 || feeBps > math.MaxUint32 {
		return nil, fmt.Errorf("fee_bps out of uint32 range: %d", feeBps)
	}
	var bps [4]byte
	binary.BigEndian.PutUint32(bps[:], uint32(feeBps))
	b.Write(bps[:])

	feeFlat, err := parseFeeFlat(feeFlatStr)
	if err != nil {
		return nil, err
	}
	var ff [32]byte
	feeFlat.FillBytes(ff[:]) // big-endian, left-zero-padded
	b.Write(ff[:])

	if err := maaWriteLP8(&b, []byte(bridge)); err != nil {
		return nil, fmt.Errorf("bridge: %w", err)
	}

	// Canonicalize: dedup by (namespace, action), sort bytewise.
	dedup := make(map[string]maaAllowEntry, len(namespaces))
	for i := range namespaces {
		e := maaAllowEntry{namespace: namespaces[i], action: actions[i], bodyHash: bodyHashes[i]}
		dedup[e.namespace+"\x00"+e.action] = e
	}
	entries := make([]maaAllowEntry, 0, len(dedup))
	for _, e := range dedup {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].namespace != entries[j].namespace {
			return entries[i].namespace < entries[j].namespace // bytewise on UTF-8
		}
		return entries[i].action < entries[j].action
	})

	if len(entries) > 0xffff {
		return nil, fmt.Errorf("too many allow-list entries: %d", len(entries))
	}
	var cnt [2]byte
	binary.BigEndian.PutUint16(cnt[:], uint16(len(entries)))
	b.Write(cnt[:])

	for _, e := range entries {
		if err := maaWriteLP8(&b, []byte(e.namespace)); err != nil {
			return nil, fmt.Errorf("namespace %q: %w", e.namespace, err)
		}
		if err := maaWriteLP8(&b, []byte(e.action)); err != nil {
			return nil, fmt.Errorf("action %q: %w", e.action, err)
		}
		switch len(e.bodyHash) {
		case 0:
			b.WriteByte(0x00)
		case 32:
			b.WriteByte(0x01)
			b.Write(e.bodyHash)
		default:
			return nil, fmt.Errorf("body_hash for %s.%s must be 32 bytes, got %d", e.namespace, e.action, len(e.bodyHash))
		}
	}

	return ethcrypto.Keccak256(b.Bytes()), nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// maaWriteLP8 writes a uint8 length prefix followed by the bytes (doc 5 §1 length-prefixed strings).
func maaWriteLP8(buf *bytes.Buffer, p []byte) error {
	if len(p) > 0xff {
		return fmt.Errorf("length-prefixed field exceeds 255 bytes (got %d)", len(p))
	}
	buf.WriteByte(byte(len(p)))
	buf.Write(p)
	return nil
}

// parseFeeFlat parses a base-unit decimal string into a non-negative big.Int that fits in 256 bits.
func parseFeeFlat(s string) (*big.Int, error) {
	if s == "" {
		return big.NewInt(0), nil
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("fee_flat is not a base-10 integer: %q", s)
	}
	if v.Sign() < 0 {
		return nil, fmt.Errorf("fee_flat must be non-negative: %s", s)
	}
	if v.BitLen() > 256 {
		return nil, fmt.Errorf("fee_flat exceeds 2^256: %s", s)
	}
	return v, nil
}

// toStr normalizes a TEXT precompile input to a Go string.
func toStr(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case *string:
		if v == nil {
			return "", nil
		}
		return *v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("expected string, got %T", value)
	}
}

// toStringSliceArray normalizes a TEXT[] precompile input to []string.
func toStringSliceArray(value any) ([]string, error) {
	switch v := value.(type) {
	case []string:
		return v, nil
	case []*string:
		out := make([]string, len(v))
		for i, p := range v {
			if p != nil {
				out[i] = *p
			}
		}
		return out, nil
	case []any:
		out := make([]string, len(v))
		for i, elem := range v {
			s, err := toStr(elem)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = s
		}
		return out, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("expected []string, got %T", value)
	}
}
