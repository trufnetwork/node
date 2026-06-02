package tn_utils

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// Golden vectors are frozen in 0GoalModularAgentAddresses/5RulesHash-Preimage-Spec.md §4 and were
// generated with go-ethereum keccak — the same hash these precompiles use. If these assertions
// fail, the on-chain derivation has drifted from the spec the SDKs implement.

func hexb(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func repeatByte(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestComputeRulesHash_GoldenVectors(t *testing.T) {
	// Vector A — bps fee, eth_truf, two actions (one with a body_hash). Input order is place,cancel
	// to prove the canonical sort (cancel < place) is applied regardless of input order.
	rhA, err := computeRulesHash(
		"bps", 250, "0", "eth_truf",
		[]string{"main", "main"},
		[]string{"ob_place_order", "ob_cancel_order"},
		[][]byte{repeatByte(0xcc, 32), nil},
	)
	if err != nil {
		t.Fatalf("vector A: %v", err)
	}
	wantA := hexb(t, "2d43a48f5715b66c65f248aa5e1d6ac50270f9e572d0e2c03134856664cba56c")
	if !bytes.Equal(rhA, wantA) {
		t.Fatalf("vector A rules_hash\n got %x\nwant %x", rhA, wantA)
	}

	// Vector B — flat fee 1e18, eth_usdc, empty allow-list.
	rhB, err := computeRulesHash(
		"flat", 0, "1000000000000000000", "eth_usdc",
		[]string{}, []string{}, [][]byte{},
	)
	if err != nil {
		t.Fatalf("vector B: %v", err)
	}
	wantB := hexb(t, "2db75f81283c5f555119e0df2f9c136d59afa17edfefba6ca4c23fc0715d4599")
	if !bytes.Equal(rhB, wantB) {
		t.Fatalf("vector B rules_hash\n got %x\nwant %x", rhB, wantB)
	}
}

func TestComputeRulesHash_OrderIndependentAndDedup(t *testing.T) {
	base, err := computeRulesHash("bps", 250, "0", "eth_truf",
		[]string{"main", "main"},
		[]string{"ob_place_order", "ob_cancel_order"},
		[][]byte{repeatByte(0xcc, 32), nil})
	if err != nil {
		t.Fatal(err)
	}

	// Reversed input order must produce the same hash (canonical sort).
	reordered, err := computeRulesHash("bps", 250, "0", "eth_truf",
		[]string{"main", "main"},
		[]string{"ob_cancel_order", "ob_place_order"},
		[][]byte{nil, repeatByte(0xcc, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(base, reordered) {
		t.Fatalf("reordered allow-list changed the hash:\n base %x\n reord %x", base, reordered)
	}

	// A duplicate (namespace, action) must not change the hash (dedup).
	deduped, err := computeRulesHash("bps", 250, "0", "eth_truf",
		[]string{"main", "main", "main"},
		[]string{"ob_place_order", "ob_cancel_order", "ob_place_order"},
		[][]byte{repeatByte(0xcc, 32), nil, repeatByte(0xcc, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(base, deduped) {
		t.Fatalf("duplicate entry changed the hash:\n base %x\n dedup %x", base, deduped)
	}

	// Conflicting body_hash for a duplicate (namespace, action): the LAST occurrence wins
	// (5RulesHash-Preimage-Spec.md §1 canonicalization rule 1: "last write wins for its body_hash").
	// The earlier 0xdd pin on ob_place_order is dropped in favor of the trailing 0xcc, so the result
	// must equal `base` (which pins ob_place_order to 0xcc).
	lastWins, err := computeRulesHash("bps", 250, "0", "eth_truf",
		[]string{"main", "main", "main"},
		[]string{"ob_place_order", "ob_cancel_order", "ob_place_order"},
		[][]byte{repeatByte(0xdd, 32), nil, repeatByte(0xcc, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(base, lastWins) {
		t.Fatalf("last-write-wins not honored for a conflicting body_hash:\n base %x\n lastWins %x", base, lastWins)
	}
}

func TestDeriveMAAAddress_GoldenVectors(t *testing.T) {
	restricted := repeatByte(0x11, 20)
	unrestricted := repeatByte(0x22, 20)

	// Vector A: rules_hash from above, 32-byte salt of 0xab.
	rhA := hexb(t, "2d43a48f5715b66c65f248aa5e1d6ac50270f9e572d0e2c03134856664cba56c")
	addrA, err := deriveMAAAddress(restricted, unrestricted, rhA, repeatByte(0xab, 32))
	if err != nil {
		t.Fatalf("vector A: %v", err)
	}
	wantA := hexb(t, "79ce248b31fc0d2016a175b36f79c5726b40387a")
	if !bytes.Equal(addrA, wantA) {
		t.Fatalf("vector A maa_address\n got %x\nwant %x", addrA, wantA)
	}
	if len(addrA) != 20 {
		t.Fatalf("address must be 20 bytes, got %d", len(addrA))
	}

	// Vector B: empty salt.
	rhB := hexb(t, "2db75f81283c5f555119e0df2f9c136d59afa17edfefba6ca4c23fc0715d4599")
	addrB, err := deriveMAAAddress(restricted, unrestricted, rhB, nil)
	if err != nil {
		t.Fatalf("vector B: %v", err)
	}
	wantB := hexb(t, "3ffaf6bb0c476826d28bb7a1a3b829dabd28cab4")
	if !bytes.Equal(addrB, wantB) {
		t.Fatalf("vector B maa_address\n got %x\nwant %x", addrB, wantB)
	}
}

func TestDeriveMAAAddress_SaltAndKeyChangeAddress(t *testing.T) {
	r := repeatByte(0x11, 20)
	u := repeatByte(0x22, 20)
	rh := repeatByte(0x33, 32)

	a1, err := deriveMAAAddress(r, u, rh, repeatByte(0x01, 32))
	if err != nil {
		t.Fatal(err)
	}
	// Different salt -> different address.
	a2, err := deriveMAAAddress(r, u, rh, repeatByte(0x02, 32))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a1, a2) {
		t.Fatal("different salt produced the same address")
	}
	// Determinism.
	a1b, err := deriveMAAAddress(r, u, rh, repeatByte(0x01, 32))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a1, a1b) {
		t.Fatal("derivation is not deterministic")
	}
	// Swapping restricted/unrestricted -> different address (order matters).
	swapped, err := deriveMAAAddress(u, r, rh, repeatByte(0x01, 32))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a1, swapped) {
		t.Fatal("swapping restricted/unrestricted produced the same address")
	}
}

func TestDeriveMAAAddress_RejectsBadLengths(t *testing.T) {
	good20 := repeatByte(0x11, 20)
	good32 := repeatByte(0x33, 32)
	if _, err := deriveMAAAddress(repeatByte(0x11, 19), good20, good32, nil); err == nil {
		t.Fatal("expected error for 19-byte restricted")
	}
	if _, err := deriveMAAAddress(good20, repeatByte(0x22, 21), good32, nil); err == nil {
		t.Fatal("expected error for 21-byte unrestricted")
	}
	if _, err := deriveMAAAddress(good20, good20, repeatByte(0x33, 31), nil); err == nil {
		t.Fatal("expected error for 31-byte rules_hash")
	}
}

func TestComputeRulesHash_Validation(t *testing.T) {
	if _, err := computeRulesHash("bogus", 0, "0", "eth_truf", nil, nil, nil); err == nil {
		t.Fatal("expected error for bad fee_mode")
	}
	if _, err := computeRulesHash("bps", 0, "-1", "eth_truf", nil, nil, nil); err == nil {
		t.Fatal("expected error for negative fee_flat")
	}
	if _, err := computeRulesHash("bps", 0, "0", "eth_truf",
		[]string{"main"}, []string{"a"}, [][]byte{repeatByte(0x00, 31)}); err == nil {
		t.Fatal("expected error for 31-byte body_hash")
	}
	// Mismatched parallel-slice lengths must error, not panic (index-out-of-range) or silently truncate.
	if _, err := computeRulesHash("bps", 0, "0", "eth_truf",
		[]string{"main"}, []string{"a", "b"}, [][]byte{nil}); err == nil {
		t.Fatal("expected error for mismatched namespaces/actions/body_hashes lengths")
	}
}
