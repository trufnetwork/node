package tn_utils

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// Golden vectors are frozen in 0GoalModularAgentAddresses/2MAA-Plan.md §5.4 and were generated with
// go-ethereum keccak — the same hash these precompiles use. If these assertions fail, the on-chain
// derivation has drifted from the spec the SDKs implement. Two-step + token-agnostic: rules_hash carries
// NO bridge; rule_id is the full 32-byte keccak (an identifier, untruncated); maa_address is the 20-byte
// keccak[12:] of the composite (unrestricted, restricted, rule_id).

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
	// Vector A — bps fee, two actions (one with a body_hash). Input order is place,cancel to prove the
	// canonical sort (cancel < place) is applied regardless of input order. NO bridge.
	rhA, err := computeRulesHash(
		"bps", 250, "0",
		[]string{"main", "main"},
		[]string{"ob_place_order", "ob_cancel_order"},
		[][]byte{repeatByte(0xcc, 32), nil},
	)
	if err != nil {
		t.Fatalf("vector A: %v", err)
	}
	wantA := hexb(t, "df0555d336647bec5e9fe1f6f613086bddf53548b67c52393aef6db4cbef062d")
	if !bytes.Equal(rhA, wantA) {
		t.Fatalf("vector A rules_hash\n got %x\nwant %x", rhA, wantA)
	}

	// Vector B — flat fee 1e18, empty allow-list.
	rhB, err := computeRulesHash(
		"flat", 0, "1000000000000000000",
		[]string{}, []string{}, [][]byte{},
	)
	if err != nil {
		t.Fatalf("vector B: %v", err)
	}
	wantB := hexb(t, "0b1edb0ad70fb94287e50c7b3deaea7bba4e500c4ae6a764ed9021faf091274a")
	if !bytes.Equal(rhB, wantB) {
		t.Fatalf("vector B rules_hash\n got %x\nwant %x", rhB, wantB)
	}
}

func TestComputeRulesHash_OrderIndependentAndDedup(t *testing.T) {
	base, err := computeRulesHash("bps", 250, "0",
		[]string{"main", "main"},
		[]string{"ob_place_order", "ob_cancel_order"},
		[][]byte{repeatByte(0xcc, 32), nil})
	if err != nil {
		t.Fatal(err)
	}

	// Reversed input order must produce the same hash (canonical sort).
	reordered, err := computeRulesHash("bps", 250, "0",
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
	deduped, err := computeRulesHash("bps", 250, "0",
		[]string{"main", "main", "main"},
		[]string{"ob_place_order", "ob_cancel_order", "ob_place_order"},
		[][]byte{repeatByte(0xcc, 32), nil, repeatByte(0xcc, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(base, deduped) {
		t.Fatalf("duplicate entry changed the hash:\n base %x\n dedup %x", base, deduped)
	}

	// Conflicting body_hash for a duplicate (namespace, action): the LAST occurrence wins (plan §5.1
	// canonicalization rule 1: "last write wins for its body_hash"). The earlier 0xdd pin on
	// ob_place_order is dropped in favor of the trailing 0xcc, so the result must equal `base`.
	lastWins, err := computeRulesHash("bps", 250, "0",
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

func TestComputeRulesHash_Validation(t *testing.T) {
	if _, err := computeRulesHash("bogus", 0, "0", nil, nil, nil); err == nil {
		t.Fatal("expected error for bad fee_mode")
	}
	if _, err := computeRulesHash("bps", 0, "-1", nil, nil, nil); err == nil {
		t.Fatal("expected error for negative fee_flat")
	}
	if _, err := computeRulesHash("bps", 0, "0",
		[]string{"main"}, []string{"a"}, [][]byte{repeatByte(0x00, 31)}); err == nil {
		t.Fatal("expected error for 31-byte body_hash")
	}
	// Mismatched parallel-slice lengths must error, not panic or silently truncate.
	if _, err := computeRulesHash("bps", 0, "0",
		[]string{"main"}, []string{"a", "b"}, [][]byte{nil}); err == nil {
		t.Fatal("expected error for mismatched namespaces/actions/body_hashes lengths")
	}
}

func TestDeriveRuleID_GoldenVectors(t *testing.T) {
	restricted := repeatByte(0x11, 20)

	// Vector A: rules_hash from above, 32-byte salt of 0xab. rule_id is the FULL 32-byte keccak.
	rhA := hexb(t, "df0555d336647bec5e9fe1f6f613086bddf53548b67c52393aef6db4cbef062d")
	idA, err := deriveRuleID(restricted, rhA, repeatByte(0xab, 32))
	if err != nil {
		t.Fatalf("vector A: %v", err)
	}
	wantA := hexb(t, "a0b517da759b794e2484dc8b9dba8f5211a53dcdf26448f19c7c68699ff7bcf1")
	if !bytes.Equal(idA, wantA) {
		t.Fatalf("vector A rule_id\n got %x\nwant %x", idA, wantA)
	}
	if len(idA) != 32 {
		t.Fatalf("rule_id must be 32 bytes (untruncated), got %d", len(idA))
	}

	// Vector B: empty salt.
	rhB := hexb(t, "0b1edb0ad70fb94287e50c7b3deaea7bba4e500c4ae6a764ed9021faf091274a")
	idB, err := deriveRuleID(restricted, rhB, nil)
	if err != nil {
		t.Fatalf("vector B: %v", err)
	}
	wantB := hexb(t, "21f40fbf0fd537f85d283cf7b5f2fe8602c1f4b910aad96ad2dad9f6e82b1ca5")
	if !bytes.Equal(idB, wantB) {
		t.Fatalf("vector B rule_id\n got %x\nwant %x", idB, wantB)
	}
}

func TestDeriveRuleID_RejectsBadLengths(t *testing.T) {
	good20 := repeatByte(0x11, 20)
	good32 := repeatByte(0x33, 32)
	if _, err := deriveRuleID(repeatByte(0x11, 19), good32, nil); err == nil {
		t.Fatal("expected error for 19-byte restricted")
	}
	if _, err := deriveRuleID(good20, repeatByte(0x33, 31), nil); err == nil {
		t.Fatal("expected error for 31-byte rules_hash")
	}
}

func TestDeriveMAAAddress_GoldenVectors(t *testing.T) {
	unrestricted := repeatByte(0x22, 20)
	restricted := repeatByte(0x11, 20)

	// Vector A: composite (unrestricted, restricted, rule_id_A).
	idA := hexb(t, "a0b517da759b794e2484dc8b9dba8f5211a53dcdf26448f19c7c68699ff7bcf1")
	addrA, err := deriveMAAAddress(unrestricted, restricted, idA)
	if err != nil {
		t.Fatalf("vector A: %v", err)
	}
	wantA := hexb(t, "84da4dbca14d429c719d65a0bb76bd7fa3c5c349")
	if !bytes.Equal(addrA, wantA) {
		t.Fatalf("vector A maa_address\n got %x\nwant %x", addrA, wantA)
	}
	if len(addrA) != 20 {
		t.Fatalf("address must be 20 bytes, got %d", len(addrA))
	}

	// Vector B: rule_id_B.
	idB := hexb(t, "21f40fbf0fd537f85d283cf7b5f2fe8602c1f4b910aad96ad2dad9f6e82b1ca5")
	addrB, err := deriveMAAAddress(unrestricted, restricted, idB)
	if err != nil {
		t.Fatalf("vector B: %v", err)
	}
	wantB := hexb(t, "cb009e348c3ad795aa6d7d81177f0daee4583128")
	if !bytes.Equal(addrB, wantB) {
		t.Fatalf("vector B maa_address\n got %x\nwant %x", addrB, wantB)
	}
}

func TestDeriveMAAAddress_KeyChangesAddressAndDeterministic(t *testing.T) {
	u := repeatByte(0x22, 20)
	r := repeatByte(0x11, 20)
	id := repeatByte(0x33, 32)

	a1, err := deriveMAAAddress(u, r, id)
	if err != nil {
		t.Fatal(err)
	}
	// Determinism.
	a1b, err := deriveMAAAddress(u, r, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a1, a1b) {
		t.Fatal("derivation is not deterministic")
	}
	// Different funder -> different address (the funder disambiguates MAAs of one rule).
	a2, err := deriveMAAAddress(repeatByte(0x44, 20), r, id)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a1, a2) {
		t.Fatal("different unrestricted produced the same address")
	}
	// Different rule_id -> different address.
	a3, err := deriveMAAAddress(u, r, repeatByte(0x55, 32))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a1, a3) {
		t.Fatal("different rule_id produced the same address")
	}
	// Swapping unrestricted/restricted -> different address (order matters).
	swapped, err := deriveMAAAddress(r, u, id)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a1, swapped) {
		t.Fatal("swapping unrestricted/restricted produced the same address")
	}
}

func TestDeriveMAAAddress_RejectsBadLengths(t *testing.T) {
	good20 := repeatByte(0x22, 20)
	good32 := repeatByte(0x33, 32)
	if _, err := deriveMAAAddress(repeatByte(0x22, 19), good20, good32); err == nil {
		t.Fatal("expected error for 19-byte unrestricted")
	}
	if _, err := deriveMAAAddress(good20, repeatByte(0x11, 21), good32); err == nil {
		t.Fatal("expected error for 21-byte restricted")
	}
	if _, err := deriveMAAAddress(good20, good20, repeatByte(0x33, 31)); err == nil {
		t.Fatal("expected error for 31-byte rule_id")
	}
}
