package tn_local

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
)

// AuthVersion is the canonical-payload prefix locked for v1. Bumping this is
// a breaking change — every SDK and every server need to agree on it. The
// server rejects unknown versions outright (no silent fall-through).
const AuthVersion = "tn_local.auth.v1"

// AuthHeader carries the per-request signature, timestamp, and version.
// SDKs put this in the optional `_auth` field of every request when
// require_signature is enabled on the server. The verifier on the server
// recovers the signing address from sig+digest and asserts it equals
// ext.nodeAddress — the only address allowed to operate on this node.
type AuthHeader struct {
	// Sig is the 65-byte EVM-compatible secp256k1 signature, hex-encoded
	// with a 0x prefix (e.g. "0xabc…1c"). v is normalized to 27/28 on the
	// wire; verification accepts {0,1,27,28} for compatibility with SDKs
	// that emit either form.
	Sig string `json:"sig"`
	// Ts is unix-milliseconds at sign time. The server rejects requests
	// outside ±replayWindowSeconds of its own clock — keep your hosts in
	// NTP sync.
	Ts int64 `json:"ts"`
	// Ver is the canonical-payload version. Must be AuthVersion exactly.
	Ver string `json:"ver"`
}

// AuthEnvelope is embedded in every request type to add the optional `_auth`
// field. Embedding (rather than duplicating the field on each type) keeps
// canonical JSON marshalling consistent — the embedded struct flattens, so
// `_auth` always sits at the top level next to the request's own keys.
type AuthEnvelope struct {
	Auth *AuthHeader `json:"_auth,omitempty"`
}

// canonicalDigest builds the keccak256 digest the SDK signs (and the server
// verifies). The format is locked to AuthVersion. Bumping it requires
// coordinated SDK + node updates.
//
// Layout:
//
//	prefix         := AuthVersion ("tn_local.auth.v1")
//	method         := JSON-RPC method name (e.g. "local.create_stream")
//	paramsHashHex  := lowercase hex of sha256(canonicalJSON(params_without_auth))
//	tsDecimal      := strconv.FormatInt(ts_ms, 10)
//	payload        := prefix + "\n" + method + "\n" + paramsHashHex + "\n" + tsDecimal
//	digest         := keccak256(payload)
//
// Splitting params out into a hash keeps the signed payload bounded
// regardless of request size.
func canonicalDigest(method string, paramsCanonicalJSON []byte, tsMs int64) []byte {
	// sha256 (NOT keccak) for the params hash so SDKs can implement it
	// with stdlib hashlib/crypto.subtle.
	paramsSha := sha256.Sum256(paramsCanonicalJSON)
	paramsShaHex := hex.EncodeToString(paramsSha[:])
	payload := AuthVersion + "\n" + method + "\n" + paramsShaHex + "\n" + strconv.FormatInt(tsMs, 10)
	return crypto.Keccak256([]byte(payload))
}

// canonicalJSON re-encodes v as JSON with sorted keys, no whitespace, and
// stable number/string handling. Implementation: marshal the value, parse
// back into a generic structure (map[string]any / []any / scalars), then
// re-marshal — Go's encoding/json sorts map keys alphabetically when
// marshalling map[string]any, which is exactly what we need to produce
// language-independent canonical bytes (Python `json.dumps(sort_keys=True)`
// and JS sorted-key stringify produce the same bytes).
func canonicalJSON(v any) ([]byte, error) {
	first, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("canonical marshal: %w", err)
	}
	var generic any
	if err := json.Unmarshal(first, &generic); err != nil {
		return nil, fmt.Errorf("canonical reparse: %w", err)
	}
	return json.Marshal(generic)
}

// canonicalParams returns the canonical JSON of req with its embedded
// AuthEnvelope.Auth set to nil. Used by both signing (SDKs) and
// verification (server). The AuthSetter interface lets the server null out
// the auth field on any request type without reflection gymnastics.
func canonicalParams(req AuthSetter) ([]byte, error) {
	original := req.GetAuth()
	req.SetAuth(nil)
	defer req.SetAuth(original)
	return canonicalJSON(req)
}

// AuthSetter is implemented by every request type that embeds AuthEnvelope.
// Embedding the envelope automatically gives a request these methods.
type AuthSetter interface {
	GetAuth() *AuthHeader
	SetAuth(*AuthHeader)
}

// GetAuth / SetAuth on the embedded envelope satisfy AuthSetter for any
// struct that embeds AuthEnvelope.
func (e *AuthEnvelope) GetAuth() *AuthHeader  { return e.Auth }
func (e *AuthEnvelope) SetAuth(a *AuthHeader) { e.Auth = a }

// authError builds a JSON-RPC error for an auth failure. We deliberately
// don't echo back the failure reason in the message — the message stays
// generic so that misconfigured clients get a consistent signal without
// leaking server internals. The reason is included as data so debug logs
// or verbose clients can surface it.
//
// Code is ErrorInvalidParams (-32602) — the closest semantic fit in the
// existing kwil-db error taxonomy ("your `_auth` field is missing or
// wrong"). HTTP-level: dispatched as 400.
func authError(reason string) *jsonrpc.Error {
	data, _ := json.Marshal(map[string]string{"reason": reason})
	return jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "tn_local: unauthenticated", data)
}

// checkAuth validates the request's _auth field against ext.nodeAddress.
// Called by every handler when ext.requireSignature is true. Returns nil
// on success; a JSON-RPC error otherwise (always uses authError so callers
// can return it directly).
//
// Verification steps:
//  1. Header present and well-formed (sig hex, ts > 0, ver == AuthVersion).
//  2. Timestamp within ±replayWindowSeconds of server time.
//  3. Signature not previously seen within the window (replay cache).
//  4. Recovered address from sig+digest equals ext.nodeAddress (strict
//     case-insensitive match — both sides are lowercase 0x-prefixed).
func (ext *Extension) checkAuth(_ context.Context, method string, req AuthSetter) *jsonrpc.Error {
	if !ext.requireSignature.Load() {
		return nil
	}
	if req == nil {
		return authError("missing request")
	}
	auth := req.GetAuth()
	if auth == nil {
		return authError("missing _auth")
	}
	if auth.Ver != AuthVersion {
		return authError("unsupported auth version")
	}
	if auth.Sig == "" {
		return authError("missing signature")
	}
	if auth.Ts <= 0 {
		return authError("invalid timestamp")
	}

	// Clock skew check.
	windowMs := ext.replayWindowSeconds.Load() * 1000
	nowMs := time.Now().UnixMilli()
	delta := nowMs - auth.Ts
	if delta < 0 {
		delta = -delta
	}
	if delta > windowMs {
		return authError("timestamp outside window")
	}

	// Decode signature.
	sigHex := strings.TrimPrefix(strings.ToLower(auth.Sig), "0x")
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		return authError("malformed signature")
	}
	if len(sig) != 65 {
		return authError("signature must be 65 bytes")
	}
	// Normalize V: ethereum/crypto.Ecrecover wants {0,1}; SDKs may send
	// {0,1} or {27,28}. Strip the EVM offset if present.
	if sig[64] >= 27 {
		sig[64] -= 27
	}

	// Compute the canonical params hash (with auth field stripped).
	paramsBytes, err := canonicalParams(req)
	if err != nil {
		return authError("canonicalize failed")
	}
	digest := canonicalDigest(method, paramsBytes, auth.Ts)

	// Recover address.
	pub, err := crypto.Ecrecover(digest, sig)
	if err != nil {
		return authError("signature recovery failed")
	}
	pubKey, err := crypto.UnmarshalPubkey(pub)
	if err != nil {
		return authError("invalid recovered pubkey")
	}
	recoveredAddr := strings.ToLower(crypto.PubkeyToAddress(*pubKey).Hex())

	// Strict address match. ext.nodeAddress is lowercase 0x-hex by
	// construction (see deriveNodeAddress in tn_local.go).
	if recoveredAddr != ext.nodeAddress {
		return authError("signer is not this node's operator")
	}

	// Replay protection: hash the wire signature and dedup.
	sigKey := crypto.Keccak256Hash(sig).Hex()
	if !ext.replayCache.add(sigKey, time.Now()) {
		return authError("replay detected")
	}

	return nil
}

// ───────────────────────────── replay cache ─────────────────────────────
//
// In-memory LRU keyed by signature hash. Bounded by entry count so memory
// stays predictable under burst traffic. Each entry remembers its insertion
// time so we can age out stale entries lazily on add (matching the
// timestamp window — anything older than the window can never replay).
//
// Restart loses the cache. The timestamp window is what protects against
// post-restart replay (an old request with ts < now-window is rejected by
// checkAuth before we even consult the cache).

type replayEntry struct {
	key string
	at  time.Time
}

type replayCache struct {
	mu       sync.Mutex
	maxSize  int
	windowMs int64 // matches Extension.replayWindowSeconds at construction time
	order    *list.List               // doubly-linked list, oldest at front
	index    map[string]*list.Element // key → list element (for O(1) lookup + remove)
}

func newReplayCache(maxSize int, windowSeconds int64) *replayCache {
	return &replayCache{
		maxSize:  maxSize,
		windowMs: windowSeconds * 1000,
		order:    list.New(),
		index:    make(map[string]*list.Element, maxSize),
	}
}

// add returns true if the key was new (and stored), false if it was a
// duplicate within the window (replay).
func (c *replayCache) add(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict expired entries from the front (oldest first).
	cutoff := now.Add(-time.Duration(c.windowMs) * time.Millisecond)
	for e := c.order.Front(); e != nil; {
		entry := e.Value.(*replayEntry)
		if entry.at.After(cutoff) {
			break // remaining entries are newer
		}
		next := e.Next()
		c.order.Remove(e)
		delete(c.index, entry.key)
		e = next
	}

	if _, dup := c.index[key]; dup {
		return false
	}

	// Evict by size if needed.
	for c.order.Len() >= c.maxSize {
		front := c.order.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*replayEntry)
		c.order.Remove(front)
		delete(c.index, entry.key)
	}

	entry := &replayEntry{key: key, at: now}
	elem := c.order.PushBack(entry)
	c.index[key] = elem
	return true
}

// size returns the current entry count (test helper).
func (c *replayCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
