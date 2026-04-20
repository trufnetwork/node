package tn_local

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
)

// ─── Canonical primitives ────────────────────────────────────────────────

func TestCanonicalJSON_DeterministicAndSorted(t *testing.T) {
	// Two semantically equal inputs in different field order must produce
	// identical canonical bytes. This is what makes the auth signature
	// reproducible across SDKs that build params with different key orders.
	a, err := canonicalJSON(map[string]any{"b": 2, "a": 1})
	require.NoError(t, err)
	b, err := canonicalJSON(map[string]any{"a": 1, "b": 2})
	require.NoError(t, err)
	require.Equal(t, string(a), string(b), "canonical JSON must be order-independent")
	require.Equal(t, `{"a":1,"b":2}`, string(a))
}

func TestCanonicalDigest_Stable(t *testing.T) {
	params := []byte(`{"stream_id":"st00000000000000000000000000demo","stream_type":"primitive"}`)
	d1 := canonicalDigest(MethodCreateStream, params, 1700000000000)
	d2 := canonicalDigest(MethodCreateStream, params, 1700000000000)
	require.Equal(t, d1, d2, "same inputs must produce same digest")

	// Method change → different digest.
	d3 := canonicalDigest(MethodDeleteStream, params, 1700000000000)
	require.NotEqual(t, d1, d3)

	// Timestamp change → different digest.
	d4 := canonicalDigest(MethodCreateStream, params, 1700000000001)
	require.NotEqual(t, d1, d4)

	// Params change → different digest.
	d5 := canonicalDigest(MethodCreateStream, []byte(`{"stream_id":"x","stream_type":"primitive"}`), 1700000000000)
	require.NotEqual(t, d1, d5)
}

// ─── Replay cache ────────────────────────────────────────────────────────

func TestReplayCache_RejectsDuplicateInWindow(t *testing.T) {
	c := newReplayCache(100, 60)
	now := time.Now()
	require.True(t, c.add("sig1", now), "first insert should succeed")
	require.False(t, c.add("sig1", now.Add(10*time.Second)), "duplicate within window should fail")
}

func TestReplayCache_AcceptsAfterWindowExpires(t *testing.T) {
	c := newReplayCache(100, 60)
	t0 := time.Now()
	require.True(t, c.add("sig1", t0))
	// Insert past the window — eviction during this add should remove sig1
	// from the cache, so a subsequent insert of sig1 within the new window
	// succeeds again. Use 120s gap so sig1 is definitely outside.
	require.True(t, c.add("sig2", t0.Add(120*time.Second)))
	require.True(t, c.add("sig1", t0.Add(120*time.Second)), "expired sig should re-add cleanly")
}

func TestReplayCache_BoundsSize(t *testing.T) {
	c := newReplayCache(2, 60)
	now := time.Now()
	require.True(t, c.add("a", now))
	require.True(t, c.add("b", now))
	require.True(t, c.add("c", now)) // evicts "a"
	require.Equal(t, 2, c.size())
	// "a" is gone, so re-adding succeeds (proves it was evicted, not retained).
	require.True(t, c.add("a", now))
}

// ─── checkAuth helpers ───────────────────────────────────────────────────

// genKey returns a random secp256k1 key + its lowercase 0x-hex address.
func genKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	k, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := strings.ToLower(crypto.PubkeyToAddress(k.PublicKey).Hex())
	return k, addr
}

// signRequest signs the given request with the given key, returning a
// populated AuthHeader. Sets req.Auth = nil during signing (signature is
// over params-without-auth) and restores it.
func signRequest(t *testing.T, key *ecdsa.PrivateKey, method string, req AuthSetter, tsMs int64) *AuthHeader {
	t.Helper()
	prev := req.GetAuth()
	req.SetAuth(nil)
	defer req.SetAuth(prev)

	params, err := canonicalJSON(req)
	require.NoError(t, err)
	digest := canonicalDigest(method, params, tsMs)
	sig, err := crypto.Sign(digest, key)
	require.NoError(t, err)
	// Normalize V to {27,28} on the wire (matches what tn_attestation
	// emits and what most EVM tooling expects).
	if sig[64] < 27 {
		sig[64] += 27
	}
	return &AuthHeader{
		Sig: "0x" + hex.EncodeToString(sig),
		Ts:  tsMs,
		Ver: AuthVersion,
	}
}

// authExtension returns an Extension configured with the given operator
// address and require_signature=true. Window is 60s.
func authExtension(t *testing.T, operatorAddr string) *Extension {
	t.Helper()
	ext := newTestExtension(nil)
	ext.nodeAddress = strings.ToLower(operatorAddr)
	ext.configureAuth(true, 60)
	return ext
}

// ─── checkAuth: header-shape failures ────────────────────────────────────

func TestCheckAuth_FlagOff_AlwaysPasses(t *testing.T) {
	ext := newTestExtension(nil)
	// requireSignature defaults to false.
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	require.Nil(t, ext.checkAuth(context.Background(), MethodCreateStream, req))

	// Even an invalid `_auth` is ignored when the flag is off (tradeoff: we
	// don't pre-validate to avoid client bugs being silent — but we also
	// don't reject, so the flag-off behavior is identical to pre-auth).
	req.Auth = &AuthHeader{Sig: "garbage", Ts: 1, Ver: "wrong"}
	require.Nil(t, ext.checkAuth(context.Background(), MethodCreateStream, req))
}

func TestCheckAuth_MissingAuth_Rejects(t *testing.T) {
	_, addr := genKey(t)
	ext := authExtension(t, addr)
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	authErr := ext.checkAuth(context.Background(), MethodCreateStream, req)
	require.NotNil(t, authErr)
	require.Equal(t, jsonrpc.ErrorInvalidParams, authErr.Code)
}

func TestCheckAuth_WrongVersion_Rejects(t *testing.T) {
	key, addr := genKey(t)
	ext := authExtension(t, addr)
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	req.Auth = signRequest(t, key, MethodCreateStream, req, time.Now().UnixMilli())
	req.Auth.Ver = "tn_local.auth.v999"
	authErr := ext.checkAuth(context.Background(), MethodCreateStream, req)
	require.NotNil(t, authErr)
}

func TestCheckAuth_MalformedSig_Rejects(t *testing.T) {
	_, addr := genKey(t)
	ext := authExtension(t, addr)
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	req.Auth = &AuthHeader{Sig: "not-hex", Ts: time.Now().UnixMilli(), Ver: AuthVersion}
	require.NotNil(t, ext.checkAuth(context.Background(), MethodCreateStream, req))

	req.Auth.Sig = "0xabcd" // valid hex but wrong length
	require.NotNil(t, ext.checkAuth(context.Background(), MethodCreateStream, req))
}

func TestCheckAuth_TimestampOutsideWindow_Rejects(t *testing.T) {
	key, addr := genKey(t)
	ext := authExtension(t, addr)
	// 10 minutes in the past — well outside the 60s window.
	staleTs := time.Now().Add(-10 * time.Minute).UnixMilli()
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	req.Auth = signRequest(t, key, MethodCreateStream, req, staleTs)
	authErr := ext.checkAuth(context.Background(), MethodCreateStream, req)
	require.NotNil(t, authErr)
}

// ─── checkAuth: cryptographic correctness ────────────────────────────────

func TestCheckAuth_OperatorSignedRequest_Accepts(t *testing.T) {
	key, addr := genKey(t)
	ext := authExtension(t, addr)
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	req.Auth = signRequest(t, key, MethodCreateStream, req, time.Now().UnixMilli())
	require.Nil(t, ext.checkAuth(context.Background(), MethodCreateStream, req))
}

// HEADLINE TEST — the one the user specifically asked for.
// A request signed by a key OTHER than the node operator's key MUST be
// rejected, even when every other field is valid. This is the property
// that closes the "anyone reaching admin acts as operator" gap.
func TestCheckAuth_WrongKey_Rejects(t *testing.T) {
	_, operatorAddr := genKey(t)
	attackerKey, attackerAddr := genKey(t)
	require.NotEqual(t, operatorAddr, attackerAddr, "test setup: addresses must differ")

	ext := authExtension(t, operatorAddr)

	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	// Attacker signs with their own key — sig is valid, ts is fresh, ver is
	// correct. The only thing wrong is *who signed*.
	req.Auth = signRequest(t, attackerKey, MethodCreateStream, req, time.Now().UnixMilli())

	authErr := ext.checkAuth(context.Background(), MethodCreateStream, req)
	require.NotNil(t, authErr, "wrong-key signature must be rejected")
	require.Equal(t, jsonrpc.ErrorInvalidParams, authErr.Code)
}

func TestCheckAuth_TamperedParams_Rejects(t *testing.T) {
	key, addr := genKey(t)
	ext := authExtension(t, addr)

	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	req.Auth = signRequest(t, key, MethodCreateStream, req, time.Now().UnixMilli())

	// Tamper with the params after signing — the signature still recovers
	// to the operator's address, but the digest no longer matches what the
	// server computes from the (tampered) params.
	req.StreamID = "st00000000000000000000000000ATTAK"
	authErr := ext.checkAuth(context.Background(), MethodCreateStream, req)
	require.NotNil(t, authErr, "tampered params must invalidate the signature")
}

func TestCheckAuth_ReplayWithinWindow_Rejects(t *testing.T) {
	key, addr := genKey(t)
	ext := authExtension(t, addr)
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	req.Auth = signRequest(t, key, MethodCreateStream, req, time.Now().UnixMilli())

	// First call accepts.
	require.Nil(t, ext.checkAuth(context.Background(), MethodCreateStream, req))
	// Second call with the SAME signature is a replay.
	authErr := ext.checkAuth(context.Background(), MethodCreateStream, req)
	require.NotNil(t, authErr, "replay of a previously-accepted signature must be rejected")
}

func TestCheckAuth_DifferentMethod_Rejects(t *testing.T) {
	key, addr := genKey(t)
	ext := authExtension(t, addr)

	// Sign for create_stream, replay against delete_stream — the digest
	// includes the method name so this must fail even though the params
	// happen to share a stream_id field.
	createReq := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	createReq.Auth = signRequest(t, key, MethodCreateStream, createReq, time.Now().UnixMilli())

	deleteReq := &DeleteStreamRequest{StreamID: "st00000000000000000000000000demo"}
	deleteReq.Auth = createReq.Auth

	authErr := ext.checkAuth(context.Background(), MethodDeleteStream, deleteReq)
	require.NotNil(t, authErr, "signature bound to a different method must not authenticate this one")
}

// ─── Verify the embedded AuthEnvelope plumbing ───────────────────────────

func TestAuthEnvelope_GetSet(t *testing.T) {
	req := &CreateStreamRequest{StreamID: "st00000000000000000000000000demo", StreamType: "primitive"}
	require.Nil(t, req.GetAuth(), "default Auth is nil")

	a := &AuthHeader{Sig: "0x" + strings.Repeat("ab", 65), Ts: 1, Ver: AuthVersion}
	req.SetAuth(a)
	require.Same(t, a, req.GetAuth())

	// AuthSetter interface satisfaction (compile-time check via assignment).
	var _ AuthSetter = req
}
