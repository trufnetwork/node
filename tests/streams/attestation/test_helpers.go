//go:build kwiltest

package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	kcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	testsetup "github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test data constants to avoid magic values
const (
	TestActionIDRequest = 10
	TestActionIDGet     = 20
	TestActionIDList    = 21
	TestStreamID        = "stream_attestation_test_00000000"
	TestDataProviderHex = "0x0000000000000000000000000000000000000b11"
	SignatureLength     = 65
	MinCanonicalLength  = 20
	DefaultBlockHeight  = 10
	InvalidTxID         = "0x0000000000000000000000000000000000000000000000000000000000000000"

	// ERC20 instance constants for balance operations
	testChain  = "sepolia"
	testEscrow = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	testERC20  = "0x2222222222222222222222222222222222222222"
)

var (
	TestDataProviderBytes = mustHexToBytes(TestDataProviderHex)
	erc20InstanceID       *types.UUID // Set during NewAttestationTestHelper
)

func mustHexToBytes(input string) []byte {
	normalized := strings.TrimPrefix(input, "0x")
	res, err := hex.DecodeString(normalized)
	if err != nil {
		panic(err)
	}
	return res
}

// TestAddresses holds reusable test addresses
type TestAddresses struct {
	Owner      *util.EthereumAddress
	Requester1 *util.EthereumAddress
	Requester2 *util.EthereumAddress
}

// NewTestAddresses creates standard test addresses
func NewTestAddresses() *TestAddresses {
	owner := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000b11")
	req1 := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000001111")
	req2 := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000002222")
	return &TestAddresses{
		Owner:      &owner,
		Requester1: &req1,
		Requester2: &req2,
	}
}

// AttestationTestHelper encapsulates common attestation test operations
type AttestationTestHelper struct {
	t         *testing.T
	ctx       context.Context
	platform  *kwilTesting.Platform
	leaderPub kcrypto.PublicKey // Leader public key for fee transfers
}

// NewAttestationTestHelper creates a new helper
func NewAttestationTestHelper(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) *AttestationTestHelper {
	// Generate a leader key for fee transfers (reused across all contexts)
	_, leaderPub, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err, "generate leader key")

	helper := &AttestationTestHelper{
		t:         t,
		ctx:       ctx,
		platform:  platform,
		leaderPub: leaderPub,
	}

	// Set up ERC20 instance for balance operations (once per test suite)
	if erc20InstanceID == nil {
		instanceID, err := erc20bridge.ForTestingForceSyncInstance(ctx, platform,
			testChain, testEscrow, testERC20, 18)
		require.NoError(t, err, "setup ERC20 instance")
		erc20InstanceID = instanceID
	}

	// Grant network_writer role to deployer by default
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	require.NoError(t, err, "create deployer address")
	helper.GrantNetworkWriterRole(deployer.Address())

	// Give deployer a large balance upfront to cover all attestation fees in tests
	// 1000 TRUF should be enough for 25 attestation requests (40 TRUF each)
	helper.GiveBalance(deployer.Address(), "1000000000000000000000") // 1000 TRUF in wei

	return helper
}

// NewEngineContext creates a standard engine context
func (h *AttestationTestHelper) NewEngineContext() *common.EngineContext {
	deployer, err := util.NewEthereumAddressFromBytes(h.platform.Deployer)
	require.NoError(h.t, err, "create deployer address")

	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: h.ctx,
			BlockContext: &common.BlockContext{
				Height:   1,
				Proposer: h.leaderPub, // Required for @leader_sender in fee transfers
			},
			Signer:        h.platform.Deployer,
			Caller:        deployer.Address(),
			TxID:          h.platform.Txid(),
			Authenticator: auth.EthPersonalSignAuth, // Required for balance operations
		},
	}
}

// NewNonExemptContext creates a context for a non-exempt user (without network_writer role)
// This is useful for testing fee validation with non-exempt users
func (h *AttestationTestHelper) NewNonExemptContext(userAddr *util.EthereumAddress) *common.EngineContext {
	// Give user balance to pay fees
	h.GiveBalance(userAddr.Address(), "1000000000000000000000") // 1000 TRUF

	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: h.ctx,
			BlockContext: &common.BlockContext{
				Height:   1,
				Proposer: h.leaderPub, // Required for @leader_sender in fee transfers
			},
			Signer:        userAddr.Bytes(),
			Caller:        userAddr.Address(),
			TxID:          h.platform.Txid(),
			Authenticator: auth.EthPersonalSignAuth, // Required for balance operations
		},
	}
}

// NewLeaderContext creates a context with leader authorization
func (h *AttestationTestHelper) NewLeaderContext(privateKey kcrypto.PrivateKey, publicKey kcrypto.PublicKey) *common.EngineContext {
	nodeSigner := auth.GetNodeSigner(privateKey)
	signer := nodeSigner.CompactID()
	caller, err := auth.GetNodeIdentifier(publicKey)
	require.NoError(h.t, err, "get node identifier")

	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: h.ctx,
			BlockContext: &common.BlockContext{
				Height:   DefaultBlockHeight,
				Proposer: publicKey,
			},
			Signer:        signer,
			Caller:        caller,
			TxID:          h.platform.Txid(),
			Authenticator: auth.Secp256k1Auth,
		},
	}
}

// NewRequesterContext creates a context for a specific requester
func (h *AttestationTestHelper) NewRequesterContext(requester *util.EthereumAddress) *common.EngineContext {
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: h.ctx,
			BlockContext: &common.BlockContext{
				Height:   1,
				Proposer: h.leaderPub, // Required for @leader_sender in fee transfers
			},
			Signer:        requester.Bytes(),
			Caller:        requester.Address(),
			TxID:          h.platform.Txid(),
			Authenticator: auth.EthPersonalSignAuth, // Required for balance operations
		},
	}
}

// CallAction executes an action and returns the result
func (h *AttestationTestHelper) CallAction(actionName string, args []any, resultFn func(*common.Row) error) *common.CallResult {
	engineCtx := h.NewEngineContext()
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", actionName, args, resultFn)
	require.NoError(h.t, err, "call action %s", actionName)
	return res
}

// AssertActionError checks that an action returned an error containing the expected message
func (h *AttestationTestHelper) AssertActionError(res *common.CallResult, expectedErrMsg string) {
	require.NotNil(h.t, res.Error, "expected error but got nil")
	require.Contains(h.t, res.Error.Error(), expectedErrMsg, "error message mismatch")
}

// RequestAttestation creates a test attestation request
func (h *AttestationTestHelper) RequestAttestation(actionName string, value int64) (requestTxID string, attestationHash []byte) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{value})
	require.NoError(h.t, err, "encode action args")

	res := h.CallAction("request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		nil, // max_fee = NULL (no limit)
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
		return nil
	})
	require.Nil(h.t, res.Error, "request_attestation should succeed")
	return requestTxID, attestationHash
}

// SignAttestation signs an attestation with a test validator key
func (h *AttestationTestHelper) SignAttestation(requestTxID string) {
	// Generate test validator key
	privateKey, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(h.t, err, "generate key")

	// Fetch canonical payload
	engineCtx := h.NewEngineContext()
	var canonical []byte
	err = h.platform.Engine.Execute(engineCtx, h.platform.DB,
		`SELECT result_canonical FROM attestations WHERE request_tx_id = $txid;`,
		map[string]any{"txid": requestTxID},
		func(row *common.Row) error {
			canonical = append([]byte(nil), row.Values[0].([]byte)...)
			return nil
		})
	require.NoError(h.t, err, "fetch canonical")
	require.NotEmpty(h.t, canonical, "canonical should exist")

	// Sign
	digest := ComputeDigest(canonical)
	secp256k1Key := privateKey.(*kcrypto.Secp256k1PrivateKey)
	signature, err := secp256k1Key.SignRaw(digest[:])
	require.NoError(h.t, err, "sign")

	// Submit signature with leader context
	leaderCtx := h.NewLeaderContext(privateKey, publicKey)
	res, err := h.platform.Engine.Call(leaderCtx, h.platform.DB, "", "sign_attestation",
		[]any{requestTxID, signature},
		func(row *common.Row) error { return nil })
	require.NoError(h.t, err, "sign_attestation call")
	require.Nil(h.t, res.Error, "sign_attestation should succeed")
}

// GrantNetworkWriterRole grants the network_writer role to a wallet
func (h *AttestationTestHelper) GrantNetworkWriterRole(walletAddr string) {
	err := testsetup.AddMemberToRoleBypass(h.ctx, h.platform, "system", "network_writer", walletAddr)
	require.NoError(h.t, err, "grant network_writer role to %s", walletAddr)
}

// CreateAttestationForRequester creates an attestation for a specific requester
func (h *AttestationTestHelper) CreateAttestationForRequester(actionName string, requester *util.EthereumAddress, value int64) {
	// Grant network_writer role to requester
	h.GrantNetworkWriterRole(requester.Address())

	// Give requester balance to pay for attestation fees (using proper kwil-db API)
	h.GiveBalance(requester.Address(), "1000000000000000000000") // 1000 TRUF in wei

	argsBytes, err := tn_utils.EncodeActionArgs([]any{value})
	require.NoError(h.t, err, "encode args")

	// Use requester context (now that balance injection works properly)
	requesterCtx := h.NewRequesterContext(requester)
	res, err := h.platform.Engine.Call(requesterCtx, h.platform.DB, "", "request_attestation",
		[]any{
			TestDataProviderHex,
			TestStreamID,
			actionName,
			argsBytes,
			false,
			nil, // max_fee = NULL (no limit)
		},
		func(row *common.Row) error { return nil })
	require.NoError(h.t, err, "request_attestation engine call")
	if res.Error != nil {
		require.NoError(h.t, res.Error, "request_attestation action failed: %v", res.Error)
	}
}

// CountRows counts the number of rows returned by an action
func (h *AttestationTestHelper) CountRows(actionName string, args []any) int {
	count := 0
	h.CallAction(actionName, args, func(row *common.Row) error {
		count++
		return nil
	})
	return count
}

// SetupTestAction creates a test action and adds it to the attestation allowlist
func (h *AttestationTestHelper) SetupTestAction(actionName string, actionID int) error {
	engineCtx := h.NewEngineContext()

	createAction := `
CREATE OR REPLACE ACTION ` + actionName + `(
	$value INT8
) PUBLIC VIEW RETURNS TABLE(event_time INT8, value NUMERIC(36,18)) {
	RETURN NEXT 1, ($value)::NUMERIC(36,18);
};`

	if err := h.platform.Engine.Execute(engineCtx, h.platform.DB, createAction, nil, nil); err != nil {
		return err
	}

	engineCtx = h.NewEngineContext()

	insertAllowlist := `
INSERT INTO attestation_actions(action_name, action_id)
VALUES ($action_name, $action_id)
ON CONFLICT (action_name) DO UPDATE SET action_id = EXCLUDED.action_id;`

	params := map[string]any{
		"action_name": actionName,
		"action_id":   actionID,
	}

	if err := h.platform.Engine.Execute(engineCtx, h.platform.DB, insertAllowlist, params, nil); err != nil {
		return err
	}

	return nil
}

// ComputeDigest computes sha256 digest of canonical payload (exported for test use)
func ComputeDigest(canonical []byte) [32]byte {
	return sha256.Sum256(canonical)
}

// GiveBalance credits TRUF balance to a wallet using ForTestingCreditBalance (proper kwil-db test API)
func (h *AttestationTestHelper) GiveBalance(walletAddr string, amountWei string) {
	app := &common.App{DB: h.platform.DB, Engine: h.platform.Engine}
	amount := types.MustParseDecimalExplicit(amountWei, 78, 0)

	err := erc20bridge.ForTestingCreditBalance(h.ctx, app, erc20InstanceID, walletAddr, amount)
	require.NoError(h.t, err, "credit balance %s wei to %s", amountWei, walletAddr)
}
