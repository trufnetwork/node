//go:build kwiltest

package order_book

import (
	"context"
	"fmt"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	balancePointCounter     int64 = 1000
	lastBalancePoint        *int64
	trufBalancePointCounter int64 = 2000
	lastTrufBalancePoint    *int64
	
	// Legacy names for compatibility with some files
	usdcPointCounter = &balancePointCounter
	trufPointCounter = &trufBalancePointCounter
)

// InjectDualBalance injects balance to BOTH bridges with proper chaining:
// 1. hoodi_tt (TRUF) for market creation fees
// 2. hoodi_tt2 (USDC) for market collateral/trading
func InjectDualBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	from := ensureNonZeroAddress(wallet)

	// 1. Inject TRUF balance
	trufBalancePointCounter++
	trufPoint := trufBalancePointCounter
	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTRUFChain,
		testTRUFEscrow,
		testTRUFERC20,
		from,
		wallet,
		amountStr,
		trufPoint,
		lastTrufBalancePoint,
	)
	if err != nil {
		return fmt.Errorf("failed to inject TRUF: %w", err)
	}
	lastTrufBalancePoint = &trufPoint

	// 2. Inject USDC balance
	balancePointCounter++
	usdcPoint := balancePointCounter
	err = testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testUSDCChain,
		testUSDCEscrow,
		testUSDCERC20,
		from,
		wallet,
		amountStr,
		usdcPoint,
		lastBalancePoint,
	)
	if err != nil {
		return fmt.Errorf("failed to inject USDC: %w", err)
	}
	lastBalancePoint = &usdcPoint

	return nil
}

// giveUSDCBalanceChained gives USDC only balance with proper linked-list chaining for ordered-sync
// Use this for vault/escrow funding where TRUF is not needed
func giveUSDCBalanceChained(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	balancePointCounter++
	usdcPoint := balancePointCounter

	from := ensureNonZeroAddress(wallet)

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testUSDCChain,
		testUSDCEscrow,
		testUSDCERC20,
		from,
		wallet,
		amountStr,
		usdcPoint,
		lastBalancePoint, // Chain to previous USDC point
	)

	if err != nil {
		return fmt.Errorf("failed to inject USDC: %w", err)
	}

	// Update USDC lastPoint for next call
	q := usdcPoint
	lastBalancePoint = &q

	return nil
}

// NewTestProposerPub generates a new proposer public key for testing.
func NewTestProposerPub(t require.TestingT) *crypto.Secp256k1PublicKey {
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	return pubGeneric.(*crypto.Secp256k1PublicKey)
}

// injectTestValidator adds a validator to the platform's VoteStore for testing.
// Returns the validator's public key (for use as BlockContext.Proposer) and
// its derived Ethereum address.
func injectTestValidator(t require.TestingT, platform *kwilTesting.Platform) (*crypto.Secp256k1PublicKey, string) {
	pub := NewTestProposerPub(t)
	platform.Validators.ForTestingAddValidator(pub.Bytes(), crypto.KeyTypeSecp256k1, 1)
	addr := fmt.Sprintf("0x%x", crypto.EthereumAddressFromPubKey(pub))
	return pub, addr
}

// ensureNonZeroAddress returns the provided address unless it's the zero address,
// in which case it returns a fallback non-zero address.
func ensureNonZeroAddress(addr string) string {
	zeroAddr := "0x0000000000000000000000000000000000000000"
	if addr == zeroAddr {
		return "0x0000000000000000000000000000000000000001"
	}
	return addr
}

// encodeQueryComponentsForTests encodes query components using ABI format for testing
// This is a shared helper for all order_book tests
// If argsBytes is provided, it uses those args; otherwise creates default args for get_record
func encodeQueryComponentsForTests(dataProvider, streamID, actionID string, argsBytes []byte) ([]byte, error) {
	// If args not provided, encode default args for get_record action
	if argsBytes == nil {
		// For get_record action, args are: (data_provider, stream_id, from, to, frozen_at, use_cache)
		var err error
		argsBytes, err = tn_utils.EncodeActionArgs([]any{
			dataProvider,       // data_provider (TEXT)
			streamID,           // stream_id (TEXT)
			int64(0),           // from (INT8)
			int64(999999999),   // to (INT8) - far future
			nil,                // frozen_at (INT8, nullable)
			false,              // use_cache (BOOL)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to encode action args: %w", err)
		}
	}

	// Now encode the full query components as ABI
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create address type: %w", err)
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes32 type: %w", err)
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create string type: %w", err)
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes type: %w", err)
	}

	abiArgs := gethAbi.Arguments{
		{Type: addressType, Name: "data_provider"},
		{Type: bytes32Type, Name: "stream_id"},
		{Type: stringType, Name: "action_id"},
		{Type: bytesType, Name: "args"},
	}

	// Convert data provider to address
	dpAddr := gethCommon.HexToAddress(dataProvider)

	// Convert stream ID to bytes32 (pad with zeros on the right)
	var streamIDBytes [32]byte
	copy(streamIDBytes[:], []byte(streamID))

	// Pack the ABI
	encoded, err := abiArgs.Pack(dpAddr, streamIDBytes, actionID, argsBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to pack ABI: %w", err)
	}

	return encoded, nil
}

// callSettleMarket is a shared helper to call the settle_market action.
// settle_market($query_id INT) determines the winning outcome from the attestation.
// The caller must set BlockContext.Timestamp >= market's settle_time.
func callSettleMarket(ctx context.Context, platform *kwilTesting.Platform, caller *util.EthereumAddress, queryID int, settlementTimestamp int64) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: settlementTimestamp,
		},
		Signer:        caller.Bytes(),
		Caller:        caller.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market", []any{int64(queryID)}, nil)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}
