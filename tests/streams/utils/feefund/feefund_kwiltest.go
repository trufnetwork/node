//go:build kwiltest

// Package feefund credits TRUF balance to test wallets so they can pay the
// universal write fees enforced by the embedded migrations
// (001-common-actions / 003-primitive-insertion / 004-composed-taxonomy /
// 024-attestation-actions). It exists in its own package to avoid an import
// cycle between `setup` (which calls it from CreateStream/CreateStreams) and
// `procedure` (which calls it from SetTaxonomy/insert helpers).
package feefund

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

// Per-action fee constants in wei (18-decimal TRUF). Each must mirror the
// hard-coded fee in its corresponding migration so that test funding stays
// in sync with on-chain charges. If a migration changes its fee, update here.
const (
	// WriteFeeWei mirrors the flat per-transaction write fee charged by
	// 003-primitive-insertion.sql `insert_records` and
	// 004-composed-taxonomy.sql `insert_taxonomy` — independent of batch
	// size (1 TRUF regardless of records/children).
	WriteFeeWei = "1000000000000000000" // 1 TRUF

	// StreamCreationFeeWei mirrors the per-stream fee charged by
	// 001-common-actions.sql `create_streams`. Per issue #3971 the fee
	// scales with array_length($stream_ids): a batch of N streams costs
	// N × StreamCreationFeeWei. No role exemption — every caller pays.
	StreamCreationFeeWei = "100000000000000000000" // 100 TRUF per stream

	// AttestationFeeWei mirrors the flat fee in 024-attestation-actions.sql.
	AttestationFeeWei = "40000000000000000000" // 40 TRUF
)

const (
	// Test bridge constants — must match the `hoodi_tt` USE block in
	// internal/migrations/erc20-bridge/000-extension.sql, since the dev
	// fee-collection actions (001/003/004/024) call hoodi_tt.balance() and
	// hoodi_tt.transfer() directly. (Mainnet override calls eth_truf via the
	// matching *.prod.sql files.)
	testFundingChain  = "hoodi"
	testFundingEscrow = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testFundingERC20  = "0x2222222222222222222222222222222222222222"
)

// EnsureWalletFunded credits the given wallet with `amountWei` (18-decimal TRUF)
// directly into the test bridge's balances table, bypassing ordered-sync. Use
// this immediately before calling a fee-charging action so the wallet has a
// non-zero balance when the action's bridge.balance() check runs.
//
// Idempotent: safe to call multiple times. Each call adds the requested amount
// (i.e. acts as a top-up, not a set-to). The bridge instance and singleton are
// (re)initialized as needed — no separate test setup required.
func EnsureWalletFunded(ctx context.Context, platform *kwilTesting.Platform, wallet, amountWei string) error {
	app := &common.App{DB: platform.DB, Engine: platform.Engine}

	id, err := erc20shim.ForTestingForceSyncInstance(ctx, platform, testFundingChain, testFundingEscrow, testFundingERC20, 18)
	if err != nil {
		return errors.Wrap(err, "force sync test bridge instance")
	}

	// Load the synced instance into the singleton so the eth_truf/sepolia_bridge
	// precompile (`balance`/`transfer`) can resolve it during action execution.
	if err := erc20shim.ForTestingInitializeExtension(ctx, platform); err != nil {
		return errors.Wrap(err, "initialize test bridge singleton")
	}

	amt, err := types.ParseDecimalExplicit(amountWei, 78, 0)
	if err != nil {
		return errors.Wrapf(err, "parse amount %s", amountWei)
	}

	if err := erc20shim.ForTestingCreditBalance(ctx, app, id, strings.ToLower(wallet), amt); err != nil {
		return errors.Wrapf(err, "credit %s TRUF to %s", amountWei, wallet)
	}
	return nil
}
