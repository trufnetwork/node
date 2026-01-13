//go:build kwiltest

package tests

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// Hoodi constants for migration-registered instance
const (
	// Real Hoodi chain and addresses (from migrations)
	MigrationHoodiChain  = "hoodi"
	MigrationHoodiEscrow = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	MigrationHoodiAlias  = "hoodi_tt" // Alias from migration 000-extension.sql
)

// TestHoodiGetWithdrawalProofAction tests the public hoodi_tt_get_withdrawal_proof action.
// This test uses the migration-registered hoodi_tt instance (not a test-specific instance).
//
// Test flow:
// 1) Seed data into migration-registered hoodi_bridge instance
// 2) User deposits and withdraws
// 3) Finalize and confirm epoch
// 4) Call public action hoodi_tt_get_withdrawal_proof
// 5) Verify returned merkle proof structure
//
// This validates the full end-to-end public API that users will call.
func TestHoodiGetWithdrawalProofAction(t *testing.T) {
	seedAndRun(t, "hoodi_tt_get_withdrawal_proof_action", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// The hoodi_tt instance is already created by migrations (000-extension.sql)
		// We just need to seed it with test data

		testUser := "0xf9820f9143699cac6f662b19a4b29e13c9393783"
		testAmount := "100000000000000000000" // 100 tokens

		// The hoodi_tt instance already exists from migrations
		// Need to sync it to database AND load into singleton
		_, err := erc20shim.ForTestingForceSyncInstance(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001", // Fake ERC20 for testing
			18,
		)
		require.NoError(t, err, "failed to sync hoodi_tt instance")

		// Load DB instances into singleton
		err = erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to initialize ERC20 extension")

		// Inject deposit to give user balance
		err = testerc20.InjectERC20Transfer(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001", // Fake ERC20
			testUser,
			testUser,
			testAmount,
			10,
			nil,
		)
		require.NoError(t, err, "failed to inject deposit")

		// User requests withdrawal via bridge action
		engineCtx := engCtx(ctx, platform, testUser, 2, false)
		amtDec, err := types.ParseDecimalExplicit(testAmount, 78, 0)
		require.NoError(t, err)

		// Call bridge() on the migration-registered instance
		r, err := platform.Engine.Call(engineCtx, platform.DB, MigrationHoodiAlias, "bridge",
			[]any{testUser, amtDec},
			func(row *common.Row) error { return nil })
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Finalize and confirm epoch
		var bh [32]byte
		copy(bh[:], []byte("test_block_hash_0000000000000000"))
		err = erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			11,
			bh,
		)
		require.NoError(t, err, "failed to finalize epoch")

		// Add validator signature for the epoch (required for withdrawal proofs)
		err = erc20shim.ForTestingAddValidatorSignatureToEpoch(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
		)
		require.NoError(t, err, "failed to add validator signature")

		// Call the PUBLIC ACTION hoodi_tt_get_withdrawal_proof
		// This is the action users will call in production
		engineCtx = engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 3, false)

		var proofRows int
		var chain, chainID, contract, recipient string
		var amount *types.Decimal
		var blockHash, root []byte
		var proofs [][]byte
		var signatures [][]byte

		r, err = platform.Engine.Call(engineCtx, platform.DB, "", "hoodi_tt_get_withdrawal_proof",
			[]any{testUser}, // Just wallet address parameter
			func(row *common.Row) error {
				proofRows++

				// Verify all 10 columns are present (including signatures)
				require.Equal(t, 10, len(row.Values), "should return 10 columns including signatures")

				// Extract values
				chain = row.Values[0].(string)
				chainID = row.Values[1].(string)
				contract = row.Values[2].(string)
				// created_at is row.Values[3] (int64)
				recipient = row.Values[4].(string)
				amount = row.Values[5].(*types.Decimal)
				blockHash = row.Values[6].([]byte)
				root = row.Values[7].([]byte)

				// Extract proofs (BYTEA[] type)
				var ok bool
				proofs, ok = row.Values[8].([][]byte)
				if !ok {
					// Fallback to []any
					proofsRaw, ok2 := row.Values[8].([]any)
					require.True(t, ok2, "proofs should be array (got type %T)", row.Values[8])
					proofs = make([][]byte, len(proofsRaw))
					for i, p := range proofsRaw {
						pb, ok3 := p.([]byte)
						require.True(t, ok3, "proof element %d should be []byte (got type %T)", i, p)
						proofs[i] = pb
					}
				}

				// Extract signatures (BYTEA[] type) - NEW!
				signatures, ok = row.Values[9].([][]byte)
				if !ok {
					// Fallback to []any
					signaturesRaw, ok2 := row.Values[9].([]any)
					require.True(t, ok2, "signatures should be array (got type %T)", row.Values[9])
					signatures = make([][]byte, len(signaturesRaw))
					for i, s := range signaturesRaw {
						sb, ok3 := s.([]byte)
						require.True(t, ok3, "signature element %d should be []byte (got type %T)", i, s)
						signatures[i] = sb
					}
				}

				return nil
			})
		require.NoError(t, err, "action call failed")
		if r != nil && r.Error != nil {
			t.Logf("Action error: %v", r.Error)
			return r.Error
		}

		// Verify we got exactly one proof
		require.Equal(t, 1, proofRows, "should return exactly one withdrawal proof")

		// Log what we got
		t.Logf("Returned data: chain=%s, chainID=%s, contract=%s, recipient=%s", chain, chainID, contract, recipient)
		t.Logf("Amount: %s", amount.String())
		t.Logf("BlockHash length: %d, Root length: %d, Proofs length: %d, Signatures length: %d", len(blockHash), len(root), len(proofs), len(signatures))

		// Verify returned data structure
		require.Equal(t, MigrationHoodiChain, chain, "chain should match")
		require.NotEmpty(t, chainID, "chain_id should not be empty")
		require.True(t, strings.EqualFold(MigrationHoodiEscrow, contract), "contract should match escrow")
		require.True(t, strings.EqualFold(testUser, recipient), "recipient should match user")
		require.Equal(t, testAmount, amount.String(), "amount should match withdrawal")
		require.NotEmpty(t, blockHash, "block_hash should not be empty")
		require.NotEmpty(t, root, "merkle root should not be empty")

		// Merkle proofs may be empty for single-element tree (no siblings needed)
		// This is actually correct! A tree with one element has no proof elements.
		t.Logf("Merkle proof elements: %d (expected 0 for single reward)", len(proofs))

		// Verify block hash matches
		require.Equal(t, bh[:], blockHash, "block hash should match finalized epoch")

		// Verify merkle proof structure
		require.Equal(t, 32, len(root), "merkle root should be 32 bytes")

		// Note: proofs array may be empty for single-element merkle tree
		// This is correct! No siblings = no proof elements needed
		for i, proof := range proofs {
			require.Equal(t, 32, len(proof), "proof element %d should be 32 bytes", i)
			t.Logf("Proof %d: 0x%s", i, hex.EncodeToString(proof))
		}
		t.Logf("Merkle root: 0x%s", hex.EncodeToString(root))
		t.Logf("Total proof elements: %d", len(proofs))

		// Verify validator signatures structure (NEW!)
		require.NotEmpty(t, signatures, "should have at least one validator signature")
		for i, sig := range signatures {
			require.Equal(t, 65, len(sig), "signature %d should be 65 bytes (r||s||v)", i)
			t.Logf("Signature %d length: %d bytes", i, len(sig))
		}
		t.Logf("Total validator signatures: %d", len(signatures))

		t.Logf("✅ Public action hoodi_tt_get_withdrawal_proof works correctly")
		t.Logf("   Chain: %s", chain)
		t.Logf("   ChainID: %s", chainID)
		t.Logf("   Contract: %s", contract)
		t.Logf("   Recipient: %s", recipient)
		t.Logf("   Amount: %s wei", amount.String())
		t.Logf("   Validator Signatures: %d", len(signatures))

		return nil
	})
}

// TestHoodiGetWithdrawalProofNoPending tests that pending epochs are not returned.
func TestHoodiGetWithdrawalProofNoPending(t *testing.T) {
	seedAndRun(t, "hoodi_tt_get_withdrawal_proof_no_pending", func(ctx context.Context, platform *kwilTesting.Platform) error {
		testUser := "0xabc0000000000000000000000000000000000001"
		testAmount := "50000000000000000000"

		// Sync the migration-created instance
		_, err := erc20shim.ForTestingForceSyncInstance(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001",
			18,
		)
		require.NoError(t, err)

		// Load DB instances into singleton
		err = erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = testerc20.InjectERC20Transfer(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001",
			testUser,
			testUser,
			testAmount,
			10,
			nil,
		)
		require.NoError(t, err)

		// User withdraws
		engineCtx := engCtx(ctx, platform, testUser, 2, false)
		amtDec, err := types.ParseDecimalExplicit(testAmount, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtx, platform.DB, MigrationHoodiAlias, "bridge",
			[]any{testUser, amtDec},
			func(row *common.Row) error { return nil })
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// DO NOT finalize epoch - leave it pending

		// Call public action
		engineCtx = engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 3, false)
		var proofRows int
		r, err = platform.Engine.Call(engineCtx, platform.DB, "", "hoodi_tt_get_withdrawal_proof",
			[]any{testUser},
			func(row *common.Row) error {
				proofRows++
				return nil
			})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Should return 0 rows (pending epoch not included)
		require.Equal(t, 0, proofRows, "should return no proofs for pending epochs")

		t.Logf("✅ Pending epochs correctly excluded from withdrawal proofs")

		return nil
	})
}

// TestHoodiGetWithdrawalProofMultipleUsers tests multiple users with withdrawals in one epoch.
// Note: Testing multiple EPOCHS requires complex timing logic not yet in test infrastructure.
// This test validates that multiple users can each retrieve their withdrawal proofs correctly.
func TestHoodiGetWithdrawalProofMultipleUsers(t *testing.T) {
	seedAndRun(t, "hoodi_tt_get_withdrawal_proof_multiple_users", func(ctx context.Context, platform *kwilTesting.Platform) error {
		userA := "0xabc0000000000000000000000000000000000001"
		userB := "0xabc0000000000000000000000000000000000002"
		amount100 := "100000000000000000000"
		amount50 := "50000000000000000000"
		amount25 := "25000000000000000000"

		// Sync the migration-created instance
		_, err := erc20shim.ForTestingForceSyncInstance(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001",
			18,
		)
		require.NoError(t, err)

		// Load DB instances into singleton
		err = erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give User A 100 tokens
		err = testerc20.InjectERC20Transfer(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001",
			userA,
			userA,
			amount100,
			10,
			nil,
		)
		require.NoError(t, err)

		// Give User B 50 tokens (chained)
		prevPoint := int64(10)
		err = testerc20.InjectERC20Transfer(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			"0x0000000000000000000000000000000000000001",
			userB,
			userB,
			amount50,
			11,
			&prevPoint,
		)
		require.NoError(t, err)

		// User A withdraws 50 tokens
		engineCtx := engCtx(ctx, platform, userA, 2, false)
		amt50Dec, err := types.ParseDecimalExplicit(amount50, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtx, platform.DB, MigrationHoodiAlias, "bridge",
			[]any{userA, amt50Dec},
			func(row *common.Row) error { return nil })
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// User B withdraws 25 tokens
		engineCtx = engCtx(ctx, platform, userB, 3, false)
		amt25Dec, err := types.ParseDecimalExplicit(amount25, 78, 0)
		require.NoError(t, err)

		r, err = platform.Engine.Call(engineCtx, platform.DB, MigrationHoodiAlias, "bridge",
			[]any{userB, amt25Dec},
			func(row *common.Row) error { return nil })
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Finalize epoch with both withdrawals
		var bh [32]byte
		copy(bh[:], []byte("test_block_hash_0000000000000000"))
		err = erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
			12,
			bh,
		)
		require.NoError(t, err)

		// Add validator signature for the epoch
		err = erc20shim.ForTestingAddValidatorSignatureToEpoch(
			ctx, platform,
			MigrationHoodiChain,
			MigrationHoodiEscrow,
		)
		require.NoError(t, err)

		// Call public action for User A
		engineCtx = engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 4, false)
		var proofRowsA int
		var amountA string

		r, err = platform.Engine.Call(engineCtx, platform.DB, "", "hoodi_tt_get_withdrawal_proof",
			[]any{userA},
			func(row *common.Row) error {
				proofRowsA++
				amountA = row.Values[5].(*types.Decimal).String()
				return nil
			})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, 1, proofRowsA, "User A should have 1 proof")
		require.Equal(t, amount50, amountA, "User A amount should be 50 tokens")

		// Call public action for User B
		var proofRowsB int
		var amountB string

		r, err = platform.Engine.Call(engineCtx, platform.DB, "", "hoodi_tt_get_withdrawal_proof",
			[]any{userB},
			func(row *common.Row) error {
				proofRowsB++
				amountB = row.Values[5].(*types.Decimal).String()
				return nil
			})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, 1, proofRowsB, "User B should have 1 proof")
		require.Equal(t, amount25, amountB, "User B amount should be 25 tokens")

		t.Logf("✅ Multiple users' withdrawals correctly returned")
		t.Logf("   User A: %d proof with amount %s wei", proofRowsA, amountA)
		t.Logf("   User B: %d proof with amount %s wei", proofRowsB, amountB)

		return nil
	})
}
