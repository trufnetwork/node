package tn_attestation

import (
	"bytes"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// registerPrecompile registers the tn_attestation precompile with queue_for_signing() and verify_payload() methods.
// This is called from InitializeExtension().
func registerPrecompile() error {
	return precompiles.RegisterPrecompile(ExtensionName, precompiles.Precompile{
		// No cache needed: queue_for_signing affects leader's in-memory state only,
		// verify_payload is deterministic and doesn't need caching
		Cache: nil,
		Methods: []precompiles.Method{
			{
				Name: "queue_for_signing",
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("attestation_hash", types.TextType, false),
				},
				AccessModifiers: []precompiles.Modifier{precompiles.SYSTEM},
				Handler: func(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
					// Validate inputs length
					if len(inputs) == 0 {
						return fmt.Errorf("expected 1 input parameter, got 0")
					}

					// Safe type assertion with comma-ok
					attestationHash, ok := inputs[0].(string)
					if !ok {
						return fmt.Errorf("attestation_hash must be a string, got %T", inputs[0])
					}

					// Validate attestation hash is not empty
					if attestationHash == "" {
						return fmt.Errorf("attestation_hash cannot be empty")
					}

					// Check if the current node is the leader
					// We check by comparing the proposer's public key with our own identity
					// Treat missing context as non-leader to preserve determinism
					isLeader := false
					if ctx != nil &&
						ctx.TxContext != nil &&
						ctx.TxContext.BlockContext != nil &&
						ctx.TxContext.BlockContext.Proposer != nil &&
						app != nil &&
						app.Service != nil &&
						app.Service.Identity != nil {
						proposerBytes := ctx.TxContext.BlockContext.Proposer.Bytes()
						isLeader = bytes.Equal(proposerBytes, app.Service.Identity)
					}

					// Only queue if we are the leader
					// For non-leaders, this is a no-op to maintain determinism
					if isLeader {
						queue := GetAttestationQueue()
						queue.Enqueue(attestationHash)

						if app != nil && app.Service != nil && app.Service.Logger != nil {
							app.Service.Logger.Debug("Queued attestation for signing",
								"hash", attestationHash,
								"queue_size", queue.Len())
						}
					}

					// Always return nil (no return value) for all validators
					// This maintains determinism while only affecting leader's in-memory state
					return nil
				},
			},
			{
				Name: "verify_payload",
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("canonical_bytes", types.ByteaType, false),
					precompiles.NewPrecompileValue("signature", types.ByteaType, false),
					precompiles.NewPrecompileValue("validator_pubkey", types.ByteaType, false),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("valid", types.BoolType, false),
					},
				},
				AccessModifiers: []precompiles.Modifier{precompiles.SYSTEM},
				Handler: func(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
					// Validate inputs length
					if len(inputs) != 3 {
						return fmt.Errorf("expected 3 input parameters, got %d", len(inputs))
					}

					// Extract and validate canonical bytes
					canonicalBytes, ok := inputs[0].([]byte)
					if !ok {
						return fmt.Errorf("canonical_bytes must be []byte, got %T", inputs[0])
					}

					// Extract and validate signature
					signature, ok := inputs[1].([]byte)
					if !ok {
						return fmt.Errorf("signature must be []byte, got %T", inputs[1])
					}

					// Extract and validate validator public key
					validatorPubkey, ok := inputs[2].([]byte)
					if !ok {
						return fmt.Errorf("validator_pubkey must be []byte, got %T", inputs[2])
					}

					// Perform verification
					valid, err := VerifyAttestationPayload(canonicalBytes, signature, validatorPubkey)
					if err != nil {
						// Return false on verification errors rather than propagating error
						// This allows SQL to handle verification failures gracefully
						return resultFn([]any{false})
					}

					return resultFn([]any{valid})
				},
			},
			{
				Name: "compute_attestation_hash",
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("canonical_bytes", types.ByteaType, false),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("attestation_hash", types.ByteaType, false),
					},
				},
				AccessModifiers: []precompiles.Modifier{precompiles.SYSTEM},
				Handler: func(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
					if len(inputs) != 1 {
						return fmt.Errorf("expected 1 input parameter, got %d", len(inputs))
					}

					canonicalBytes, ok := inputs[0].([]byte)
					if !ok {
						return fmt.Errorf("canonical_bytes must be []byte, got %T", inputs[0])
					}

					payload, err := ParseCanonicalPayload(canonicalBytes)
					if err != nil {
						return fmt.Errorf("parse canonical payload: %w", err)
					}

					hash := computeAttestationHash(payload)
					hashBytes := append([]byte(nil), hash[:]...)
					return resultFn([]any{hashBytes})
				},
			},
		},
	})
}
