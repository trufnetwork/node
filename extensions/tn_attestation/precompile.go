package tn_attestation

import (
	"bytes"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// registerPrecompile registers the tn_attestation precompile with queue_for_signing() method.
// This is called from InitializeExtension().
func registerPrecompile() error {
	return precompiles.RegisterPrecompile(ExtensionName, precompiles.Precompile{
		// No cache needed: queue_for_signing affects leader's in-memory state only
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
		},
	})
}
