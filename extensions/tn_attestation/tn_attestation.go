package tn_attestation

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/node/extensions/leaderwatch"
)

// InitializeExtension registers the tn_attestation extension.
// This includes:
// - Registering the queue_for_signing() precompile
// - Registering leader watch callbacks for signing worker // TODO: WIP
func InitializeExtension() {
	// Register the precompile for queue_for_signing() method
	if err := registerPrecompile(); err != nil {
		panic(fmt.Sprintf("failed to register %s precompile: %v", ExtensionName, err))
	}

	// Register engine ready hook
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}

	// Register leader watch callbacks (for Issue 6 - leader signing worker)
	if err := leaderwatch.Register(ExtensionName, leaderwatch.Callbacks{
		OnAcquire:  onLeaderAcquire,
		OnLose:     onLeaderLose,
		OnEndBlock: onLeaderEndBlock,
	}); err != nil {
		panic(fmt.Sprintf("failed to register %s leader watcher: %v", ExtensionName, err))
	}
}

// engineReadyHook is called when the engine is ready.
func engineReadyHook(ctx context.Context, app *common.App) error {
	if app == nil || app.Service == nil {
		return nil
	}

	logger := app.Service.Logger.New(ExtensionName)
	logger.Info("tn_attestation extension ready",
		"queue_size", GetAttestationQueue().Len())

	return nil
}

// onLeaderAcquire is called when the node becomes the leader.
// TODO: Start the signing worker here
func onLeaderAcquire(ctx context.Context, app *common.App, block *common.BlockContext) {
	if app == nil || app.Service == nil {
		return
	}

	logger := app.Service.Logger.New(ExtensionName)
	logger.Info("tn_attestation: acquired leadership")

	// TODO: Implement signing worker startup
	// Reference implementation:
	//   ext := GetExtension()
	//   ext.startSigningWorker(ctx, app)
}

// onLeaderLose is called when the node loses leadership.
// TODO: Stop the signing worker here
func onLeaderLose(ctx context.Context, app *common.App, block *common.BlockContext) {
	if app == nil || app.Service == nil {
		return
	}

	logger := app.Service.Logger.New(ExtensionName)
	logger.Info("tn_attestation: lost leadership")

	// TODO: Implement signing worker shutdown
	// Reference implementation:
	//   ext := GetExtension()
	//   ext.stopSigningWorker()
}

// onLeaderEndBlock is called on every EndBlock when the node is the leader.
// Currently dequeues and logs hashes to prevent unbounded memory growth.
// TODO: Implement actual signing and submission of attestations.
func onLeaderEndBlock(ctx context.Context, app *common.App, block *common.BlockContext) {
	if app == nil || app.Service == nil {
		return
	}

	// Dequeue all pending attestation hashes to prevent unbounded growth
	queue := GetAttestationQueue()
	hashes := queue.DequeueAll()

	// If there are hashes, log them (signing implementation pending in Issue 6)
	if len(hashes) > 0 {
		logger := app.Service.Logger.New(ExtensionName)
		logger.Info("tn_attestation: dequeued attestations for signing",
			"count", len(hashes),
			"block_height", block.Height,
			"note", "signing implementation pending (Issue 6)")

		// TODO: Implement actual signing and submission
		// Reference implementation:
		//   ext := GetExtension()
		//   for _, hash := range hashes {
		//       signature, err := ext.signAttestation(ctx, app, hash)
		//       if err != nil {
		//           logger.Error("failed to sign attestation", "hash", hash, "error", err)
		//           continue
		//       }
		//       if err := ext.submitSignature(ctx, app, hash, signature); err != nil {
		//           logger.Error("failed to submit signature", "hash", hash, "error", err)
		//       }
		//   }
	}
}
