package tn_attestation

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/app/key"
	appconf "github.com/trufnetwork/kwil-db/app/node/conf"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/node/extensions/leaderwatch"
)

// InitializeExtension registers the tn_attestation extension.
// This includes:
// - Registering the queue_for_signing() precompile
// - Registering leader watch callbacks for the signing workflow
func InitializeExtension() {
	// Register the precompile for queue_for_signing() method
	if err := registerPrecompile(); err != nil {
		panic(fmt.Sprintf("failed to register %s precompile: %v", ExtensionName, err))
	}

	// Register engine ready hook
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}

	// Register leader watch callbacks for signing workflow lifecycle
	if err := leaderwatch.Register(ExtensionName, leaderwatch.Callbacks{
		OnAcquire:  onLeaderAcquire,
		OnLose:     onLeaderLose,
		OnEndBlock: onLeaderEndBlock,
	}); err != nil {
		panic(fmt.Sprintf("failed to register %s leader watcher: %v", ExtensionName, err))
	}
}

// engineReadyHook is called when the engine is ready.
// It initializes the validator signer with the node's private key.
func engineReadyHook(ctx context.Context, app *common.App) error {
	if app == nil || app.Service == nil {
		return nil
	}

	ext := getExtension()
	ext.setService(app.Service)
	ext.setApp(app)
	ext.applyConfig(app.Service)

	logger := ext.Logger()

	// Load the validator's private key from the node key file
	rootDir := appconf.RootDir()
	if rootDir == "" {
		logger.Warn("tn_attestation extension ready without validator signer (root dir is empty)",
			"queue_size", GetAttestationQueue().Len())
		return nil
	}

	keyPath := config.NodeKeyFilePath(rootDir)
	privateKey, err := key.LoadNodeKey(keyPath)
	if err != nil {
		logger.Warn("tn_attestation extension ready without validator signer (failed to load node key)",
			"queue_size", GetAttestationQueue().Len(),
			"key_path", keyPath,
			"error", err)
		return nil
	}

	// Initialize the validator signer with the loaded private key
	if err := InitializeValidatorSigner(privateKey); err != nil {
		logger.Warn("tn_attestation extension ready without validator signer (unsupported node key)",
			"error", err,
			"queue_size", GetAttestationQueue().Len())
		// Leave the extension running so the node can operate without signing support.
		return nil
	}

	if ext.NodeSigner() == nil {
		ext.setNodeSigner(auth.GetNodeSigner(privateKey))
	}

	// Log the validator address for debugging
	signer := GetValidatorSigner()
	if signer != nil {
		logger.Info("tn_attestation extension ready",
			"queue_size", GetAttestationQueue().Len(),
			"validator_address", signer.Address())
	}

	ext.ensureBroadcaster(app.Service)

	return nil
}

// onLeaderAcquire is called when the node becomes the leader.
// TODO: Start the signing worker here
func onLeaderAcquire(ctx context.Context, app *common.App, block *common.BlockContext) {
	if app == nil || app.Service == nil {
		return
	}

	ext := getExtension()
	ext.setService(app.Service)
	ext.setApp(app)
	if block != nil {
		ext.setLeader(true, block.Height)
	}

	logger := ext.Logger()
	logger.Info("tn_attestation: acquired leadership")

	queue := GetAttestationQueue()
	if n := queue.Len(); n > 0 {
		logger.Debug("tn_attestation: clearing residual attestation queue after leader acquisition", "dropped", n)
	}
	queue.Clear()

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

	ext := getExtension()
	ext.setService(app.Service)
	ext.setApp(app)
	if block != nil {
		ext.setLeader(false, block.Height)
	}

	logger := ext.Logger()
	logger.Info("tn_attestation: lost leadership")

	queue := GetAttestationQueue()
	if n := queue.Len(); n > 0 {
		logger.Debug("tn_attestation: clearing attestation queue on leader loss", "dropped", n)
	}
	queue.Clear()

	// TODO: Implement signing worker shutdown
	// Reference implementation:
	//   ext := GetExtension()
	//   ext.stopSigningWorker()
}

// onLeaderEndBlock is called on every EndBlock when the node is the leader.
// It processes queued attestation hashes and, in later phases, also performs
// periodic scans to recover any missed notifications.
func onLeaderEndBlock(ctx context.Context, app *common.App, block *common.BlockContext) {
	if app == nil || app.Service == nil {
		return
	}

	ext := getExtension()
	ext.setService(app.Service)
	ext.setApp(app)

	if !ext.Leader() {
		return
	}

	// Dequeue all pending attestation hashes to prevent unbounded growth
	queue := GetAttestationQueue()
	hashes := queue.DequeueAll()

	if len(hashes) > 0 {
		logger := ext.Logger()
		logger.Info("tn_attestation: processing queued attestations",
			"count", len(hashes),
			"block_height", block.Height)
		ext.processAttestationHashes(ctx, hashes)
	}

	if block != nil && ext.shouldPerformScan(block.Height) {
		logger := ext.Logger()
		pending, err := ext.fetchPendingHashes(ctx, int(ext.ScanBatchLimit()))
		if err != nil {
			logger.Error("tn_attestation: fallback scan failed", "error", err)
		} else if len(pending) > 0 {
			logger.Info("tn_attestation: fallback scan found unsigned attestations",
				"count", len(pending),
				"block_height", block.Height)
			ext.processAttestationHashes(ctx, pending)
		}
	}
}
