// Package tn_attestation implements the attestation signing workflow for TN validators.
//
// When a user requests an attestation via request_attestation (SQL), the extension:
// 1. Queues the hash via queue_for_signing precompile (non-deterministic, leader-only)
// 2. Processes queued hashes on leader's EndBlock
// 3. Signs canonical payloads using the validator's secp256k1 key
// 4. Broadcasts sign_attestation transactions back to consensus
//
// Key components:
// - ValidatorSigner: Thread-safe secp256k1 signing with EVM compatibility
// - CanonicalPayload: Structured representation of the 8-field attestation format
// - Leader callbacks: OnAcquire, OnLose, OnEndBlock lifecycle hooks
//
// Initialize the extension by calling InitializeExtension() during node startup.
package tn_attestation
