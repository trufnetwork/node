package tn_attestation

import (
	"bytes"
	"fmt"

	kcrypto "github.com/trufnetwork/kwil-db/core/crypto"
)

// VerifyAttestationPayload validates that the signature proves the canonical payload
// is authentic, unmodified, and originated from the claimed validator.
//
// Verification steps:
// 1. Parse canonical payload structure
// 2. Compute signing digest (sha256 of canonical bytes)
// 3. Recover public key from signature
// 4. Verify recovered key matches validator public key
//
// Called from get_signed_attestation SQL action before returning payloads to users.
func VerifyAttestationPayload(canonicalBytes, signature, validatorPubkey []byte) (bool, error) {
	// Parse canonical payload to ensure structure is valid
	payload, err := ParseCanonicalPayload(canonicalBytes)
	if err != nil {
		return false, fmt.Errorf("parse canonical payload: %w", err)
	}

	// Compute signing digest (sha256 of canonical bytes)
	digest := payload.SigningDigest()

	// Normalise recovery ID to compact {0,1} form expected by RecoverSecp256k1KeyFromSigHash.
	if len(signature) != 65 {
		return false, fmt.Errorf("signature must be 65 bytes, got %d", len(signature))
	}
	normSignature := append([]byte(nil), signature...)
	recoveryID, err := toCompactRecoveryID(normSignature[64])
	if err != nil {
		return false, fmt.Errorf("normalise recovery id: %w", err)
	}
	normSignature[64] = recoveryID

	// Recover signer's public key from signature
	recoveredPubKey, err := kcrypto.RecoverSecp256k1KeyFromSigHash(digest[:], normSignature)
	if err != nil {
		return false, fmt.Errorf("recover public key from signature: %w", err)
	}

	// Extract compact public key representation for comparison
	recoveredCompact := recoveredPubKey.Bytes()

	// Verify recovered public key matches the claimed validator public key
	if !bytes.Equal(recoveredCompact, validatorPubkey) {
		return false, fmt.Errorf("signature verification failed: recovered pubkey %x does not match validator pubkey %x",
			recoveredCompact, validatorPubkey)
	}

	return true, nil
}

func toCompactRecoveryID(v byte) (byte, error) {
	switch {
	case v <= 1:
		return v, nil
	case v >= 27 && v <= 34:
		return (v - 27) & 1, nil
	default:
		return 0, fmt.Errorf("invalid recovery id %d", v)
	}
}
