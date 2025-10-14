package tn_attestation

import (
	"testing"

	"github.com/stretchr/testify/require"
	kcrypto "github.com/trufnetwork/kwil-db/core/crypto"
)

func TestVerifyAttestationPayload_ValidSignature(t *testing.T) {
	// Generate validator key pair
	privateKey, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)

	// Create validator signer
	signer, err := NewValidatorSigner(privateKey)
	require.NoError(t, err)

	// Create canonical payload
	canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	// Sign the payload digest
	digest := payload.SigningDigest()
	signature, err := signer.SignDigest(digest[:])
	require.NoError(t, err)
	require.Len(t, signature, 65, "signature should be 65 bytes")

	// Get validator public key bytes
	validatorPubkey := publicKey.Bytes()

	// Verify the payload
	valid, err := VerifyAttestationPayload(canonical, signature, validatorPubkey)
	require.NoError(t, err)
	require.True(t, valid, "valid signature should verify successfully")
}

func TestVerifyAttestationPayload_InvalidSignature(t *testing.T) {
	// Generate validator key pair
	privateKey, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)

	// Create validator signer
	signer, err := NewValidatorSigner(privateKey)
	require.NoError(t, err)

	// Create canonical payload
	canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	// Sign the payload digest
	digest := payload.SigningDigest()
	signature, err := signer.SignDigest(digest[:])
	require.NoError(t, err)

	// Corrupt the signature by flipping some bits
	signature[10] ^= 0xFF
	signature[20] ^= 0xFF

	// Get validator public key bytes
	validatorPubkey := publicKey.Bytes()

	// Verify should fail
	valid, err := VerifyAttestationPayload(canonical, signature, validatorPubkey)
	require.Error(t, err, "corrupted signature should fail verification")
	require.False(t, valid)
}

func TestVerifyAttestationPayload_WrongValidatorPubkey(t *testing.T) {
	// Generate two different key pairs
	privateKey1, _, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	_, publicKey2, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)

	// Create validator signer
	signer, err := NewValidatorSigner(privateKey1)
	require.NoError(t, err)

	// Create canonical payload
	canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	// Sign with key1
	digest := payload.SigningDigest()
	signature, err := signer.SignDigest(digest[:])
	require.NoError(t, err)

	// Try to verify with key2's public key
	wrongPubkey := publicKey2.Bytes()

	// Verification should fail
	valid, err := VerifyAttestationPayload(canonical, signature, wrongPubkey)
	require.Error(t, err, "signature from different key should fail verification")
	require.Contains(t, err.Error(), "does not match")
	require.False(t, valid)
}

func TestVerifyAttestationPayload_MalformedCanonical(t *testing.T) {
	// Generate validator key pair
	privateKey, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)

	// Create validator signer
	signer, err := NewValidatorSigner(privateKey)
	require.NoError(t, err)

	// Create malformed canonical payload (too short)
	malformedCanonical := []byte{0x01, 0x02, 0x03}

	// Create a valid signature (doesn't matter since parsing will fail first)
	digest := [32]byte{}
	signature, err := signer.SignDigest(digest[:])
	require.NoError(t, err)

	validatorPubkey := publicKey.Bytes()

	// Verification should fail during parsing
	valid, err := VerifyAttestationPayload(malformedCanonical, signature, validatorPubkey)
	require.Error(t, err, "malformed canonical should fail parsing")
	require.Contains(t, err.Error(), "parse canonical payload")
	require.False(t, valid)
}

func TestVerifyAttestationPayload_InvalidSignatureLength(t *testing.T) {
	// Generate validator key pair
	_, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)

	// Create canonical payload
	canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})

	// Create signature with wrong length
	invalidSignature := []byte{0x01, 0x02, 0x03, 0x04} // Only 4 bytes instead of 65

	validatorPubkey := publicKey.Bytes()

	// Verification should fail due to invalid signature length
	valid, err := VerifyAttestationPayload(canonical, invalidSignature, validatorPubkey)
	require.Error(t, err, "invalid signature length should fail")
	require.False(t, valid)
}

func TestVerifyAttestationPayload_EmptyInputs(t *testing.T) {
	t.Run("nil canonical", func(t *testing.T) {
		valid, err := VerifyAttestationPayload(nil, []byte{}, []byte{})
		require.Error(t, err)
		require.False(t, valid)
	})

	t.Run("empty canonical", func(t *testing.T) {
		valid, err := VerifyAttestationPayload([]byte{}, make([]byte, 65), []byte{0x01})
		require.Error(t, err)
		require.False(t, valid)
	})

	t.Run("nil signature", func(t *testing.T) {
		canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})
		valid, err := VerifyAttestationPayload(canonical, nil, []byte{0x01})
		require.Error(t, err)
		require.False(t, valid)
	})

	t.Run("nil validator pubkey", func(t *testing.T) {
		privateKey, _, err := kcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})
		payload, err := ParseCanonicalPayload(canonical)
		require.NoError(t, err)

		digest := payload.SigningDigest()
		signature, err := signer.SignDigest(digest[:])
		require.NoError(t, err)

		valid, err := VerifyAttestationPayload(canonical, signature, nil)
		require.Error(t, err)
		require.False(t, valid)
	})
}

func TestVerifyAttestationPayload_WrongRecoveryID(t *testing.T) {
	// Generate validator key pair
	privateKey, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)

	// Create validator signer
	signer, err := NewValidatorSigner(privateKey)
	require.NoError(t, err)

	// Create canonical payload
	canonical := buildCanonical(1, 1, 100, []byte("provider"), []byte("stream"), 1, []byte{0x01}, []byte{0x02})
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	// Sign the payload digest
	digest := payload.SigningDigest()
	signature, err := signer.SignDigest(digest[:])
	require.NoError(t, err)

	// Corrupt the recovery ID (byte 64)
	signature[64] = 99 // Invalid recovery ID

	validatorPubkey := publicKey.Bytes()

	// Verification should fail
	valid, err := VerifyAttestationPayload(canonical, signature, validatorPubkey)
	require.Error(t, err, "invalid recovery ID should fail verification")
	require.False(t, valid)
}
