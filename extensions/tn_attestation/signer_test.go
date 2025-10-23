package tn_attestation

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
)

func TestValidatorSigner(t *testing.T) {
	t.Run("NewValidatorSigner", func(t *testing.T) {
		// Generate a test private key
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)
		assert.NotNil(t, signer)
		assert.NotNil(t, signer.privateKey)
	})

	t.Run("NewValidatorSignerWithNilKey", func(t *testing.T) {
		signer, err := NewValidatorSigner(nil)
		assert.Error(t, err)
		assert.Nil(t, signer)
		assert.Contains(t, err.Error(), "private key cannot be nil")
	})

	t.Run("SignDigest", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		digest := sha256.Sum256([]byte("attestation payload"))

		signature, err := signer.SignDigest(digest[:])
		require.NoError(t, err)
		assert.NotNil(t, signature)
		assert.Equal(t, 65, len(signature))
	})

	t.Run("SignDigestInvalidLength", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		signature, err := signer.SignDigest([]byte{})
		assert.Error(t, err)
		assert.Nil(t, signature)
		assert.Contains(t, err.Error(), "digest must be 32 bytes")
	})

	t.Run("SignKeccak256", func(t *testing.T) {
		// Generate a test private key
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		// Test payload
		payload := []byte("test attestation payload")

		// Sign the payload
		signature, err := signer.SignKeccak256(payload)
		require.NoError(t, err)
		assert.NotNil(t, signature)
		assert.Equal(t, 65, len(signature), "signature should be 65 bytes [R || S || V]")
	})

	t.Run("SignKeccak256EmptyPayload", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		// Try to sign empty payload
		signature, err := signer.SignKeccak256([]byte{})
		assert.Error(t, err)
		assert.Nil(t, signature)
		assert.Contains(t, err.Error(), "payload cannot be empty")
	})

	t.Run("SignatureVerificationWithDigest", func(t *testing.T) {
		// Generate a test private key
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		// Test payload
		payload := []byte("test attestation payload")
		digest := sha256.Sum256(payload)

		// Sign the digest
		signature, err := signer.SignDigest(digest[:])
		require.NoError(t, err)

		// Verify V byte is EVM-compatible (27 or 28)
		v := signature[64]
		assert.True(t, v == 27 || v == 28, "V must be 27 or 28, got %d", v)

		// Verify the signature can be recovered
		// Note: Go's crypto.SigToPub expects V as 0/1, so convert temporarily
		testSig := make([]byte, len(signature))
		copy(testSig, signature)
		testSig[64] -= 27 // Convert 27/28 → 0/1

		recoveredPubKey, err := crypto.SigToPub(digest[:], testSig)
		require.NoError(t, err)

		// Verify the recovered public key matches the signer's public key
		expectedPubKey := crypto.FromECDSAPub(&signer.privateKey.PublicKey)
		actualPubKey := crypto.FromECDSAPub(recoveredPubKey)
		assert.Equal(t, expectedPubKey, actualPubKey, "recovered public key should match signer's public key")
	})

	t.Run("PublicKey", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		pubKey := signer.PublicKey()
		assert.NotNil(t, pubKey)
		assert.NotEmpty(t, pubKey)
		// Uncompressed public key is 65 bytes (0x04 + 32-byte X + 32-byte Y)
		assert.Equal(t, 65, len(pubKey))
	})

	t.Run("Address", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		address := signer.Address()
		assert.NotEmpty(t, address)
		// Ethereum address format: 0x + 40 hex characters
		assert.Equal(t, 42, len(address))
		assert.True(t, address[:2] == "0x", "address should start with 0x")
	})

	t.Run("DeterministicSignatureDigest", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		payload := []byte("deterministic test payload")
		digest := sha256.Sum256(payload)

		// Sign the same payload twice
		sig1, err := signer.SignDigest(digest[:])
		require.NoError(t, err)

		sig2, err := signer.SignDigest(digest[:])
		require.NoError(t, err)

		// Signatures should be identical for the same payload and key
		assert.Equal(t, sig1, sig2, "signatures should be deterministic")
	})

	t.Run("ConcurrentSigning", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		var wg sync.WaitGroup
		numGoroutines := 100
		results := make([][]byte, numGoroutines)

		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(idx int) {
				defer wg.Done()
				payload := []byte("concurrent test payload")
				digest := sha256.Sum256(payload)
				signature, err := signer.SignDigest(digest[:])
				require.NoError(t, err)
				results[idx] = signature
			}(i)
		}

		wg.Wait()

		// All signatures should be identical and valid
		for i := 1; i < numGoroutines; i++ {
			assert.Equal(t, results[0], results[i], "all concurrent signatures should be identical")
		}
	})
}

func TestValidatorSignerSingleton(t *testing.T) {
	// Reset singleton before each test
	defer ResetValidatorSignerForTesting()

	t.Run("InitializeValidatorSigner", func(t *testing.T) {
		ResetValidatorSignerForTesting()

		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		err = InitializeValidatorSigner(privateKey)
		require.NoError(t, err)

		signer := GetValidatorSigner()
		assert.NotNil(t, signer)
	})

	t.Run("SingletonOnlyInitializedOnce", func(t *testing.T) {
		ResetValidatorSignerForTesting()

		privateKey1, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		privateKey2, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		// Initialize with first key
		err = InitializeValidatorSigner(privateKey1)
		require.NoError(t, err)

		signer1 := GetValidatorSigner()
		address1 := signer1.Address()

		// Try to initialize with second key (should be ignored)
		err = InitializeValidatorSigner(privateKey2)
		require.NoError(t, err) // No error, but should be ignored

		signer2 := GetValidatorSigner()
		address2 := signer2.Address()

		// Address should remain the same (first key)
		assert.Equal(t, address1, address2, "singleton should only be initialized once")
		assert.Same(t, signer1, signer2, "should return the same instance")
	})

	t.Run("GetValidatorSignerBeforeInit", func(t *testing.T) {
		ResetValidatorSignerForTesting()

		signer := GetValidatorSigner()
		assert.Nil(t, signer, "should return nil before initialization")
	})

	t.Run("ResetForTesting", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		err = InitializeValidatorSigner(privateKey)
		require.NoError(t, err)

		signer := GetValidatorSigner()
		assert.NotNil(t, signer)

		// Reset
		ResetValidatorSignerForTesting()

		signer = GetValidatorSigner()
		assert.Nil(t, signer, "should return nil after reset")
	})
}

func TestEVMCompatibility(t *testing.T) {
	t.Run("SignatureRecoverableByEcrecover", func(t *testing.T) {
		// Generate a test private key
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		// Create test payload (simulating attestation structure)
		payload := []byte("version|algo|dataProvider|streamId|actionId|args|result")
		digest := sha256.Sum256(payload)

		// Sign the payload
		signature, err := signer.SignDigest(digest[:])
		require.NoError(t, err)

		// Verify signature format is EVM-compatible
		assert.Equal(t, 65, len(signature), "signature must be 65 bytes for EVM compatibility")

		// Verify V byte is 27 or 28 (EVM standard, required by OpenZeppelin and modern contracts)
		v := signature[64]
		assert.True(t, v == 27 || v == 28, "V must be 27 or 28 for EVM compatibility, got %d", v)

		// Recover the signer address from the signature (simulating Solidity ecrecover)
		testSig := make([]byte, len(signature))
		copy(testSig, signature)
		testSig[64] -= 27 // Convert 27/28 → 0/1 for Go's Ecrecover

		recoveredPubKey, err := crypto.Ecrecover(digest[:], testSig)
		require.NoError(t, err)
		assert.NotNil(t, recoveredPubKey)

		// Convert recovered public key to address
		pubKey, err := crypto.UnmarshalPubkey(recoveredPubKey)
		require.NoError(t, err)
		recoveredAddress := crypto.PubkeyToAddress(*pubKey)

		// Verify the address matches the signer's address
		expectedAddress := signer.Address()
		assert.Equal(t, expectedAddress, recoveredAddress.Hex(), "ecrecover should recover correct address")
	})

	t.Run("VByteIsEVMCompatible", func(t *testing.T) {
		privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		signer, err := NewValidatorSigner(privateKey)
		require.NoError(t, err)

		// Test multiple signatures to ensure V is always 27 or 28
		for i := 0; i < 10; i++ {
			payload := []byte(fmt.Sprintf("test payload %d", i))
			digest := sha256.Sum256(payload)
			signature, err := signer.SignDigest(digest[:])
			require.NoError(t, err)

			// Signature must be 65 bytes
			require.Equal(t, 65, len(signature))

			// V byte (signature[64]) must be 27 or 28 for EVM compatibility
			// This is required by:
			// - Ethereum Yellow Paper Appendix F
			// - OpenZeppelin ECDSA.sol (rejects V < 27)
			// - Modern smart contracts with explicit V validation
			v := signature[64]
			assert.True(t, v == 27 || v == 28,
				"V must be 27 or 28 for EVM compatibility, got %d (iteration %d)", v, i)
		}
	})
}

func TestNonSecp256k1KeyHandling(t *testing.T) {
	t.Run("Ed25519KeyRejected", func(t *testing.T) {
		// Generate an Ed25519 key (32 bytes)
		ed25519Key, _, err := kwilcrypto.GenerateEd25519Key(nil)
		require.NoError(t, err)

		// Try to create ValidatorSigner with Ed25519 key
		signer, err := NewValidatorSigner(ed25519Key)

		// Should fail because Ed25519 keys cannot be converted to ECDSA
		assert.Error(t, err, "Ed25519 key should be rejected")
		assert.Nil(t, signer, "signer should be nil for Ed25519 key")
		assert.Contains(t, err.Error(), "failed to convert private key to ECDSA",
			"error should indicate ECDSA conversion failure")
	})

	t.Run("Secp256k1KeyAccepted", func(t *testing.T) {
		// Generate a secp256k1 key for comparison
		secp256k1Key, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)

		// Should succeed
		signer, err := NewValidatorSigner(secp256k1Key)
		assert.NoError(t, err, "secp256k1 key should be accepted")
		assert.NotNil(t, signer, "signer should be created for secp256k1 key")
	})
}
