package tn_attestation

import (
	"crypto/ecdsa"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/crypto"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
)

// ValidatorSigner handles signing attestation payloads using the validator's secp256k1 private key.
// It provides thread-safe signing operations that produce EVM-compatible signatures.
type ValidatorSigner struct {
	privateKey *ecdsa.PrivateKey
	mu         sync.RWMutex
}

var (
	validatorSigner     *ValidatorSigner
	validatorSignerOnce sync.Once
)

// NewValidatorSigner creates a new ValidatorSigner from a Kwil private key.
// The private key must be a secp256k1 key.
func NewValidatorSigner(privateKey kwilcrypto.PrivateKey) (*ValidatorSigner, error) {
	if privateKey == nil {
		return nil, fmt.Errorf("private key cannot be nil")
	}

	// Extract the raw bytes from Kwil's PrivateKey
	keyBytes := privateKey.Bytes()

	// Convert to ECDSA private key for signing
	ecdsaKey, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert private key to ECDSA: %w", err)
	}

	return &ValidatorSigner{
		privateKey: ecdsaKey,
	}, nil
}

// SignDigest signs the provided 32-byte digest (already hashed) and returns a 65-byte EVM-compatible signature.
func (s *ValidatorSigner) SignDigest(digest []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.privateKey == nil {
		return nil, fmt.Errorf("private key not initialized")
	}

	if len(digest) != crypto.DigestLength {
		return nil, fmt.Errorf("digest must be %d bytes, got %d", crypto.DigestLength, len(digest))
	}

	signature, err := crypto.Sign(digest, s.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign digest: %w", err)
	}

	// Convert V to EVM-compatible {27,28}.
	// crypto.Sign returns V in various forms ({0,1}, {27,28}, or {27+chainId*2, 28+chainId*2}).
	// We normalize: subtract 27 to get base form, mask to {0,1}, then add 27 for EVM format.
	v := signature[64]
	if v >= 27 {
		v -= 27 // Strip any offset (27 or 27+chainId*2)
	}
	v &= 1                 // Ensure only bit 0 remains (normalize to 0 or 1)
	signature[64] = v + 27 // Add EVM offset to get {27,28}
	return signature, nil
}

// SignKeccak256 signs the keccak256 hash of the payload and returns a 65-byte EVM-compatible signature.
// The signature format is [R || S || V] where:
// - R: 32 bytes (signature R component)
// - S: 32 bytes (signature S component)
// - V: 1 byte (recovery ID, 27 or 28 for EVM compatibility)
func (s *ValidatorSigner) SignKeccak256(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("payload cannot be empty")
	}

	// Compute keccak256 hash of the payload
	hash := crypto.Keccak256Hash(payload)

	// Sign the hash using secp256k1
	return s.SignDigest(hash.Bytes())
}

// PublicKey returns the public key associated with this signer (for verification).
func (s *ValidatorSigner) PublicKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.privateKey == nil {
		return nil
	}

	return crypto.FromECDSAPub(&s.privateKey.PublicKey)
}

// Address returns the Ethereum address derived from the public key.
func (s *ValidatorSigner) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.privateKey == nil {
		return ""
	}

	address := crypto.PubkeyToAddress(s.privateKey.PublicKey)
	return address.Hex()
}

// InitializeValidatorSigner initializes the global validator signer singleton.
// This should be called once during extension initialization with the validator's private key.
func InitializeValidatorSigner(privateKey kwilcrypto.PrivateKey) error {
	var err error
	validatorSignerOnce.Do(func() {
		validatorSigner, err = NewValidatorSigner(privateKey)
	})
	return err
}

// GetValidatorSigner returns the global validator signer singleton.
// Returns nil if InitializeValidatorSigner has not been called.
func GetValidatorSigner() *ValidatorSigner {
	return validatorSigner
}

// ResetValidatorSignerForTesting resets the global validator signer singleton.
// This should only be used in tests.
func ResetValidatorSignerForTesting() {
	validatorSigner = nil
	validatorSignerOnce = sync.Once{}
}
