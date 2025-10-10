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

// SignKeccak256 signs the keccak256 hash of the payload and returns a 65-byte EVM-compatible signature.
// The signature format is [R || S || V] where:
// - R: 32 bytes (signature R component)
// - S: 32 bytes (signature S component)
// - V: 1 byte (recovery ID, 27 or 28 for EVM compatibility)
func (s *ValidatorSigner) SignKeccak256(payload []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.privateKey == nil {
		return nil, fmt.Errorf("private key not initialized")
	}

	if len(payload) == 0 {
		return nil, fmt.Errorf("payload cannot be empty")
	}

	// Compute keccak256 hash of the payload
	hash := crypto.Keccak256Hash(payload)

	// Sign the hash using secp256k1
	signature, err := crypto.Sign(hash.Bytes(), s.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign payload: %w", err)
	}

	// crypto.Sign returns 65-byte signature [R || S || V] where V is 0 or 1
	// EVM's ecrecover expects V as 27 or 28, so convert:
	// V=0 (even Y) → 27, V=1 (odd Y) → 28
	signature[64] += 27

	return signature, nil
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
