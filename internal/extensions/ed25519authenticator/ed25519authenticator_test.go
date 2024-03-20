package ed25519authenticator_test

import (
	"crypto/ed25519"
	"github.com/stretchr/testify/assert"
	"github.com/truflation/tsn-db/internal/extensions/ed25519authenticator"
	"testing"
)

type ed25519AuthenticatorTest struct {
	ed25519Authenticator *ed25519authenticator.Ed25519Authenticator
}

func newEd25519AuthenticatorTest(t *testing.T) ed25519AuthenticatorTest {
	return ed25519AuthenticatorTest{
		ed25519Authenticator: &ed25519authenticator.Ed25519Authenticator{},
	}
}

func TestEd25519Authenticator_Verify(t *testing.T) {
	instance := newEd25519AuthenticatorTest(t)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err, "Failed to generate key pair")
	msg := []byte("test message")

	t.Run("success - it should return nil", func(t *testing.T) {
		signature := ed25519.Sign(privateKey, msg)
		err = instance.ed25519Authenticator.Verify(publicKey, msg, signature)
		assert.NoError(t, err, "Signature verification failed with valid signature")
	})

	t.Run("error - it should return error", func(t *testing.T) {
		invalidSignature := []byte("invalid signature")
		err = instance.ed25519Authenticator.Verify(publicKey, msg, invalidSignature)
		assert.Error(t, err, "Signature verification did not fail with invalid signature")
	})
}

func TestEd25519Authenticator_Identifier(t *testing.T) {
	instance := newEd25519AuthenticatorTest(t)
	sender := []byte{0x01, 0x02, 0x03, 0x04}
	identifier, err := instance.ed25519Authenticator.Identifier(sender)

	t.Run("success - it should return the hex encoding of the public key", func(t *testing.T) {
		expectedIdentifier := "01020304"
		assert.NoError(t, err, "Identifier function returned an error")
		assert.Equal(t, expectedIdentifier, identifier, "Identifier does not match the expected hexadecimal string")
	})
}
