package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwiltypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/sdk-go/core/tnclient"
)

const (
	kwildEndpoint      = "http://localhost:8484"
	deployerPrivateKey = "0000000000000000000000000000000000000000000000000000000000000001"
)

// TestAttestationE2E runs end-to-end test for the attestation extension
// covering the complete workflow: request → queue → end-of-block → sign → retrieve
func TestAttestationE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Parse deployer private key
	deployerWallet, err := kwilcrypto.Secp256k1PrivateKeyFromHex(deployerPrivateKey)
	require.NoError(t, err, "Failed to parse deployer private key")

	// Create TN client
	tnClient, err := tnclient.NewClient(ctx, kwildEndpoint, tnclient.WithSigner(auth.GetUserSigner(deployerWallet)))
	if err != nil {
		t.Skipf("Failed to create TN client (is the test environment running?): %v", err)
		return
	}

	t.Log("Connected to kwild at", kwildEndpoint)

	// Run the test
	t.Run("CompleteAttestationWorkflow", func(t *testing.T) {
		testCompleteAttestationWorkflow(ctx, t, tnClient)
	})
}

func testCompleteAttestationWorkflow(ctx context.Context, t *testing.T, client *tnclient.Client) {
	t.Log("Testing complete attestation workflow...")

	const testActionName = "e2e_test_action"

	// Step 1: Create test action and add to allowlist
	t.Log("Step 1: Creating test action and adding to allowlist...")
	err := createTestAction(ctx, t, client, testActionName)
	require.NoError(t, err, "Failed to create test action")

	// Step 2: Submit request_attestation transaction
	t.Log("Step 2: Submitting request_attestation transaction...")
	requestTxID, attestationHash, err := requestAttestation(ctx, t, client, testActionName)
	require.NoError(t, err, "Failed to request attestation")
	require.NotEmpty(t, requestTxID, "request_tx_id should not be empty")
	require.NotEmpty(t, attestationHash, "attestation_hash should not be empty")
	t.Logf("Attestation requested: request_tx_id=%s, hash=%x", requestTxID, attestationHash)

	// Step 3: Wait for end-of-block processing and signature generation
	t.Log("Step 3: Waiting for end-of-block processing and signature generation...")
	signatureFound := waitForSignature(ctx, t, client, requestTxID, 90*time.Second)
	require.True(t, signatureFound, "Signature not generated within expected time (90s)")

	// Step 4: Retrieve and verify signed attestation
	t.Log("Step 4: Retrieving and verifying signed attestation...")
	payload, err := getSignedAttestation(ctx, t, client, requestTxID)
	require.NoError(t, err, "Failed to get signed attestation")
	require.NotNil(t, payload, "Signed payload should not be nil")
	require.Greater(t, len(payload), 65, "Payload should contain canonical + 65-byte signature")

	// Verify signature is appended at the end
	signature := payload[len(payload)-65:]
	canonical := payload[:len(payload)-65]
	require.Len(t, signature, 65, "Signature should be exactly 65 bytes")
	require.NotEmpty(t, canonical, "Canonical payload should not be empty")

	t.Log("Test completed successfully!")
	t.Logf("Final payload size: %d bytes (canonical: %d, signature: 65)", len(payload), len(canonical))
}

// createTestAction creates a simple test action and adds it to the attestation allowlist
func createTestAction(ctx context.Context, t *testing.T, client *tnclient.Client, actionName string) error {
	// Create the action
	createActionSQL := fmt.Sprintf(`
		CREATE OR REPLACE ACTION %s(
			$value INT8
		) PUBLIC VIEW RETURNS TABLE(result INT8) {
			RETURN NEXT $value;
		};
	`, actionName)

	txHash, err := client.GetKwilClient().Execute(ctx, "", "execute", [][]any{{createActionSQL}})
	if err != nil {
		return fmt.Errorf("failed to create action: %w", err)
	}
	t.Logf("Created action, tx hash: %s", txHash.String())

	// Wait for action creation to be confirmed
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return fmt.Errorf("action creation not confirmed: %w", err)
	}

	// Add action to attestation allowlist (action_id = 100 for test)
	insertAllowlistSQL := fmt.Sprintf(`
		INSERT INTO attestation_actions(action_name, action_id)
		VALUES ('%s', 100)
		ON CONFLICT (action_name) DO UPDATE SET action_id = EXCLUDED.action_id;
	`, actionName)

	txHash, err = client.GetKwilClient().Execute(ctx, "", "execute", [][]any{{insertAllowlistSQL}})
	if err != nil {
		return fmt.Errorf("failed to add action to allowlist: %w", err)
	}
	t.Logf("Added action to allowlist, tx hash: %s", txHash.String())

	// Wait for allowlist update to be confirmed
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return fmt.Errorf("allowlist update not confirmed: %w", err)
	}

	t.Log("Test action created and allowlisted successfully")
	return nil
}

// requestAttestation submits a request_attestation transaction and returns the request_tx_id and attestation_hash
func requestAttestation(ctx context.Context, t *testing.T, client *tnclient.Client, actionName string) (string, []byte, error) {
	// Encode action arguments (value = 42)
	argsBytes := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2a} // Simple encoding of [42]

	// Submit request_attestation
	txHash, err := client.GetKwilClient().Execute(ctx, "", "request_attestation", [][]any{{
		[]byte("test-provider"),
		[]byte("test-stream"),
		actionName,
		argsBytes,
		false,
		int64(0),
	}})
	if err != nil {
		return "", nil, fmt.Errorf("failed to execute request_attestation: %w", err)
	}
	t.Logf("Submitted request_attestation, tx hash: %s", txHash.String())

	// Wait for transaction confirmation
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return "", nil, fmt.Errorf("request_attestation not confirmed: %w", err)
	}

	// The transaction ID is the hex string representation of the hash
	requestTxID := txHash.String()
	t.Logf("Request confirmed: request_tx_id=%s", requestTxID)

	return requestTxID, nil, nil
}

// waitForSignature polls for signature generation by checking if get_signed_attestation succeeds
func waitForSignature(ctx context.Context, t *testing.T, client *tnclient.Client, requestTxID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	attempt := 0

	for time.Now().Before(deadline) {
		attempt++

		// Try to get signed attestation
		result, err := client.GetKwilClient().Call(ctx, "", "get_signed_attestation", []any{requestTxID})

		if err == nil && result != nil && result.QueryResult != nil && len(result.QueryResult.Values) > 0 {
			t.Logf("Signature found after %d attempts", attempt)
			return true
		}

		// Log progress every 5 attempts
		if attempt%5 == 0 {
			t.Logf("Waiting for signature... (attempt %d)", attempt)
		}

		// Wait before next attempt
		time.Sleep(3 * time.Second)
	}

	t.Logf("Signature not found after %d attempts over %v", attempt, timeout)
	return false
}

// getSignedAttestation retrieves the signed attestation payload
func getSignedAttestation(ctx context.Context, t *testing.T, client *tnclient.Client, requestTxID string) ([]byte, error) {
	result, err := client.GetKwilClient().Call(ctx, "", "get_signed_attestation", []any{requestTxID})
	if err != nil {
		return nil, fmt.Errorf("failed to call get_signed_attestation: %w", err)
	}

	if result == nil || result.QueryResult == nil || len(result.QueryResult.Values) == 0 {
		return nil, fmt.Errorf("no result returned from get_signed_attestation")
	}

	// Extract payload from result using ExportToStringMap
	resultMap := result.QueryResult.ExportToStringMap()
	if len(resultMap) == 0 {
		return nil, fmt.Errorf("no rows in result")
	}

	// The payload column should be present
	payloadStr, exists := resultMap[0]["payload"]
	if !exists {
		return nil, fmt.Errorf("payload column not found in result")
	}

	// Convert the string representation to bytes
	// The payload is returned as a bytea which exports as a string
	payload := []byte(payloadStr)

	return payload, nil
}

// waitForTxConfirmation waits for a transaction to be confirmed
func waitForTxConfirmation(ctx context.Context, client *tnclient.Client, txHash kwiltypes.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		result, err := client.GetKwilClient().TxQuery(ctx, txHash)
		if err == nil && result != nil && result.Height > 0 {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("transaction not confirmed within %v", timeout)
}
