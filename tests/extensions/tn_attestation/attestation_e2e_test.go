package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clienttypes "github.com/trufnetwork/kwil-db/core/client/types"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwiltypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/sdk-go/core/tnclient"
	"github.com/trufnetwork/sdk-go/core/util"
)

const (
	kwildEndpoint      = "http://localhost:8484"
	deployerPrivateKey = "0000000000000000000000000000000000000000000000000000000000000001"
)

// TestAttestationE2E runs end-to-end test for the attestation extension
// using a realistic stream-based workflow: create stream → insert data → request attestation → verify signature
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
	t.Run("CompleteStreamAttestationWorkflow", func(t *testing.T) {
		testCompleteStreamAttestationWorkflow(ctx, t, tnClient, deployerWallet)
	})
}

func testCompleteStreamAttestationWorkflow(ctx context.Context, t *testing.T, client *tnclient.Client, deployerWallet *kwilcrypto.Secp256k1PrivateKey) {
	t.Log("Testing complete stream-based attestation workflow...")

	// Generate unique stream ID for this test
	testStreamName := fmt.Sprintf("attestation_e2e_test_%d", time.Now().Unix())
	streamID := util.GenerateStreamId(testStreamName)

	// Get data provider address from wallet
	pubKey, ok := deployerWallet.Public().(*kwilcrypto.Secp256k1PublicKey)
	require.True(t, ok, "Failed to convert public key to Secp256k1PublicKey")
	dataProviderAddr := kwilcrypto.EthereumAddressFromPubKey(pubKey)
	dataProviderHex := fmt.Sprintf("0x%x", dataProviderAddr)

	t.Logf("Using stream: %s", streamID.String())
	t.Logf("Data provider: %s", dataProviderHex)

	// Cleanup: delete stream at the end
	defer func() {
		t.Log("Cleanup: Deleting stream...")
		_ = deleteStream(ctx, t, client, dataProviderHex, streamID.String())
	}()

	// Step 1: Register as data provider
	t.Log("Step 1: Registering as data provider...")
	err := registerDataProvider(ctx, t, client)
	require.NoError(t, err, "Failed to register data provider")

	// Step 2: Create primitive stream
	t.Log("Step 2: Creating primitive stream...")
	err = createPrimitiveStream(ctx, t, client, streamID.String())
	require.NoError(t, err, "Failed to create primitive stream")

	// Step 3: Insert sample data
	t.Log("Step 3: Inserting sample data...")
	err = insertStreamData(ctx, t, client, dataProviderHex, streamID.String())
	require.NoError(t, err, "Failed to insert stream data")

	// Step 4: Request attestation for get_record query
	t.Log("Step 4: Requesting attestation for get_record...")
	requestTxID, err := requestStreamAttestation(ctx, t, client, dataProviderHex, streamID.String())
	require.NoError(t, err, "Failed to request attestation")
	require.NotEmpty(t, requestTxID, "request_tx_id should not be empty")
	t.Logf("Attestation requested: request_tx_id=%s", requestTxID)

	// Step 5: Wait for end-of-block processing and signature generation
	t.Log("Step 5: Waiting for end-of-block processing and signature generation...")
	signatureFound := waitForSignature(ctx, t, client, requestTxID, 90*time.Second)
	require.True(t, signatureFound, "Signature not generated within expected time (90s)")

	// Step 6: Retrieve and verify signed attestation
	t.Log("Step 6: Retrieving and verifying signed attestation...")
	payload, err := getSignedAttestation(ctx, t, client, requestTxID)
	require.NoError(t, err, "Failed to get signed attestation")
	require.NotNil(t, payload, "Signed payload should not be nil")
	require.Greater(t, len(payload), 65, "Payload should contain canonical + 65-byte signature")

	// Verify signature is appended at the end
	signature := payload[len(payload)-65:]
	canonical := payload[:len(payload)-65]
	require.Len(t, signature, 65, "Signature should be exactly 65 bytes")
	require.NotEmpty(t, canonical, "Canonical payload should not be empty")

	// Step 7: Log attestation result to console
	t.Log("=== Attestation Result ===")
	t.Logf("Signed attestation payload (hex): %s", hex.EncodeToString(payload))
	t.Logf("Payload length: %d bytes", len(payload))
	t.Logf("Canonical portion (%d bytes): %s", len(canonical), hex.EncodeToString(canonical))
	t.Logf("Signature portion (65 bytes): %s", hex.EncodeToString(signature))
	t.Log("=========================")

	t.Log("Test completed successfully!")
	t.Logf("Final payload size: %d bytes (canonical: %d, signature: 65)", len(payload), len(canonical))
}

// registerDataProvider registers the signer as a data provider
func registerDataProvider(ctx context.Context, t *testing.T, client *tnclient.Client) error {
	// Call register_data_provider action
	txHash, err := client.GetKwilClient().Execute(ctx, "", "register_data_provider", [][]any{{}}, clienttypes.WithSyncBroadcast(true))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unknown action") {
			t.Logf("register_data_provider action not available, skipping registration: %v", err)
			return nil
		}
		return fmt.Errorf("failed to execute register_data_provider: %w", err)
	}

	t.Logf("Registered data provider, tx hash: %s", txHash.String())

	// Wait for confirmation
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return fmt.Errorf("data provider registration not confirmed: %w", err)
	}

	return nil
}

// createPrimitiveStream creates a new primitive stream
func createPrimitiveStream(ctx context.Context, t *testing.T, client *tnclient.Client, streamID string) error {
	// Call create_stream action
	txHash, err := client.GetKwilClient().Execute(ctx, "", "create_stream", [][]any{{
		streamID,
		"primitive",
	}}, clienttypes.WithSyncBroadcast(true))
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	t.Logf("Created stream, tx hash: %s", txHash.String())

	// Wait for confirmation
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return fmt.Errorf("stream creation not confirmed: %w", err)
	}

	return nil
}

// insertStreamData inserts sample time-series data into the stream
func insertStreamData(ctx context.Context, t *testing.T, client *tnclient.Client, dataProvider, streamID string) error {
	// Insert a single data point: event_time=1000, value=100.5
	eventTime := int64(1000)
	valueDecimal, err := kwiltypes.ParseDecimalExplicit("100.5", 36, 18)
	require.NoError(t, err, "failed to build decimal value")

	txHash, err := client.GetKwilClient().Execute(ctx, "", "insert_record", [][]any{{
		dataProvider,
		streamID,
		eventTime,
		valueDecimal,
	}}, clienttypes.WithSyncBroadcast(true))
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	t.Logf("Inserted record (event_time=%d, value=%s), tx hash: %s", eventTime, valueDecimal.String(), txHash.String())

	// Wait for confirmation
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return fmt.Errorf("record insertion not confirmed: %w", err)
	}

	// Wait a bit for the data to be fully indexed
	time.Sleep(2 * time.Second)

	return nil
}

// requestStreamAttestation requests an attestation for a get_record query on the stream
func requestStreamAttestation(ctx context.Context, t *testing.T, client *tnclient.Client, dataProviderHex, streamID string) (string, error) {
	dataProviderBytes, err := hex.DecodeString(dataProviderHex[2:])
	if err != nil {
		return "", fmt.Errorf("failed to decode data provider: %w", err)
	}
	streamIDBytes := []byte(streamID)

	argsData := []any{
		dataProviderHex,
		streamID,
		int64(1000),
		int64(1000),
		nil,
		false,
	}

	argsBytes, err := tn_utils.EncodeActionArgs(argsData)
	if err != nil {
		return "", fmt.Errorf("failed to encode args: %w", err)
	}

	txHash, err := client.GetKwilClient().Execute(ctx, "", "request_attestation", [][]any{{
		dataProviderBytes,
		streamIDBytes,
		"get_record",
		argsBytes,
		false,
		int64(0),
	}}, clienttypes.WithSyncBroadcast(true))
	if err != nil {
		return "", fmt.Errorf("failed to execute request_attestation: %w", err)
	}
	t.Logf("Submitted request_attestation, tx hash: %s", txHash.String())

	// Wait for transaction confirmation
	if err := waitForTxConfirmation(ctx, client, txHash, 30*time.Second); err != nil {
		return "", fmt.Errorf("request_attestation not confirmed: %w", err)
	}

	// The transaction ID is the hex string representation of the hash
	requestTxID := txHash.String()
	t.Logf("Request confirmed: request_tx_id=%s", requestTxID)

	return requestTxID, nil
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

// deleteStream deletes a stream for cleanup
func deleteStream(ctx context.Context, t *testing.T, client *tnclient.Client, dataProvider, streamID string) error {
	txHash, err := client.GetKwilClient().Execute(ctx, "", "delete_stream", [][]any{{
		dataProvider,
		streamID,
	}}, clienttypes.WithSyncBroadcast(true))
	if err != nil {
		t.Logf("Warning: Failed to delete stream: %v", err)
		return err
	}

	t.Logf("Deleted stream, tx hash: %s", txHash.String())
	return nil
}
