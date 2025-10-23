package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	clienttypes "github.com/trufnetwork/kwil-db/core/client/types"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwiltypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/node/extensions/tn_attestation"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/sdk-go/core/tnclient"
	"github.com/trufnetwork/sdk-go/core/util"
)

const (
	kwildEndpoint        = "http://localhost:8484"
	deployerPrivateKey   = "0000000000000000000000000000000000000000000000000000000000000001"
	testEventTimestamp   = int64(1000)
	testDecimalValue     = "100.5"
	testDecimalPrecision = 36
	testDecimalScale     = 18
)

var dataPointsABIArgs = mustBuildDataPointsABIArgs()

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

	// Step 1: Create primitive stream
	t.Log("Step 1: Creating primitive stream...")
	err := createPrimitiveStream(ctx, t, client, streamID.String())
	require.NoError(t, err, "Failed to create primitive stream")

	// Step 2: Insert sample data
	t.Log("Step 2: Inserting sample data...")
	err = insertStreamData(ctx, t, client, dataProviderHex, streamID.String())
	require.NoError(t, err, "Failed to insert stream data")

	// Step 3: Request attestation for get_record query
	t.Log("Step 3: Requesting attestation for get_record...")
	requestTxID, argsBytes, err := requestStreamAttestation(ctx, t, client, dataProviderHex, streamID.String())
	require.NoError(t, err, "Failed to request attestation")
	require.NotEmpty(t, requestTxID, "request_tx_id should not be empty")
	t.Logf("Attestation requested: request_tx_id=%s", requestTxID)

	// Step 4: Wait for end-of-block processing and signature generation
	t.Log("Step 4: Waiting for end-of-block processing and signature generation...")
	signatureFound := waitForSignature(ctx, t, client, requestTxID, 90*time.Second)
	require.True(t, signatureFound, "Signature not generated within expected time (90s)")

	// Step 5: Retrieve and verify signed attestation
	t.Log("Step 5: Retrieving and verifying signed attestation...")
	payload, err := getSignedAttestation(ctx, t, client, requestTxID)
	require.NoError(t, err, "Failed to get signed attestation")
	require.NotNil(t, payload, "Signed payload should not be nil")
	require.Greater(t, len(payload), 65, "Payload should contain canonical + 65-byte signature")

	// Verify signature is appended at the end
	signature := payload[len(payload)-65:]
	canonical := payload[:len(payload)-65]
	require.Len(t, signature, 65, "Signature should be exactly 65 bytes")
	require.NotEmpty(t, canonical, "Canonical payload should not be empty")

	// Decode canonical payload and validate every field matches the request
	payloadFields, err := tn_attestation.ParseCanonicalPayload(canonical)
	require.NoError(t, err, "Canonical payload should parse")
	require.Equal(t, uint8(1), payloadFields.Version, "Unexpected canonical version")
	require.Equal(t, uint8(0), payloadFields.Algorithm, "Unexpected signature algorithm")
	require.NotZero(t, payloadFields.BlockHeight, "Block height should be recorded")
	require.Equal(t, dataProviderAddr, payloadFields.DataProvider, "Data provider mismatch in canonical payload")
	require.Equal(t, []byte(streamID.String()), payloadFields.StreamID, "Stream ID mismatch in canonical payload")
	require.Equal(t, uint16(1), payloadFields.ActionID, "get_record should map to action_id 1")
	require.Equal(t, argsBytes, payloadFields.Args, "Canonical args do not match request args")

	canonicalArgs, err := tn_utils.DecodeActionArgs(payloadFields.Args)
	require.NoError(t, err, "Canonical args should decode")
	require.Len(t, canonicalArgs, 6, "Canonical args length mismatch")
	require.Equal(t, dataProviderHex, mustStringArg(t, canonicalArgs[0]), "Data provider argument mismatch")
	require.Equal(t, streamID.String(), mustStringArg(t, canonicalArgs[1]), "Stream ID argument mismatch")

	startTs := mustInt64Arg(t, canonicalArgs[2])
	require.Equal(t, testEventTimestamp, startTs, "Start timestamp mismatch")

	endTs := mustInt64Arg(t, canonicalArgs[3])
	require.Equal(t, testEventTimestamp, endTs, "End timestamp mismatch")

	require.Nil(t, canonicalArgs[4], "Filter argument should remain nil")
	require.False(t, mustBoolArg(t, canonicalArgs[5]), "use_cache must be forced false for deterministic attestation")

	// Validate the canonical result matches the inserted datapoint
	require.NotEmpty(t, payloadFields.Result, "Canonical result payload should not be empty")
	expectedDecimal, err := kwiltypes.ParseDecimalExplicit(testDecimalValue, testDecimalPrecision, testDecimalScale)
	require.NoError(t, err, "Failed to parse expected decimal value")
	require.NotNil(t, expectedDecimal, "Expected decimal must not be nil")
	require.Equal(t, uint16(testDecimalScale), expectedDecimal.Scale(), "Unexpected decimal scale")

	expectedTimestampBig := new(big.Int).SetUint64(uint64(testEventTimestamp))
	expectedValueBig := new(big.Int).Set(expectedDecimal.BigInt())
	if expectedDecimal.IsNegative() {
		expectedValueBig.Neg(expectedValueBig)
	}

	expectedPayload, err := dataPointsABIArgs.Pack([]*big.Int{expectedTimestampBig}, []*big.Int{expectedValueBig})
	require.NoError(t, err, "Failed to pack expected datapoints ABI payload")
	require.Equal(t, expectedPayload, payloadFields.Result, "Canonical result payload mismatch")

	actualTimestamps, actualValues := unpackDataPointsABI(t, payloadFields.Result)
	require.Len(t, actualTimestamps, 1, "Expected exactly one timestamp in ABI payload")
	require.Len(t, actualValues, 1, "Expected exactly one value in ABI payload")
	require.Equal(t, expectedTimestampBig, actualTimestamps[0], "Canonical result timestamp mismatch")
	require.Equal(t, 0, expectedValueBig.Cmp(actualValues[0]), "Canonical result value mismatch")

	// Verify signature authenticity and signer identity
	digest := payloadFields.SigningDigest()
	normalizedSig := append([]byte(nil), signature...)
	if normalizedSig[64] >= 27 {
		normalizedSig[64] -= 27
	}
	require.True(t, normalizedSig[64] == 0 || normalizedSig[64] == 1, "Signature recovery ID must be 0 or 1")

	publicKey, err := crypto.SigToPub(digest[:], normalizedSig)
	require.NoError(t, err, "Signature should recover public key")
	require.NotNil(t, publicKey, "Recovered public key must not be nil")

	recoveredAddr := crypto.PubkeyToAddress(*publicKey)
	require.NotEqual(t, (common.Address{}), recoveredAddr, "Recovered validator address should not be zero")
	require.True(t, crypto.VerifySignature(crypto.FromECDSAPub(publicKey), digest[:], normalizedSig[:64]), "Signature must verify against canonical digest")
	t.Logf("Validator address recovered from signature: %s", recoveredAddr.Hex())

	// Step 6: Log attestation result to console
	t.Log("=== Attestation Result ===")
	t.Logf("Signed attestation payload (hex): %s", hex.EncodeToString(payload))
	t.Logf("Payload length: %d bytes", len(payload))
	t.Logf("Canonical portion (%d bytes): %s", len(canonical), hex.EncodeToString(canonical))
	t.Logf("Signature portion (65 bytes): %s", hex.EncodeToString(signature))
	t.Log("=========================")

	t.Log("Test completed successfully!")
	t.Logf("Final payload size: %d bytes (canonical: %d, signature: 65)", len(payload), len(canonical))
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
	eventTime := testEventTimestamp
	valueDecimal, err := kwiltypes.ParseDecimalExplicit(testDecimalValue, testDecimalPrecision, testDecimalScale)
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
func requestStreamAttestation(ctx context.Context, t *testing.T, client *tnclient.Client, dataProviderHex, streamID string) (string, []byte, error) {
	argsData := []any{
		dataProviderHex,
		streamID,
		testEventTimestamp,
		testEventTimestamp,
		nil,
		false,
	}

	argsBytes, err := tn_utils.EncodeActionArgs(argsData)
	if err != nil {
		return "", nil, fmt.Errorf("failed to encode args: %w", err)
	}

	txHash, err := client.GetKwilClient().Execute(ctx, "", "request_attestation", [][]any{{
		dataProviderHex,
		streamID,
		"get_record",
		argsBytes,
		false,
		int64(0),
	}}, clienttypes.WithSyncBroadcast(true))
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

	return requestTxID, argsBytes, nil
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

	if len(result.QueryResult.Values) > 0 && len(result.QueryResult.Values[0]) > 0 {
		switch v := result.QueryResult.Values[0][0].(type) {
		case []byte:
			return append([]byte(nil), v...), nil
		case string:
			decoded, err := decodePayloadString(v)
			if err != nil {
				return nil, err
			}
			return decoded, nil
		}
	}

	resultMap := result.QueryResult.ExportToStringMap()
	if len(resultMap) == 0 {
		return nil, fmt.Errorf("no rows in result")
	}

	payloadStr, exists := resultMap[0]["payload"]
	if !exists {
		return nil, fmt.Errorf("payload column not found in result")
	}

	decoded, err := decodePayloadString(payloadStr)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// waitForTxConfirmation waits for a transaction to be confirmed
func waitForTxConfirmation(ctx context.Context, client *tnclient.Client, txHash kwiltypes.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		result, err := client.GetKwilClient().TxQuery(ctx, txHash)
		if err == nil && result != nil && result.Height > 0 {
			if result.Result == nil {
				return fmt.Errorf("transaction %s has no result payload", txHash.String())
			}
			if result.Result.Code != uint32(kwiltypes.CodeOk) {
				return fmt.Errorf("transaction %s failed: code=%d log=%s", txHash.String(), result.Result.Code, result.Result.Log)
			}
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

func decodePayloadString(raw string) ([]byte, error) {
	if raw == "" {
		return nil, fmt.Errorf("payload string is empty")
	}

	candidates := []string{
		strings.TrimPrefix(strings.TrimPrefix(raw, "\\x"), "\\\\x"),
		raw,
	}

	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		trimmed = strings.TrimPrefix(strings.TrimPrefix(trimmed, "0x"), "0X")
		if trimmed == "" {
			continue
		}

		if decoded, err := hex.DecodeString(trimmed); err == nil {
			return decoded, nil
		}
		if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
			return decoded, nil
		}
	}

	// As a last resort, treat the original string as raw bytes.
	return []byte(raw), nil
}

func mustStringArg(t *testing.T, value any) string {
	t.Helper()
	switch v := value.(type) {
	case string:
		return v
	case *string:
		require.NotNil(t, v, "string pointer argument should not be nil")
		return *v
	default:
		require.Failf(t, "unexpected string argument type", "got %T", value)
		return ""
	}
}

func mustBoolArg(t *testing.T, value any) bool {
	t.Helper()
	switch v := value.(type) {
	case bool:
		return v
	case *bool:
		require.NotNil(t, v, "bool pointer argument should not be nil")
		return *v
	default:
		require.Failf(t, "unexpected bool argument type", "got %T", value)
		return false
	}
}

func mustInt64Arg(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case int64:
		return v
	case *int64:
		require.NotNil(t, v, "int64 pointer argument should not be nil")
		return *v
	default:
		require.Failf(t, "unexpected int64 argument type", "got %T", value)
		return 0
	}
}

func mustBuildDataPointsABIArgs() gethAbi.Arguments {
	uint256Slice, err := gethAbi.NewType("uint256[]", "", nil)
	if err != nil {
		panic(fmt.Sprintf("failed to initialise uint256[] ABI type: %v", err))
	}
	int256Slice, err := gethAbi.NewType("int256[]", "", nil)
	if err != nil {
		panic(fmt.Sprintf("failed to initialise int256[] ABI type: %v", err))
	}
	return gethAbi.Arguments{
		{Type: uint256Slice},
		{Type: int256Slice},
	}
}

func unpackDataPointsABI(t *testing.T, payload []byte) ([]*big.Int, []*big.Int) {
	t.Helper()
	values, err := dataPointsABIArgs.Unpack(payload)
	require.NoError(t, err, "failed to unpack datapoints ABI payload")

	timestamps, ok := values[0].([]*big.Int)
	require.True(t, ok, "unexpected timestamps ABI type %T", values[0])

	dataValues, ok := values[1].([]*big.Int)
	require.True(t, ok, "unexpected values ABI type %T", values[1])

	return timestamps, dataValues
}
