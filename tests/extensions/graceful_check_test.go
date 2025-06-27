package extensions_test

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/client"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/core/types/jsonrpc"
	"github.com/trufnetwork/node/tests/utils"
)

// This test requires a pre-built docker image of the custom kwild node from this (tsn-db) repo.
// For example, build with: `docker build -t tsn-kwild:latest -f ../../deployments/Dockerfile ../..`
const kwilImage = "tsn-kwild:latest"

func TestGracefulCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1. Setup the test fixture with a single kwild node.
	// We need a key for the database owner.
	ownerPrivKey, err := crypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	ownerIdent, err := types.NewAccountIdentifier(ownerPrivKey.PubKey().Bytes(), ownerPrivKey.PubKey().Type())
	require.NoError(t, err)

	fixture := utils.NewLocalNodeFixture(t)
	cfg := &utils.KwilNodeConfig{
		DBOwnerPubKey: hex.EncodeToString(ownerIdent),
	}
	err = fixture.Setup(ctx, kwilImage, cfg)
	require.NoError(t, err)
	defer fixture.Teardown(ctx)

	// 2. Create a client to interact with the kwild node.
	c, err := client.NewClient(ctx, fixture.KwilProvider, &client.Options{})
	require.NoError(t, err)

	// 3. Define and deploy the schema that uses the `hello` extension.
	schemaContent := `
DATABASE graceful_db;
USE hello AS hello;

CREATE ACTION graceful_check() PUBLIC VIEW RETURNS (msg TEXT) {
    RETURN hello.greet();
}
`
	schema := &types.Schema{}
	err = schema.UnmarshalText([]byte(schemaContent))
	require.NoError(t, err)
	schema.Owner = ownerIdent

	// The client needs a signer to send transactions.
	signer, err := crypto.NewSigner(ownerPrivKey)
	require.NoError(t, err)

	// Deploying a schema is a transaction, so it needs to be signed.
	txHash, err := c.Deploy(ctx, schema, client.WithSigner(signer))
	require.NoError(t, err)

	// Wait for the deployment transaction to be confirmed.
	res, err := c.WaitTx(ctx, txHash, 10*time.Second)
	require.NoError(t, err)
	require.EqualValues(t, jsonrpc.TxSuccess, res.Code, "deployment tx failed with log: %s", res.Log)

	dbid := schema.DBID()

	// 4. Call the `graceful_check` action.
	// Since it's a VIEW action, we can use `Call`.
	callRes, err := c.Call(ctx, &jsonrpc.CallRequest{
		Body: &jsonrpc.CallRequest_ActionCall{
			ActionCall: &jsonrpc.ActionCallRequest{
				DBID:   dbid,
				Action: "graceful_check",
			},
		},
	})
	require.NoError(t, err)
	require.Empty(t, callRes.Error, "call resulted in an error: %s", callRes.Error)

	// 5. Assert the result.
	// We expect the `hello.greet()` to return "Hello from custom extension!".
	require.NotNil(t, callRes.Result)
	records, err := types.ParseResult(callRes.Result)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Contains(t, records[0], "msg")
	require.Equal(t, "Hello from custom extension!", records[0]["msg"])

	t.Log("Successfully called action using custom extension.")
}
