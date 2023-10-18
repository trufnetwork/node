package transactions_test

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/kwilteam/kwil-db/core/crypto"
	"github.com/kwilteam/kwil-db/core/crypto/auth"
	"github.com/kwilteam/kwil-db/core/types/transactions"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE: this fails because of legacy issues with RLP itself. We should be aware
// of these issues with RLP (allows encoding nil fields, but cannot decode
// them). If we add the `rlp:"nil"` tag, it can decode, but it's a breaking
// change to transaction serialization.
func Test_TransactionMarshalUnmarshal(t *testing.T) {
	tx := &transactions.Transaction{}
	serialized, err := tx.MarshalBinary()
	require.NoError(t, err)

	tx2 := &transactions.Transaction{}
	err = tx2.UnmarshalBinary(serialized)
	require.Error(t, err)
}

// testing serialization of a transaction, since Luke found a bug
func Test_TransactionMarshal(t *testing.T) {
	tx := &transactions.Transaction{
		Signature: &auth.Signature{
			Signature: []byte("signature"),
			Type:      auth.EthPersonalSignAuth,
		},
		Body: &transactions.TransactionBody{
			Payload:     []byte("payload"),
			PayloadType: transactions.PayloadTypeDeploySchema,
			Fee:         big.NewInt(100),
			Nonce:       1,
			Salt:        []byte("salt"),
		},
		Sender: []byte("sender"),
	}

	serialized, err := tx.MarshalBinary()
	require.NoError(t, err)

	tx2 := &transactions.Transaction{}
	err = tx2.UnmarshalBinary(serialized)
	require.NoError(t, err)

	require.Equal(t, tx, tx2)
}

func TestTransaction_Sign(t *testing.T) {
	// secp256k1
	secp2561k1PvKeyHex := "f1aa5a7966c3863ccde3047f6a1e266cdc0c76b399e256b8fede92b1c69e4f4e"
	secp256k1PrivateKey, err := crypto.Secp256k1PrivateKeyFromHex(secp2561k1PvKeyHex)
	require.NoError(t, err, "error parse private secp2561k1PvKeyHex")

	ethPersonalSigner := auth.EthPersonalSigner{Secp256k1PrivateKey: *secp256k1PrivateKey}

	expectPersonalSignConcatSigHex := "4a8f9a2eea6fc6b6d055a13603bd9fc9495283a20d12cf44742673fb297a8f7f2b61231eeac778df354f10191562167e86275bebd55dbdfe7d2377b96e09d74901"
	expectPersonalSignConcatSigBytes, _ := hex.DecodeString(expectPersonalSignConcatSigHex)
	expectPersonalSignConcatSig := &auth.Signature{
		Signature: expectPersonalSignConcatSigBytes,
		Type:      auth.EthPersonalSignAuth,
	}

	rawPayload := transactions.ActionExecution{
		DBID:   "xf617af1ca774ebbd6d23e8fe12c56d41d25a22d81e88f67c6c6ee0d4",
		Action: "create_user",
		Arguments: [][]string{
			{"foo", "32"},
		},
	}

	payloadRLP, err := rawPayload.MarshalBinary()
	require.NoError(t, err)

	type args struct {
		mst    transactions.SignedMsgSerializationType
		signer auth.Signer
	}
	tests := []struct {
		name    string
		args    args
		wantSig *auth.Signature
		wantErr bool
	}{
		{
			name: "not support message serialization type",
			args: args{
				mst:    transactions.SignedMsgSerializationType("not support message serialization type"),
				signer: &ethPersonalSigner,
			},
			wantErr: true,
		},
		{
			name: "eth personal_sign concat string",
			args: args{
				mst:    transactions.SignedMsgConcat,
				signer: &ethPersonalSigner,
			},
			wantSig: expectPersonalSignConcatSig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tx := transactions.Transaction{
				Body: &transactions.TransactionBody{
					Payload:     payloadRLP,
					PayloadType: rawPayload.Type(),
					Fee:         big.NewInt(100),
					Nonce:       1,
					Salt:        []byte("salt"),
					Description: "By signing this message, you'll reveal your xxx to zzz",
				},
				Serialization: tt.args.mst,
			}

			err := tx.Sign(tt.args.signer)
			if tt.wantErr {
				assert.Error(t1, err, "Sign(%v)", tt.args.mst)
				return
			}

			require.NoError(t1, err, "error signing tx")
			require.Equal(t1, tt.wantSig.Type, tx.Signature.Type,
				"mismatch signature type")
			require.Equal(t1, hex.EncodeToString(tt.wantSig.Signature),
				hex.EncodeToString(tx.Signature.Signature), "mismatch signature")

			msgBts, err := tx.SerializeMsg()
			require.NoError(t1, err, "error serializing message")

			authenticator := tt.args.signer.Authenticator()
			err = authenticator.Verify(tx.Sender, msgBts, tx.Signature.Signature)
			require.NoError(t1, err, "error verifying message")
		})
	}
}

func TestTransactionBody_SerializeMsg(t *testing.T) {
	rawPayload := transactions.ActionExecution{
		DBID:   "xf617af1ca774ebbd6d23e8fe12c56d41d25a22d81e88f67c6c6ee0d4",
		Action: "create_user",
		Arguments: [][]string{
			{"foo", "32"},
		},
	}

	payload, err := rawPayload.MarshalBinary()
	require.NoError(t, err)

	defaultDescription := "By signing this message, you'll reveal your xxx to zzz"
	longDescrption := `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
`

	type args struct {
		mst         transactions.SignedMsgSerializationType
		description string
	}

	tests := []struct {
		name    string
		args    args
		wantMsg string //hex string
		wantErr bool
	}{
		{
			name: "non support message serialization type",
			args: args{
				mst:         transactions.SignedMsgSerializationType("non support message serialization type"),
				description: defaultDescription,
			},
			wantMsg: "",
			wantErr: true,
		},
		{
			name: "description too long",
			args: args{
				mst:         transactions.SignedMsgConcat,
				description: longDescrption,
			},
			wantMsg: "",
			wantErr: true,
		},
		{
			name: "concat string",
			args: args{
				mst:         transactions.SignedMsgConcat,
				description: defaultDescription,
			},
			wantMsg: "4279207369676e696e672074686973206d6573736167652c20796f75276c6c2072657665616c20796f75722078787820746f207a7a7a0a0a5061796c6f6164547970653a20657865637574655f616374696f6e0a5061796c6f61644469676573743a20386531326432386530313665316139306331386662333037316331316137663038306462383764330a4665653a203130300a4e6f6e63653a20310a53616c743a2037333631366337340a0a4b77696c20f09f968b0a",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			txBody := &transactions.TransactionBody{
				Payload:     payload,
				PayloadType: rawPayload.Type(),
				Fee:         big.NewInt(100),
				Nonce:       1,
				Salt:        []byte("salt"),
				Description: tt.args.description,
			}

			got, err := txBody.SerializeMsg(tt.args.mst)
			if tt.wantErr { // TODO: verify expect error
				assert.Error(t1, err, "SerializeMsg(%v)", tt.args.mst)
				return
			}

			assert.NoError(t1, err, "SerializeMsg(%v)", tt.args.mst)
			assert.Equalf(t1, tt.wantMsg, hex.EncodeToString(got), "SerializeMsg(%v)", tt.args.mst)
			fmt.Printf("msg to sign: \n%s\n", string(got))
		})
	}
}