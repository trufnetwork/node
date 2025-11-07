package testctx

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	defaultLeaderOnce sync.Once
	defaultLeaderPub  *crypto.Secp256k1PublicKey
)

func defaultLeaderPublicKey() *crypto.Secp256k1PublicKey {
	defaultLeaderOnce.Do(func() {
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		if err != nil {
			panic(fmt.Sprintf("failed to generate default leader key: %v", err))
		}
		var ok bool
		defaultLeaderPub, ok = pubGeneric.(*crypto.Secp256k1PublicKey)
		if !ok {
			panic("default leader public key is not secp256k1")
		}
	})
	return defaultLeaderPub
}

// NewTxContextWithAuth constructs a TxContext that includes a deterministic leader proposer.
func NewTxContextWithAuth(ctx context.Context, platform *kwilTesting.Platform, signer []byte, caller string, authenticator string, height int64) *common.TxContext {
	if height < 0 {
		height = 1
	}
	return &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   height,
			Proposer: defaultLeaderPublicKey(),
		},
		Signer:        signer,
		Caller:        caller,
		TxID:          platform.Txid(),
		Authenticator: authenticator,
	}
}

// NewEngineContext returns an EngineContext configured for an Ethereum address signer.
func NewEngineContext(ctx context.Context, platform *kwilTesting.Platform, addr util.EthereumAddress, height int64) *common.EngineContext {
	txContext := NewTxContextWithAuth(ctx, platform, addr.Bytes(), addr.Address(), coreauth.EthPersonalSignAuth, height)
	return &common.EngineContext{TxContext: txContext}
}
