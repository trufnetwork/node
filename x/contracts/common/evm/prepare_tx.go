package evm

import (
	"context"
	"crypto/ecdsa"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ec "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

func PrepareTxAuth(ctx context.Context, c *ethclient.Client, chainId *big.Int, privateKey *ecdsa.PrivateKey) (*bind.TransactOpts, error) {
	addr := ec.PubkeyToAddress(privateKey.PublicKey)

	// get pending nonce
	nonce, err := c.PendingNonceAt(ctx, addr)
	if err != nil {
		return nil, err
	}

	// create new auth
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainId)
	if err != nil {
		return nil, err
	}

	// set values
	auth.Nonce = big.NewInt(int64(nonce))

	// suggest gas
	gasPrice, err := c.SuggestGasPrice(context.Background())
	if err != nil {
		return nil, err
	}

	auth.Value = big.NewInt(0)     // in wei
	auth.GasLimit = uint64(300000) // in units
	auth.GasPrice = gasPrice

	return auth, nil
}