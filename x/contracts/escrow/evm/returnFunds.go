package evm

import (
	"context"
	"kwil/x/types/contracts/escrow"

	kwilCommon "kwil/x/contracts/common/evm"

	"github.com/ethereum/go-ethereum/common"
)

// ReturnFunds calls the returnDeposit function
func (c *contract) ReturnFunds(ctx context.Context, params *escrow.ReturnFundsParams) (*escrow.ReturnFundsResponse, error) {

	auth, err := kwilCommon.PrepareTxAuth(ctx, c.client, c.chainId, c.privateKey)
	if err != nil {
		return nil, err
	}

	res, err := c.ctr.ReturnDeposit(auth, common.HexToAddress(params.Recipient), params.Amount, params.Fee, params.CorrelationId)
	if err != nil {
		return nil, err
	}

	return &escrow.ReturnFundsResponse{
		TxHash: res.Hash().String(),
	}, nil
}