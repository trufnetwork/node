package procedure

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
)

func GrantRole(ctx context.Context, input GrantRoleInput) error {
	caller := string(input.Platform.Deployer)

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       caller,
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "grant_role", []any{input.Owner, input.RoleName, input.Wallet}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if r.Error != nil {
		return r.Error
	}
	return nil
}

func RevokeRole(ctx context.Context, input RevokeRoleInput) error {
	caller := string(input.Platform.Deployer)

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       caller,
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "revoke_role", []any{input.Owner, input.RoleName, input.Wallet}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if r.Error != nil {
		return r.Error
	}
	return nil
}

func IsMemberOf(ctx context.Context, input IsMemberOfInput) (bool, error) {
	caller := string(input.Platform.Deployer)

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       caller,
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_member_of", []any{input.Owner, input.RoleName, input.Wallet}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return false, err
	}
	if r.Error != nil {
		return false, r.Error
	}

	if len(resultRows) == 0 {
		return false, nil
	}

	if len(resultRows[0]) > 0 {
		if isMember, ok := resultRows[0][0].(bool); ok {
			return isMember, nil
		}
	}

	return false, nil
}
