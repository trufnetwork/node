package procedure

import (
	"context"
	"fmt"

	"github.com/kwilteam/kwil-db/common"
)

func GrantRoles(ctx context.Context, input GrantRolesInput) error {
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "grant_roles", []any{input.Owner, input.RoleName, input.Wallets}, func(row *common.Row) error {
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

func RevokeRoles(ctx context.Context, input RevokeRolesInput) error {
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "revoke_roles", []any{input.Owner, input.RoleName, input.Wallets}, func(row *common.Row) error {
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

func AreMembersOf(ctx context.Context, input AreMembersOfInput) (map[string]bool, error) {
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

	results := make(map[string]bool)
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "are_members_of", []any{input.Owner, input.RoleName, input.Wallets}, func(row *common.Row) error {
		var wallet string
		var isMember bool

		if w, ok := row.Values[0].(string); ok {
			wallet = w
		} else {
			return fmt.Errorf("expected string for wallet, got %T", row.Values[0])
		}

		if m, ok := row.Values[1].(bool); ok {
			isMember = m
		} else {
			return fmt.Errorf("expected bool for is_member, got %T", row.Values[1])
		}

		results[wallet] = isMember
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, r.Error
	}

	return results, nil
}

func AddRoleManagers(ctx context.Context, input AddRoleManagersInput) error {
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "add_role_managers", []any{input.OwnerAddress, input.RoleName, input.ManagerWallets}, func(row *common.Row) error {
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

func RemoveRoleManagers(ctx context.Context, input RemoveRoleManagersInput) error {
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "remove_role_managers", []any{input.OwnerAddress, input.RoleName, input.ManagerWallets}, func(row *common.Row) error {
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
