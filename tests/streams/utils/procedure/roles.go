package procedure

import (
	"context"
	"fmt"

	"github.com/kwilteam/kwil-db/common"
	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/util"
)

func GrantRoles(ctx context.Context, input GrantRolesInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes for GrantRoles")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
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
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes for RevokeRoles")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
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
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ethereum address from deployer bytes for AreMembersOf")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
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

func SetRoleManager(ctx context.Context, input SetRoleManagerInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes for SetRoleManager")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "set_role_manager", []any{input.Owner, input.RoleName, input.ManagerOwner, input.ManagerRoleName}, func(row *common.Row) error {
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

func ListRoleMembers(ctx context.Context, input ListRoleMembersInput) ([]string, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ethereum address from deployer bytes for ListRoleMembers")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
	}

	engineContext := &common.EngineContext{TxContext: txContext}

	// capture wallets
	var wallets []string

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "list_role_members", []any{input.Owner, input.RoleName, nil, nil}, func(row *common.Row) error {
		if len(row.Values) < 1 {
			return fmt.Errorf("unexpected row values length: %d", len(row.Values))
		}
		w, ok := row.Values[0].(string)
		if !ok {
			return fmt.Errorf("expected wallet string, got %T", row.Values[0])
		}
		wallets = append(wallets, w)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, r.Error
	}

	return wallets, nil
}
