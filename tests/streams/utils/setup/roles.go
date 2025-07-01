package setup

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/util"
)

type CreateTestRoleInput struct {
	Platform    *kwilTesting.Platform
	Owner       string
	RoleName    string
	DisplayName string
}

func CreateTestRole(ctx context.Context, input CreateTestRoleInput) error {
	var callerAddr string
	if input.Owner == "system" {
		callerAddr = "0x0000000000000000000000000000000000000000"
	} else {
		// For user-owned roles, the caller must be the address of the signer.
		// The platform passed in should already have the correct signer.
		deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "invalid deployer on platform for CreateTestRole")
		}
		callerAddr = deployer.Address()

		// Also assert that the owner parameter matches the signer's address.
		if !strings.EqualFold(callerAddr, input.Owner) {
			return errors.Errorf("owner parameter (%s) does not match platform signer address (%s)", input.Owner, callerAddr)
		}
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         input.Platform.Txid(),
		Signer:       input.Platform.Deployer,
		Caller:       callerAddr,
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Override authorization for system operations
	}

	roleType := "user"
	if input.Owner == "system" {
		roleType = "system"
	}

	insertRoleSQL := `
		INSERT INTO roles (owner, role_name, display_name, role_type, created_at)
		VALUES ($owner, $role_name, $display_name, $role_type, 0)
		ON CONFLICT (owner, role_name) DO NOTHING
	`

	return input.Platform.Engine.Execute(engineContext, input.Platform.DB, insertRoleSQL, map[string]any{
		"$owner":        strings.ToLower(input.Owner),
		"$role_name":    strings.ToLower(input.RoleName),
		"$display_name": input.DisplayName,
		"$role_type":    roleType,
	}, func(row *common.Row) error {
		return nil
	})
}

func AddMemberToRoleBypass(ctx context.Context, platform *kwilTesting.Platform, owner, roleName, wallet string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         platform.Txid(),
		Signer:       []byte("system"),
		Caller:       "0x0000000000000000000000000000000000000000",
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	sql := `INSERT INTO role_members (owner, role_name, wallet, granted_at, granted_by)
			VALUES ($owner, $role_name, $wallet, 0, 'system')
			ON CONFLICT (owner, role_name, wallet) DO NOTHING`

	return platform.Engine.Execute(engineContext, platform.DB, sql, map[string]any{
		"$owner":     strings.ToLower(owner),
		"$role_name": strings.ToLower(roleName),
		"$wallet":    strings.ToLower(wallet),
	}, func(row *common.Row) error {
		return nil
	})
}

func SetRoleManagerBypass(ctx context.Context, platform *kwilTesting.Platform, targetOwner, targetRole, managerOwner, managerRole string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         platform.Txid(),
		Signer:       []byte("system"),
		Caller:       "0x0000000000000000000000000000000000000000",
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	sql := `UPDATE roles SET manager_owner = $manager_owner, manager_role_name = $manager_role_name WHERE owner = $owner AND role_name = $role_name`

	return platform.Engine.Execute(engineContext, platform.DB, sql, map[string]any{
		"$manager_owner":     strings.ToLower(managerOwner),
		"$manager_role_name": strings.ToLower(managerRole),
		"$owner":             strings.ToLower(targetOwner),
		"$role_name":         strings.ToLower(targetRole),
	}, func(row *common.Row) error {
		return nil
	})
}
