package setup

import (
	"context"
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
)

type CreateTestRoleInput struct {
	Platform    *kwilTesting.Platform
	Owner       string
	RoleName    string
	DisplayName string
}

func CreateTestRole(ctx context.Context, input CreateTestRoleInput) error {
	input.Platform.Deployer = []byte(input.Owner)

	var callerAddr string
	if input.Owner == "system" {
		callerAddr = "0x0000000000000000000000000000000000000000"
	} else {
		callerAddr = input.Owner
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 0,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: callerAddr,
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Override authorization for system operations
		InvalidTxCtx:  false,
	}

	// Determine role type based on owner
	roleType := "user"
	if input.Owner == "system" {
		roleType = "system"
	}

	// Use direct SQL execution to insert the role - no parameter binding, use direct values
	insertRoleSQL := fmt.Sprintf(`
		INSERT INTO roles (owner, role_name, display_name, role_type, created_at)
		VALUES ('%s', '%s', '%s', '%s', 0)
		ON CONFLICT (owner, role_name) DO NOTHING
	`, strings.ToLower(input.Owner), input.RoleName, input.DisplayName, roleType)

	return input.Platform.Engine.Execute(engineContext, input.Platform.DB, insertRoleSQL, nil, func(row *common.Row) error {
		return nil
	})
}

type AddManagersToRoleInput struct {
	Platform       *kwilTesting.Platform
	Owner          string
	RoleName       string
	ManagerWallets []string
}

// AddManagersToRole uses system privileges to directly insert managers for a role.
// This is a test utility for setting up state.
func AddManagersToRole(ctx context.Context, input AddManagersToRoleInput) error {
	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 0,
		},
		TxID:   input.Platform.Txid(),
		Signer: []byte("system"),                             // Execute as system
		Caller: "0x0000000000000000000000000000000000000000", // System's zero address
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
		InvalidTxCtx:  false,
	}

	for _, managerWallet := range input.ManagerWallets {
		insertManagerSQL := fmt.Sprintf(`
			INSERT INTO role_managers (owner, role_name, manager_wallet, assigned_at, assigned_by)
			VALUES ('%s', '%s', '%s', 0, 'system')
			ON CONFLICT (owner, role_name, manager_wallet) DO NOTHING
		`, strings.ToLower(input.Owner), input.RoleName, strings.ToLower(managerWallet))

		err := input.Platform.Engine.Execute(engineContext, input.Platform.DB, insertManagerSQL, nil, func(row *common.Row) error {
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
