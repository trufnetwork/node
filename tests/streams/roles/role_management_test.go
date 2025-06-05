package tests

import (
	"context"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
)

// Test addresses - using valid Ethereum address format
const (
	roleOwner          = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d4e"
	wallet1            = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d41"
	wallet2            = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d42"
	wallet3            = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d43"
	unauthorizedWallet = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d44"
	managerWallet      = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d45"
)

type roleTestCase struct {
	name string
	run  func(t *testing.T, ctx context.Context, platform *kwilTesting.Platform)
}

func TestRoleManagementSuite(t *testing.T) {
	testCases := []roleTestCase{
		{
			name: "ROLE01_RoleManagement",
			run:  testRoleManagement,
		},
		{
			name: "ROLE02_PermissionValidation",
			run:  testPermissionValidation,
		},
		{
			name: "ROLE03_InvalidInputValidation",
			run:  testInvalidInputValidation,
		},
		{
			name: "ROLE04_RoleManagerPermissions",
			run:  testRoleManagerPermissions,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
				Name:        tc.name,
				SeedScripts: migrations.GetSeedScriptPaths(),
				FunctionTests: []kwilTesting.TestFunc{
					func(ctx context.Context, platform *kwilTesting.Platform) error {
						tc.run(t, ctx, platform)
						return nil // Errors are handled by require/assert
					},
				},
			}, testutils.GetTestOptions())
		})
	}
}

func testRoleManagement(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = "test_role"
	// Setup: Create a test role first
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, DisplayName: "Test Role"})
	require.NoError(t, err)

	// Test 1: Grant role to wallet1
	t.Run("Grant role to wallet1", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.NoError(t, err)

		isMember, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.NoError(t, err)
		require.True(t, isMember, "Wallet1 should be a member after grant")
	})

	// Test 2: Grant same role again (should be idempotent)
	t.Run("Grant same role again should be idempotent", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.NoError(t, err)

		isMember, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.NoError(t, err)
		require.True(t, isMember, "Wallet1 should still be a member")
	})

	// Test 3: Grant role to multiple wallets
	t.Run("Grant role to multiple wallets", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet2})
		require.NoError(t, err)

		err = procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet3})
		require.NoError(t, err)

		isMember2, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet2})
		require.NoError(t, err)
		require.True(t, isMember2)

		isMember3, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet3})
		require.NoError(t, err)
		require.True(t, isMember3)
	})

	// Test 4: Revoke role from wallet2
	t.Run("Revoke role from wallet2", func(t *testing.T) {
		err := procedure.RevokeRole(ctx, procedure.RevokeRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet2})
		require.NoError(t, err)

		isMember, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet2})
		require.NoError(t, err)
		require.False(t, isMember, "Wallet2 should not be a member after revoke")

		// Verify other wallets are still members
		isMember1, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.NoError(t, err)
		require.True(t, isMember1, "Wallet1 should still be a member")

		isMember3, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet3})
		require.NoError(t, err)
		require.True(t, isMember3, "Wallet3 should still be a member")
	})
}

func testPermissionValidation(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = "permission_test_role"
	// Setup: Create a test role
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, DisplayName: "Permission Test Role"})
	require.NoError(t, err)

	t.Run("Unauthorized grant should fail", func(t *testing.T) {
		unauthPlatform := procedure.WithSigner(platform, []byte(unauthorizedWallet))
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: unauthPlatform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Only role owner or a manager can grant roles")
	})

	t.Run("Unauthorized revoke should fail", func(t *testing.T) {
		// First grant the role as the owner
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.NoError(t, err)

		// Try to revoke as unauthorized user
		unauthPlatform := procedure.WithSigner(platform, []byte(unauthorizedWallet))
		err = procedure.RevokeRole(ctx, procedure.RevokeRoleInput{Platform: unauthPlatform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Only role owner or a manager can revoke roles")
	})

	t.Run("Check membership for non-existent role should fail", func(t *testing.T) {
		_, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: "non_existent_role", Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Role does not exist")
	})
}

func testInvalidInputValidation(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = "input_test_role"
	// Create a test role for valid operations
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, DisplayName: "Input Test Role"})
	require.NoError(t, err)

	t.Run("Invalid wallet address should fail", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: "invalid_wallet"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Invalid wallet address")

		_, err = procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: "invalid_wallet"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Invalid wallet address")
	})

	t.Run("Invalid owner address should fail", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: "invalid_owner", RoleName: roleName, Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Invalid owner address")

		_, err = procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: "invalid_owner", RoleName: roleName, Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Invalid owner address")
	})

	t.Run("Grant to non-existent role should fail", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: platform, Owner: roleOwner, RoleName: "non_existent_role", Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Role does not exist")
	})

	t.Run("Revoke from non-member should fail", func(t *testing.T) {
		// Ensure wallet1 is not a member for this test
		err := procedure.RevokeRole(ctx, procedure.RevokeRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		if err != nil { // It's okay if it fails because it's already not a member
			require.Contains(t, err.Error(), "Wallet is not a member of role")
		}

		err = procedure.RevokeRole(ctx, procedure.RevokeRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallet: wallet1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Wallet is not a member of role")
	})
}

func testRoleManagerPermissions(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const managedRoleName = "managed_role"

	// Setup: Create a role and assign a manager to it.
	// We use 'system' as the owner here to test the specific and important use case
	// where management MUST be delegated, as 'system' cannot sign transactions itself.
	// The underlying manager permission logic is generic for any role.
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{
		Platform:    platform,
		Owner:       "system",
		RoleName:    managedRoleName,
		DisplayName: "A Managed Role",
	})
	require.NoError(t, err)

	err = setup.AddManagerToRole(ctx, setup.AddManagerToRoleInput{
		Platform:      platform,
		Owner:         "system",
		RoleName:      managedRoleName,
		ManagerWallet: managerWallet,
	})
	require.NoError(t, err)

	managerPlatform := procedure.WithSigner(platform, []byte(managerWallet))

	t.Run("Manager can grant a role", func(t *testing.T) {
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{
			Platform: managerPlatform,
			Owner:    "system",
			RoleName: managedRoleName,
			Wallet:   wallet1,
		})
		require.NoError(t, err)

		// Verify membership
		isMember, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: "system", RoleName: managedRoleName, Wallet: wallet1})
		require.NoError(t, err)
		require.True(t, isMember, "Wallet1 should be a member after manager grant")
	})

	t.Run("Manager can revoke a role", func(t *testing.T) {
		// First ensure the member exists
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{Platform: managerPlatform, Owner: "system", RoleName: managedRoleName, Wallet: wallet1})
		require.NoError(t, err)

		// Now revoke
		err = procedure.RevokeRole(ctx, procedure.RevokeRoleInput{
			Platform: managerPlatform,
			Owner:    "system",
			RoleName: managedRoleName,
			Wallet:   wallet1,
		})
		require.NoError(t, err)

		// Verify revoked
		isMember, err := procedure.IsMemberOf(ctx, procedure.IsMemberOfInput{Platform: platform, Owner: "system", RoleName: managedRoleName, Wallet: wallet1})
		require.NoError(t, err)
		require.False(t, isMember, "Wallet1 should not be a member after manager revoke")
	})

	t.Run("Non-manager cannot grant a role", func(t *testing.T) {
		unauthPlatform := procedure.WithSigner(platform, []byte(unauthorizedWallet))
		err := procedure.GrantRole(ctx, procedure.GrantRoleInput{
			Platform: unauthPlatform,
			Owner:    "system",
			RoleName: managedRoleName,
			Wallet:   wallet2,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Only role owner or a manager can grant roles")
	})
}
