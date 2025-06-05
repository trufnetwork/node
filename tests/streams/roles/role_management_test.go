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

// Role and error message constants to avoid magic strings and improve maintainability.
const (
	// Role Names
	roleMgmtRole       = "test_role"
	permissionTestRole = "permission_test_role"
	inputTestRole      = "input_test_role"
	managedRole        = "managed_role"
	batchOpRole        = "batch_op_role"
	batchManagerRole   = "batch_manager_role"
	nonExistentRole    = "non_existent_role"

	// Special Owners
	systemOwner = "system"

	// Error Messages
	errOnlyOwnerOrManager   = "Only role owner or a manager can"
	errRoleDoesNotExist     = "Role does not exist"
	errInvalidWalletAddress = "Invalid wallet address"
	errInvalidOwnerAddress  = "Invalid owner address"
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
		{
			name: "ROLE05_BatchOperations",
			run:  testBatchOperations,
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
	const roleName = roleMgmtRole
	// Setup: Create a test role first
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, DisplayName: "Test Role"})
	require.NoError(t, err)

	// Test 1: Grant role to wallet1
	t.Run("Grant role to wallet1", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should be a member after grant")
	})

	// Test 2: Grant same role again (should be idempotent)
	t.Run("Grant same role again should be idempotent", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should still be a member")
	})

	// Test 3: Grant role to multiple wallets
	t.Run("Grant role to multiple wallets", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2, wallet3}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2, wallet3}})
		require.NoError(t, err)
		require.True(t, members[wallet2])
		require.True(t, members[wallet3])
	})

	// Test 4: Revoke role from wallet2
	t.Run("Revoke role from wallet2", func(t *testing.T) {
		err := procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet2, wallet3}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should still be a member")
		require.False(t, members[wallet2], "Wallet2 should not be a member after revoke")
		require.True(t, members[wallet3], "Wallet3 should still be a member")
	})
}

func testPermissionValidation(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = permissionTestRole
	// Setup: Create a test role
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, DisplayName: "Permission Test Role"})
	require.NoError(t, err)

	t.Run("Unauthorized grant should fail", func(t *testing.T) {
		unauthPlatform := procedure.WithSigner(platform, []byte(unauthorizedWallet))
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: unauthPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errOnlyOwnerOrManager)
	})

	t.Run("Unauthorized revoke should fail", func(t *testing.T) {
		// First grant the role as the owner
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		// Try to revoke as unauthorized user
		unauthPlatform := procedure.WithSigner(platform, []byte(unauthorizedWallet))
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: unauthPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errOnlyOwnerOrManager)
	})

	t.Run("Check membership for non-existent role should fail", func(t *testing.T) {
		_, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: nonExistentRole, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errRoleDoesNotExist)
	})
}

func testInvalidInputValidation(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = inputTestRole
	// Create a test role for valid operations
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: roleOwner, RoleName: roleName, DisplayName: "Input Test Role"})
	require.NoError(t, err)

	t.Run("Invalid wallet address should fail", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{"invalid_wallet"}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errInvalidWalletAddress)

		_, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{"invalid_wallet"}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errInvalidWalletAddress)
	})

	t.Run("Invalid owner address should fail", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: "invalid_owner", RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errInvalidOwnerAddress)

		_, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: "invalid_owner", RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errInvalidOwnerAddress)
	})

	t.Run("Grant to non-existent role should fail", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: platform, Owner: roleOwner, RoleName: nonExistentRole, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.Contains(t, err.Error(), errRoleDoesNotExist)
	})

	t.Run("Revoke from non-member should be idempotent", func(t *testing.T) {
		// Ensure wallet1 is not a member for this test by revoking it first (it's ok if it's already not a member)
		err := procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		// Revoke again, should not fail
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err, "Revoking a non-member should be idempotent and not return an error")
	})
}

func testRoleManagerPermissions(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const managedRoleName = managedRole

	// Setup: Create a role and assign a manager to it.
	// We use 'system' as the owner here to test the specific and important use case
	// where management MUST be delegated, as 'system' cannot sign transactions itself.
	// The underlying manager permission logic is generic for any role.
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{
		Platform:    platform,
		Owner:       systemOwner,
		RoleName:    managedRoleName,
		DisplayName: "A Managed Role",
	})
	require.NoError(t, err)

	err = setup.AddManagersToRole(ctx, setup.AddManagersToRoleInput{
		Platform:       platform,
		Owner:          systemOwner,
		RoleName:       managedRoleName,
		ManagerWallets: []string{managerWallet},
	})
	require.NoError(t, err)

	managerPlatform := procedure.WithSigner(platform, []byte(managerWallet))

	t.Run("Manager can grant a role", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: managerPlatform,
			Owner:    systemOwner,
			RoleName: managedRoleName,
			Wallets:  []string{wallet1},
		})
		require.NoError(t, err)

		// Verify membership
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: systemOwner, RoleName: managedRoleName, Wallets: []string{wallet1}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should be a member after manager grant")
	})

	t.Run("Manager can revoke a role", func(t *testing.T) {
		// First ensure the member exists
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: managerPlatform, Owner: systemOwner, RoleName: managedRoleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		// Now revoke
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{
			Platform: managerPlatform,
			Owner:    systemOwner,
			RoleName: managedRoleName,
			Wallets:  []string{wallet1},
		})
		require.NoError(t, err)

		// Verify revoked
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: systemOwner, RoleName: managedRoleName, Wallets: []string{wallet1}})
		require.NoError(t, err)
		require.False(t, members[wallet1], "Wallet1 should not be a member after manager revoke")
	})

	t.Run("Non-manager cannot grant a role", func(t *testing.T) {
		unauthPlatform := procedure.WithSigner(platform, []byte(unauthorizedWallet))
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: unauthPlatform,
			Owner:    systemOwner,
			RoleName: managedRoleName,
			Wallets:  []string{wallet2},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), errOnlyOwnerOrManager)
	})
}

func testBatchOperations(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = batchOpRole
	const managerRoleName = batchManagerRole

	// Create a platform signed by the role owner to ensure permissions are correct for subsequent operations.
	// This isolates the test from state leaked from previous test cases that modify the platform's signer.
	ownerPlatform := procedure.WithSigner(platform, []byte(roleOwner))

	// Setup: Create test roles. These are created once in the parent transaction.
	// Sub-tests will inherit this state but their own changes will be rolled back.
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, DisplayName: "Batch Ops Role"})
	require.NoError(t, err)

	err = setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: systemOwner, RoleName: managerRoleName, DisplayName: "Batch Manager Role"})
	require.NoError(t, err)

	err = setup.AddManagersToRole(ctx, setup.AddManagersToRoleInput{
		Platform:       platform,
		Owner:          systemOwner,
		RoleName:       managerRoleName,
		ManagerWallets: []string{managerWallet},
	})
	require.NoError(t, err)

	// Each sub-test is wrapped in withTx to ensure it runs in its own isolated transaction.
	t.Run("Grant and revoke empty list of wallets should succeed", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{}})
		require.NoError(t, err, "Granting to an empty list should not fail")

		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{}})
		require.NoError(t, err, "Revoking an empty list should not fail")

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{}})
		require.NoError(t, err)
		require.Empty(t, members, "Checking membership of empty list should return empty map")
	}))

	t.Run("Grant batch and revoke partial batch", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Grant to three wallets
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet2, wallet3}})
		require.NoError(t, err)

		// Revoke two of them
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet3}})
		require.NoError(t, err)

		// Check membership
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet2, wallet3}})
		require.NoError(t, err)
		require.False(t, members[wallet1], "Wallet1 should have been revoked")
		require.True(t, members[wallet2], "Wallet2 should still be a member")
		require.False(t, members[wallet3], "Wallet3 should have been revoked")
	}))

	t.Run("Idempotent batch grant with overlap", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Grant to wallet1, wallet2
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet2}})
		require.NoError(t, err)

		// Grant again to wallet2, wallet3
		err = procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2, wallet3}})
		require.NoError(t, err)

		// Check membership
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: txPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet2, wallet3}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should be a member")
		require.True(t, members[wallet2], "Wallet2 should be a member")
		require.True(t, members[wallet3], "Wallet3 should be a member")
	}))

	t.Run("Manager can grant and revoke batch", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		managerPlatform := procedure.WithSigner(txPlatform, []byte(managerWallet))

		// Manager grants roles to a batch
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: managerPlatform,
			Owner:    systemOwner,
			RoleName: managerRoleName,
			Wallets:  []string{wallet1, wallet2},
		})
		require.NoError(t, err)

		// Manager revokes one of them
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{
			Platform: managerPlatform,
			Owner:    systemOwner,
			RoleName: managerRoleName,
			Wallets:  []string{wallet2},
		})
		require.NoError(t, err)

		// Verify final state
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: txPlatform, Owner: systemOwner, RoleName: managerRoleName, Wallets: []string{wallet1, wallet2}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should remain a member after manager's batch operations")
		require.False(t, members[wallet2], "Wallet2 should have been revoked by manager in a batch operation")
	}))

	t.Run("Duplicate wallets in batch array should be handled gracefully", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Test grant with duplicates - should not fail and should grant the role once
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: txPlatform,
			Owner:    roleOwner,
			RoleName: roleName,
			Wallets:  []string{wallet1, wallet1, wallet2, wallet1},
		})
		require.NoError(t, err, "Grant with duplicate wallets should not fail")

		// Verify each wallet is a member exactly once (no duplicate entries)
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{
			Platform: txPlatform,
			Owner:    roleOwner,
			RoleName: roleName,
			Wallets:  []string{wallet1, wallet2},
		})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should be a member despite duplicates in array")
		require.True(t, members[wallet2], "Wallet2 should be a member")

		// Test revoke with duplicates - should not fail and should revoke the role once
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{
			Platform: txPlatform,
			Owner:    roleOwner,
			RoleName: roleName,
			Wallets:  []string{wallet1, wallet1, wallet1},
		})
		require.NoError(t, err, "Revoke with duplicate wallets should not fail")

		// Verify wallet1 is actually revoked
		members, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{
			Platform: txPlatform,
			Owner:    roleOwner,
			RoleName: roleName,
			Wallets:  []string{wallet1, wallet2},
		})
		require.NoError(t, err)
		require.False(t, members[wallet1], "Wallet1 should be revoked despite duplicates in revoke array")
		require.True(t, members[wallet2], "Wallet2 should still be a member")

		// Test membership check with duplicates
		members, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{
			Platform: txPlatform,
			Owner:    roleOwner,
			RoleName: roleName,
			Wallets:  []string{wallet2, wallet2, wallet3, wallet2},
		})
		require.NoError(t, err, "Membership check with duplicates should not fail")
		require.True(t, members[wallet2], "Wallet2 membership should be correctly reported")
		require.False(t, members[wallet3], "Wallet3 should not be a member")
	}))
}
