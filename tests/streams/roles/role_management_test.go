package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
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
	errOnlyOwnerOrManager   = "is not the owner or a member of the manager role for"
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
		{
			name: "ROLE06_SetRoleManager",
			run:  testSetRoleManager,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
				Name:        tc.name,
				SeedScripts: migrations.GetSeedScriptPaths(),
				FunctionTests: []kwilTesting.TestFunc{
					func(ctx context.Context, platform *kwilTesting.Platform) error {
						// Initialize platform deployer with a valid address to prevent errors in procedure calls.
						defaultDeployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
						platform.Deployer = defaultDeployer.Bytes()

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
	// Create a dedicated platform for the role owner
	ownerAddr := util.Unsafe_NewEthereumAddressFromString(roleOwner)
	ownerPlatform := procedure.WithSigner(platform, ownerAddr.Bytes())

	// Setup: Create a test role first using the owner's platform
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, DisplayName: "Test Role"})
	require.NoError(t, err)

	// Test 1: Grant role to wallet1
	t.Run("Grant role to wallet1", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should be a member after grant")
	})

	// Test 2: Grant same role again (should be idempotent)
	t.Run("Grant same role again should be idempotent", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should still be a member")
	})

	// Test 3: Grant role to multiple wallets
	t.Run("Grant role to multiple wallets", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2, wallet3}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2, wallet3}})
		require.NoError(t, err)
		require.True(t, members[wallet2])
		require.True(t, members[wallet3])
	})

	// Test 4: Revoke role from wallet2
	t.Run("Revoke role from wallet2", func(t *testing.T) {
		err := procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet2}})
		require.NoError(t, err)

		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1, wallet2, wallet3}})
		require.NoError(t, err)
		require.True(t, members[wallet1], "Wallet1 should still be a member")
		require.False(t, members[wallet2], "Wallet2 should not be a member after revoke")
		require.True(t, members[wallet3], "Wallet3 should still be a member")
	})

	// Test 5: List role members returns correct slice
	t.Run("List role members", func(t *testing.T) {
		wallets, err := procedure.ListRoleMembers(ctx, procedure.ListRoleMembersInput{Platform: platform, Owner: roleOwner, RoleName: roleName})
		require.NoError(t, err)
		// Expecting wallet1 and wallet3 (wallet2 revoked)
		require.Contains(t, wallets, wallet1)
		require.Contains(t, wallets, wallet3)
		require.NotContains(t, wallets, wallet2)
	})

	t.Run("Case insensitivity is handled correctly", func(t *testing.T) {
		const upperCaseRole = "CASE_TEST_ROLE"
		const lowerCaseRole = "case_test_role"
		const upperCaseOwner = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		const lowerCaseOwner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		const upperCaseWallet = "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
		const lowerCaseWallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

		ownerAddr := util.Unsafe_NewEthereumAddressFromString(lowerCaseOwner)
		ownerPlatform := procedure.WithSigner(platform, ownerAddr.Bytes())

		// 1. Create role with mixed case
		err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: upperCaseOwner, RoleName: upperCaseRole, DisplayName: "Case Test"})
		require.NoError(t, err)

		// 2. Grant using mixed case
		err = procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: upperCaseOwner, RoleName: upperCaseRole, Wallets: []string{upperCaseWallet}})
		require.NoError(t, err)

		// 3. Check membership using lowercase
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: lowerCaseOwner, RoleName: lowerCaseRole, Wallets: []string{lowerCaseWallet}})
		require.NoError(t, err)
		require.True(t, members[lowerCaseWallet], "Should be a member when checking with lowercase")

		// 4. Revoke using lowercase
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: ownerPlatform, Owner: lowerCaseOwner, RoleName: lowerCaseRole, Wallets: []string{lowerCaseWallet}})
		require.NoError(t, err)

		// 5. Check membership again, should be false
		members, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: upperCaseOwner, RoleName: upperCaseRole, Wallets: []string{upperCaseWallet}})
		require.NoError(t, err)
		require.False(t, members[lowerCaseWallet], "Should not be a member after revoking with lowercase")
	})
}

func testPermissionValidation(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = permissionTestRole
	// Setup: Create a test role
	ownerAddr := util.Unsafe_NewEthereumAddressFromString(roleOwner)
	ownerPlatform := procedure.WithSigner(platform, ownerAddr.Bytes())
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, DisplayName: "Permission Test Role"})
	require.NoError(t, err)

	t.Run("Unauthorized grant should fail", func(t *testing.T) {
		unauthAddr := util.Unsafe_NewEthereumAddressFromString(unauthorizedWallet)
		unauthPlatform := procedure.WithSigner(platform, unauthAddr.Bytes())
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: unauthPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		expectedErr := fmt.Sprintf("Caller %s is not the owner or a member of the manager role for %s:%s", strings.ToLower(unauthorizedWallet), strings.ToLower(roleOwner), strings.ToLower(roleName))
		require.ErrorContains(t, err, expectedErr)
	})

	t.Run("Unauthorized revoke should fail", func(t *testing.T) {
		// First grant the role as the owner
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		// Try to revoke as unauthorized user
		unauthAddr := util.Unsafe_NewEthereumAddressFromString(unauthorizedWallet)
		unauthPlatform := procedure.WithSigner(platform, unauthAddr.Bytes())
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: unauthPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		expectedErr := fmt.Sprintf("Caller %s is not the owner or a member of the manager role for %s:%s", strings.ToLower(unauthorizedWallet), strings.ToLower(roleOwner), strings.ToLower(roleName))
		require.ErrorContains(t, err, expectedErr)
	})

	t.Run("Check membership for non-existent role should fail", func(t *testing.T) {
		_, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: nonExistentRole, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.ErrorContains(t, err, errRoleDoesNotExist)
	})
}

func testInvalidInputValidation(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = inputTestRole
	// Create a test role for valid operations
	ownerAddr := util.Unsafe_NewEthereumAddressFromString(roleOwner)
	ownerPlatform := procedure.WithSigner(platform, ownerAddr.Bytes())
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, DisplayName: "Input Test Role"})
	require.NoError(t, err)

	t.Run("Invalid wallet address should fail", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{"invalid_wallet"}})
		require.Error(t, err)
		require.ErrorContains(t, err, errInvalidWalletAddress)

		_, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: roleOwner, RoleName: roleName, Wallets: []string{"invalid_wallet"}})
		require.Error(t, err)
		require.ErrorContains(t, err, errInvalidWalletAddress)
	})

	t.Run("Invalid owner address should fail", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: "invalid_owner", RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.ErrorContains(t, err, errInvalidOwnerAddress)

		_, err = procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: platform, Owner: "invalid_owner", RoleName: roleName, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.ErrorContains(t, err, errInvalidOwnerAddress)
	})

	t.Run("Grant to non-existent role should fail", func(t *testing.T) {
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: nonExistentRole, Wallets: []string{wallet1}})
		require.Error(t, err)
		require.ErrorContains(t, err, errRoleDoesNotExist)
	})

	t.Run("Revoke from non-member should be idempotent", func(t *testing.T) {
		// Ensure wallet1 is not a member for this test by revoking it first (it's ok if it's already not a member)
		err := procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err)

		// Revoke again, should not fail
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, Wallets: []string{wallet1}})
		require.NoError(t, err, "Revoking a non-member should be idempotent and not return an error")
	})
}

func testRoleManagerPermissions(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const managedRoleName = managedRole
	const managerOfManagedRole = "manager_of_managed_role"

	// Setup: Create a managed role and a manager role, then link them.
	// This tests the important use case where management MUST be delegated.
	// 1. Create the target role.
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{
		Platform:    platform,
		Owner:       systemOwner,
		RoleName:    managedRoleName,
		DisplayName: "A Managed Role",
	})
	require.NoError(t, err)

	// 2. Create the manager role.
	err = setup.CreateTestRole(ctx, setup.CreateTestRoleInput{
		Platform:    platform,
		Owner:       systemOwner,
		RoleName:    managerOfManagedRole,
		DisplayName: "Manager For The Managed Role",
	})
	require.NoError(t, err)

	// 3. Bootstrap: Make managerWallet a member of the manager role.
	err = setup.AddMemberToRoleBypass(ctx, platform, systemOwner, managerOfManagedRole, managerWallet)
	require.NoError(t, err)

	// 4. Bootstrap: Link the target role to the manager role.
	err = setup.SetRoleManagerBypass(ctx, platform, systemOwner, managedRoleName, systemOwner, managerOfManagedRole)
	require.NoError(t, err)

	managerAddr := util.Unsafe_NewEthereumAddressFromString(managerWallet)
	managerPlatform := procedure.WithSigner(platform, managerAddr.Bytes())

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
		unauthAddr := util.Unsafe_NewEthereumAddressFromString(unauthorizedWallet)
		unauthPlatform := procedure.WithSigner(platform, unauthAddr.Bytes())
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: unauthPlatform,
			Owner:    systemOwner,
			RoleName: managedRoleName,
			Wallets:  []string{wallet2},
		})
		require.Error(t, err)
		expectedErr := fmt.Sprintf("Caller %s is not the owner or a member of the manager role for %s:%s", strings.ToLower(unauthorizedWallet), systemOwner, managedRoleName)
		require.ErrorContains(t, err, expectedErr)
	})
}

func testBatchOperations(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const roleName = batchOpRole
	const managerRoleName = batchManagerRole

	// Create a platform signed by the role owner to ensure permissions are correct for subsequent operations.
	// This isolates the test from state leaked from previous test cases that modify the platform's signer.
	ownerAddr := util.Unsafe_NewEthereumAddressFromString(roleOwner)
	ownerPlatform := procedure.WithSigner(platform, ownerAddr.Bytes())

	// Setup: Create test roles. These are created once in the parent transaction.
	// Sub-tests will inherit this state but their own changes will be rolled back.
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: roleName, DisplayName: "Batch Ops Role"})
	require.NoError(t, err)

	// Create manager role and link it to a target role for manager batch tests
	const batchManagedRole = "batch_managed_role"
	err = setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: systemOwner, RoleName: batchManagedRole, DisplayName: "Batch Managed Role"})
	require.NoError(t, err)

	err = setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: platform, Owner: systemOwner, RoleName: managerRoleName, DisplayName: "Batch Manager Role"})
	require.NoError(t, err)

	err = setup.AddMemberToRoleBypass(ctx, platform, systemOwner, managerRoleName, managerWallet)
	require.NoError(t, err)

	err = setup.SetRoleManagerBypass(ctx, platform, systemOwner, batchManagedRole, systemOwner, managerRoleName)
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
		managerAddr := util.Unsafe_NewEthereumAddressFromString(managerWallet)
		managerPlatform := procedure.WithSigner(txPlatform, managerAddr.Bytes())

		// Manager grants roles to a batch for the role they manage
		err := procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: managerPlatform,
			Owner:    systemOwner,
			RoleName: batchManagedRole,
			Wallets:  []string{wallet1, wallet2},
		})
		require.NoError(t, err)

		// Manager revokes one of them
		err = procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{
			Platform: managerPlatform,
			Owner:    systemOwner,
			RoleName: batchManagedRole,
			Wallets:  []string{wallet2},
		})
		require.NoError(t, err)

		// Verify final state
		members, err := procedure.AreMembersOf(ctx, procedure.AreMembersOfInput{Platform: txPlatform, Owner: systemOwner, RoleName: batchManagedRole, Wallets: []string{wallet1, wallet2}})
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

func testSetRoleManager(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	const (
		setManagerTestRole = "set_manager_test_role"
		newManagerRole     = "new_manager_role"
	)
	// Setup platforms for different signers
	ownerAddr := util.Unsafe_NewEthereumAddressFromString(roleOwner)
	ownerPlatform := procedure.WithSigner(platform, ownerAddr.Bytes())

	unauthAddr := util.Unsafe_NewEthereumAddressFromString(unauthorizedWallet)

	// Setup: Create the target role and a potential manager role. This setup is inherited by all sub-tests.
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: setManagerTestRole, DisplayName: "Set Manager Test"})
	require.NoError(t, err)
	err = setup.CreateTestRole(ctx, setup.CreateTestRoleInput{Platform: ownerPlatform, Owner: roleOwner, RoleName: newManagerRole, DisplayName: "New Manager"})
	require.NoError(t, err)

	t.Run("Role owner can set a manager role", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		err := procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform:        txPlatform,
			Owner:           roleOwner,
			RoleName:        setManagerTestRole,
			ManagerOwner:    testutils.Ptr(roleOwner),
			ManagerRoleName: testutils.Ptr(newManagerRole),
		})
		require.NoError(t, err)
	}))

	t.Run("Non-owner cannot set a manager role", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Create a new platform for this transaction with the unauthorized signer
		unauthTxPlatform := procedure.WithSigner(txPlatform, unauthAddr.Bytes())

		err := procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform:        unauthTxPlatform, // Use unauthorized signer but with the transaction's DB
			Owner:           roleOwner,
			RoleName:        setManagerTestRole,
			ManagerOwner:    testutils.Ptr(roleOwner),
			ManagerRoleName: testutils.Ptr(newManagerRole),
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "is not the owner of role") // from helper_assert_is_role_owner
	}))

	t.Run("Role owner can clear a manager role", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// First, set it
		err := procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform: txPlatform, Owner: roleOwner, RoleName: setManagerTestRole, ManagerOwner: testutils.Ptr(roleOwner), ManagerRoleName: testutils.Ptr(newManagerRole),
		})
		require.NoError(t, err)

		// Then, clear it by passing NULLs
		err = procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform:        txPlatform,
			Owner:           roleOwner,
			RoleName:        setManagerTestRole,
			ManagerOwner:    nil, // Go nil becomes SQL NULL
			ManagerRoleName: nil,
		})
		require.NoError(t, err)
	}))

	t.Run("Setting a non-existent manager role should fail due to FK constraint", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		err := procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform:        txPlatform,
			Owner:           roleOwner,
			RoleName:        setManagerTestRole,
			ManagerOwner:    testutils.Ptr(roleOwner),
			ManagerRoleName: testutils.Ptr("this_role_does_not_exist"),
		})
		require.Error(t, err)
		// The error comes from the DB's foreign key constraint
		require.ErrorContains(t, err, "violates foreign key constraint")
	}))

	t.Run("Setting manager with only one NULL parameter should fail", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		err := procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform:        txPlatform,
			Owner:           roleOwner,
			RoleName:        setManagerTestRole,
			ManagerOwner:    testutils.Ptr(roleOwner),
			ManagerRoleName: nil, // Only one is nil
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "To set a manager role, both manager_owner and manager_role_name must be provided. To remove a manager, both must be NULL.")
	}))

	t.Run("Setting manager with only one NULL parameter should fail (case 2)", testutils.WithTx(ownerPlatform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		err := procedure.SetRoleManager(ctx, procedure.SetRoleManagerInput{
			Platform:        txPlatform,
			Owner:           roleOwner,
			RoleName:        setManagerTestRole,
			ManagerOwner:    nil,
			ManagerRoleName: testutils.Ptr(newManagerRole), // Only one is nil
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "To set a manager role, both manager_owner and manager_role_name must be provided. To remove a manager, both must be NULL.")
	}))
}
