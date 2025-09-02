package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestPermissionGates(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "permission_gates_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				testStreamCreationPermissionGates(t, ctx, platform)
				return nil
			},
		},
	}, testutils.GetTestOptions())
}

func testStreamCreationPermissionGates(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	// Initialize platform deployer with a valid address
	defaultDeployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
	platform.Deployer = defaultDeployer.Bytes()

	// Test addresses - using valid Ethereum address format
	const (
		authorizedWriter = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d51"
		unauthorizedUser = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d52"
		managerWallet    = "0x742d35cc6634c0532925a3b8d67e2c4e4d4c4d53"
	)

	// Setup: Create required system roles and assign one authorized writer.
	// Note: Data providers are now auto-created when granting network_writer role
	setupSystemRoles(t, ctx, platform, managerWallet, authorizedWriter)

	// Create platforms with the correct signers for each identity.
	managerAddr := util.Unsafe_NewEthereumAddressFromString(managerWallet)
	authorizedAddr := util.Unsafe_NewEthereumAddressFromString(authorizedWriter)
	unauthorizedAddr := util.Unsafe_NewEthereumAddressFromString(unauthorizedUser)

	managerPlatform := procedure.WithSigner(platform, managerAddr.Bytes())
	authorizedPlatform := procedure.WithSigner(platform, authorizedAddr.Bytes())
	unauthorizedPlatform := procedure.WithSigner(platform, unauthorizedAddr.Bytes())

	// Defines a test case for stream creation permissions.
	type permissionTestCase struct {
		name          string
		platform      *kwilTesting.Platform
		userAddress   string
		action        func(p *kwilTesting.Platform, userAddress string) error
		expectSuccess bool
		expectedError string
	}

	testCases := []permissionTestCase{
		{
			name:        "unauthorized user cannot create single stream",
			platform:    unauthorizedPlatform,
			userAddress: unauthorizedUser,
			action: func(p *kwilTesting.Platform, user string) error {
				streamID := util.GenerateStreamId("unauthorized_single")
				return setup.UntypedCreateStream(ctx, p, streamID.String(), user, "primitive")
			},
			expectSuccess: false,
			expectedError: "Caller does not have the required system:network_writer role",
		},
		{
			name:        "unauthorized user cannot create multiple streams",
			platform:    unauthorizedPlatform,
			userAddress: unauthorizedUser,
			action: func(p *kwilTesting.Platform, user string) error {
				return setup.CreateStreamsWithOptions(ctx, p, generateStreamInfos(user, 2), setup.CreateStreamsOptions{
					SkipAutoRoleGrant: true,
				})
			},
			expectSuccess: false,
			expectedError: "Caller does not have the required system:network_writer role",
		},
		{
			name:        "authorized user can create single stream",
			platform:    authorizedPlatform,
			userAddress: authorizedWriter,
			action: func(p *kwilTesting.Platform, user string) error {
				streamID := util.GenerateStreamId("authorized_single")
				return setup.UntypedCreateStream(ctx, p, streamID.String(), user, "primitive")
			},
			expectSuccess: true,
		},
		{
			name:        "authorized user can create multiple streams",
			platform:    authorizedPlatform,
			userAddress: authorizedWriter,
			action: func(p *kwilTesting.Platform, user string) error {
				return setup.CreateStreamsWithOptions(ctx, p, generateStreamInfos(user, 2), setup.CreateStreamsOptions{
					SkipAutoRoleGrant: true,
				})
			},
			expectSuccess: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.action(tc.platform, tc.userAddress)
			if tc.expectSuccess {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedError)
			}
		})
	}

	t.Run("permission gates respond to role state changes", func(t *testing.T) {
		// Revoke the role
		err := procedure.RevokeRoles(ctx, procedure.RevokeRolesInput{
			Platform: managerPlatform,
			Owner:    "system",
			RoleName: "network_writer",
			Wallets:  []string{authorizedWriter},
		})
		require.NoError(t, err)

		// Now verify the formerly authorized user cannot create a stream
		streamID := util.GenerateStreamId("post_revocation")
		err = setup.UntypedCreateStream(ctx, authorizedPlatform, streamID.String(), authorizedWriter, "primitive")
		require.ErrorContains(t, err, "Caller does not have the required system:network_writer role", "User should not be able to create stream after role is revoked")

		// Re-grant the role
		err = procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: managerPlatform,
			Owner:    "system",
			RoleName: "network_writer",
			Wallets:  []string{authorizedWriter},
		})
		require.NoError(t, err)

		// Verify the user can create streams again
		streamID2 := util.GenerateStreamId("post_regrant")
		err = setup.UntypedCreateStream(ctx, authorizedPlatform, streamID2.String(), authorizedWriter, "primitive")
		require.NoError(t, err, "User should be able to create stream after role is re-granted")
	})
}

// setupSystemRoles is a helper to create the necessary system roles for testing.
func setupSystemRoles(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, managerAddr, writerAddr string) {
	const writerRoleName = "network_writer"
	const managerRoleName = "network_writers_manager"

	// 1. Create the target role (network_writer)
	err := setup.CreateTestRole(ctx, setup.CreateTestRoleInput{
		Platform:    platform,
		Owner:       "system",
		RoleName:    writerRoleName,
		DisplayName: "Network Writer",
	})
	require.NoError(t, err)

	// 2. Create the role that will manage the target role (network_writers_manager)
	err = setup.CreateTestRole(ctx, setup.CreateTestRoleInput{
		Platform:    platform,
		Owner:       "system",
		RoleName:    managerRoleName,
		DisplayName: "Network Writers Manager",
	})
	require.NoError(t, err)

	// 3. BOOTSTRAP: Directly add the manager wallet as a MEMBER of the manager role.
	// This bypasses the normal action validation to solve the chicken-and-egg problem.
	err = setup.AddMemberToRoleBypass(ctx, platform, "system", managerRoleName, managerAddr)
	require.NoError(t, err)

	// 4. BOOTSTRAP: Set the network_writer role to be managed by the network_writers_manager role.
	err = setup.SetRoleManagerBypass(ctx, platform, "system", writerRoleName, "system", managerRoleName)
	require.NoError(t, err)

	// Now the manager (a member of network_writers_manager) can grant the network_writer role.
	managerAddress := util.Unsafe_NewEthereumAddressFromString(managerAddr)
	managerPlatform := procedure.WithSigner(platform, managerAddress.Bytes())
	err = procedure.GrantRoles(ctx, procedure.GrantRolesInput{
		Platform: managerPlatform,
		Owner:    "system",
		RoleName: writerRoleName,
		Wallets:  []string{writerAddr},
	})
	require.NoError(t, err)
}

// generateStreamInfos is a helper to create a slice of StreamInfo for batch creation tests.
func generateStreamInfos(dataProvider string, count int) []setup.StreamInfo {
	infos := make([]setup.StreamInfo, count)
	dpAddress := util.Unsafe_NewEthereumAddressFromString(dataProvider)

	for i := 0; i < count; i++ {
		streamType := setup.ContractTypePrimitive
		if i%2 != 0 {
			streamType = setup.ContractTypeComposed
		}
		infos[i] = setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     util.GenerateStreamId(fmt.Sprintf("batch_stream_%d", i)),
				DataProvider: dpAddress,
			},
			Type: streamType,
		}
	}
	return infos
}
