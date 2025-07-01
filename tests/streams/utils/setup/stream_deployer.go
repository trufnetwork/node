package setup

import (
	"context"

	"github.com/pkg/errors"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/sdk-go/core/util"
)

// EnableStreamDeployerInput contains the configuration for enabling a user to deploy streams
type EnableStreamDeployerInput struct {
	Platform     *kwilTesting.Platform
	DeployerAddr string // The address that should be able to deploy streams
	ManagerAddr  string // Optional: if provided, this address will be able to manage the network_writer role
}

// EnableStreamDeployer sets up the necessary role management to allow a user to deploy streams.
// This function handles the bootstrapping required for the new role management system.
//
// It creates:
// 1. system:network_writer role (required to create streams)
// 2. system:network_writers_manager role (to manage the network_writer role)
// 3. Grants deployer the network_writer role
// 4. If manager provided, grants them the manager role
//
// This should be called once per test that needs to create streams.
func EnableStreamDeployer(ctx context.Context, input EnableStreamDeployerInput) error {
	const writerRoleName = "network_writer"
	const managerRoleName = "network_writers_manager"

	// 1. Create the target role (network_writer) - required for stream creation
	err := CreateTestRole(ctx, CreateTestRoleInput{
		Platform:    input.Platform,
		Owner:       "system",
		RoleName:    writerRoleName,
		DisplayName: "Network Writer",
	})
	if err != nil {
		return errors.Wrap(err, "failed to create network_writer role")
	}

	// 2. Create the manager role (network_writers_manager)
	err = CreateTestRole(ctx, CreateTestRoleInput{
		Platform:    input.Platform,
		Owner:       "system",
		RoleName:    managerRoleName,
		DisplayName: "Network Writers Manager",
	})
	if err != nil {
		return errors.Wrap(err, "failed to create network_writers_manager role")
	}

	// 3. If a manager address is provided, bootstrap them as a member of the manager role
	if input.ManagerAddr != "" {
		err = AddMemberToRoleBypass(ctx, input.Platform, "system", managerRoleName, input.ManagerAddr)
		if err != nil {
			return errors.Wrap(err, "failed to add manager to network_writers_manager role")
		}

		// 4. Set the network_writer role to be managed by the network_writers_manager role
		err = SetRoleManagerBypass(ctx, input.Platform, "system", writerRoleName, "system", managerRoleName)
		if err != nil {
			return errors.Wrap(err, "failed to set manager for network_writer role")
		}

		// 5. Grant the deployer the network_writer role using the manager
		managerAddr := util.Unsafe_NewEthereumAddressFromString(input.ManagerAddr)
		managerPlatform := procedure.WithSigner(input.Platform, managerAddr.Bytes())

		err = procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: managerPlatform,
			Owner:    "system",
			RoleName: writerRoleName,
			Wallets:  []string{input.DeployerAddr},
		})
		if err != nil {
			return errors.Wrap(err, "failed to grant network_writer role to deployer")
		}
	} else {
		// If no manager is provided, directly bootstrap the deployer as a network_writer
		err = AddMemberToRoleBypass(ctx, input.Platform, "system", writerRoleName, input.DeployerAddr)
		if err != nil {
			return errors.Wrap(err, "failed to add deployer to network_writer role")
		}
	}

	return nil
}

// EnablePlatformStreamDeployer is a convenience function that enables the platform's deployer
// to create streams. This is the most common use case for tests.
//
// This is the **high ROI function** - most tests just need to add one line:
// setup.EnablePlatformStreamDeployer(ctx, platform)
func EnablePlatformStreamDeployer(ctx context.Context, platform *kwilTesting.Platform) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "invalid platform deployer address")
	}

	// Simply grant the platform deployer the network_writer role
	// The roles already exist from the seed scripts
	return AddMemberToRoleBypass(ctx, platform, "system", "network_writer", deployer.Address())
}

// EnableMultipleStreamDeployers sets up role management to allow multiple users to deploy streams.
// This is useful for tests that need multiple independent stream deployers.
func EnableMultipleStreamDeployers(ctx context.Context, platform *kwilTesting.Platform, deployerAddresses []string, managerAddr string) error {
	const writerRoleName = "network_writer"
	const managerRoleName = "network_writers_manager"

	// Create roles if they don't exist
	err := CreateTestRole(ctx, CreateTestRoleInput{
		Platform:    platform,
		Owner:       "system",
		RoleName:    writerRoleName,
		DisplayName: "Network Writer",
	})
	if err != nil {
		return errors.Wrap(err, "failed to create network_writer role")
	}

	err = CreateTestRole(ctx, CreateTestRoleInput{
		Platform:    platform,
		Owner:       "system",
		RoleName:    managerRoleName,
		DisplayName: "Network Writers Manager",
	})
	if err != nil {
		return errors.Wrap(err, "failed to create network_writers_manager role")
	}

	// Bootstrap manager if provided
	if managerAddr != "" {
		err = AddMemberToRoleBypass(ctx, platform, "system", managerRoleName, managerAddr)
		if err != nil {
			return errors.Wrap(err, "failed to add manager to network_writers_manager role")
		}

		err = SetRoleManagerBypass(ctx, platform, "system", writerRoleName, "system", managerRoleName)
		if err != nil {
			return errors.Wrap(err, "failed to set manager for network_writer role")
		}

		// Grant all deployers the network_writer role
		managerAddress := util.Unsafe_NewEthereumAddressFromString(managerAddr)
		managerPlatform := procedure.WithSigner(platform, managerAddress.Bytes())

		err = procedure.GrantRoles(ctx, procedure.GrantRolesInput{
			Platform: managerPlatform,
			Owner:    "system",
			RoleName: writerRoleName,
			Wallets:  deployerAddresses,
		})
		if err != nil {
			return errors.Wrap(err, "failed to grant network_writer role to deployers")
		}
	} else {
		// Bootstrap all deployers directly
		for _, addr := range deployerAddresses {
			err = AddMemberToRoleBypass(ctx, platform, "system", writerRoleName, addr)
			if err != nil {
				return errors.Wrapf(err, "failed to add deployer %s to network_writer role", addr)
			}
		}
	}

	return nil
}
