package adapters

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
	"time"
)

func StartGanacheDockerService(t *testing.T, ctx context.Context, chainId string) *ganacheContainer {
	//t.Helper()

	container, err := setupGanache(ctx,
		chainId,
		WithNetwork(kwilTestNetworkName),
		WithPort(GanachePort),
		WithWaitStrategy(
			wait.ForLog("RPC Listening on 0.0.0.0:8545")))

	require.NoError(t, err, "Could not start ganache container")

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx), "Could not stop ganache container")
	})

	err = container.ShowPortInfo(ctx)
	require.NoError(t, err)

	return container
}

func StartDBDockerService(t *testing.T, ctx context.Context, files map[string]string) *postgresContainer {
	//t.Helper()

	//dbURL := func(host string, port nat.Port) string {
	//	return fmt.Sprintf("postgres://%:%s@%s:%s/%s?sslmode=disable", pgUser, pgPassword, host, port.Port(), kwildDatabase)
	//}

	container, err := setupPostgres(ctx,
		WithNetwork(kwilTestNetworkName),
		WithPort(PgPort),
		WithInitialDatabase(pgUser, pgPassword),
		WithFiles(files),
		WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Second*20)))
	//wait.ForSQL(nat.Port(PgPort), "pgx", dbURL).WithStartupTimeout(time.Second*30)))

	require.NoError(t, err, "Could not start postgres container")

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx), "Could not stop postgres container")
	})

	err = container.ShowPortInfo(ctx)
	require.NoError(t, err)

	return container
}

func StartKwildDockerService(t *testing.T, ctx context.Context, envs map[string]string) *kwildContainer {
	//t.Helper()

	container, err := setupKwild(ctx,
		//WithDockerFile("kwil"),
		WithNetwork(kwilTestNetworkName),
		WithPort(KwildPort),
		WithEnv(envs),
		// ForListeningPort requires image has /bin/sh
		WithWaitStrategy(wait.ForLog("grpc server started"),
			wait.ForLog("deposits synced")))

	require.NoError(t, err, "Could not start kwil container")

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx), "Could not stop kwil container")
	})

	err = container.ShowPortInfo(ctx)
	require.NoError(t, err)
	return container
}

func StartDockerServer(t *testing.T, port string, cmd string) testcontainers.Container {
	//t.Helper()

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		FromDockerfile: newTCDockerfile(cmd),
		ExposedPorts:   []string{fmt.Sprintf("%s:%s", port, port)},
		WaitingFor:     wait.ForListeningPort(nat.Port(port)).WithStartupTimeout(startupTimeout),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	return container
}

func newTCDockerfile(cmd string) testcontainers.FromDockerfile {
	return testcontainers.FromDockerfile{
		Context:    "../../.",
		Dockerfile: fmt.Sprintf("docker/%s.dockerfile", cmd),
		BuildArgs: map[string]*string{
			"bin_to_build": &cmd,
		},
		PrintBuildLog: true,
	}
}
