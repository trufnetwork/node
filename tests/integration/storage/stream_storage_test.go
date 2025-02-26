package stream_storage_test

// TODO: Disabled for kwil-db v.0.10.0 upgrade
//import (
//	"bytes"
//	"context"
//	"errors"
//	"fmt"
//	"os/exec"
//	"strconv"
//	"strings"
//	"testing"
//	"time"
//
//	"github.com/ethereum/go-ethereum/crypto"
//	kwilcrypto "github.com/kwilteam/kwil-db/core/crypto"
//	"github.com/kwilteam/kwil-db/core/crypto/auth"
//	"github.com/trufnetwork/sdk-go/core/tnclient"
//	"github.com/trufnetwork/sdk-go/core/util"
//)
//
///*
// * Test the stream storage
//
//	1. Start containers for kwil and postgres, isolated from the rest of the system
//	2. measure the current directory size:
//		- postgres: /var/lib/postgresql/data
//		- kwil: /root/.kwildb/
//	3. run the stream storage test:
//		- create 1,000 streams
//		- initialize all streams with 1000 messages each
//	4. measure the directory size of kwil and postgres
//	5. drop all streams
//	6. measure the directory size of kwil and postgres
//	7. log all sizes
//	8. teardown containers
//*/
//
//// containerSpec defines the configuration for a container
//type containerSpec struct {
//	name        string
//	image       string
//	tmpfsPath   string
//	envVars     []string
//	healthCheck func(d *docker) error
//}
//
//// testContainers defines the containers needed for the test
//var containers = struct {
//	postgres containerSpec
//	tsndb    containerSpec
//}{
//	postgres: containerSpec{
//		name:      "test-kwil-postgres",
//		image:     "kwildb/postgres:latest",
//		tmpfsPath: "/var/lib/postgresql/data",
//		envVars:   []string{"POSTGRES_HOST_AUTH_METHOD=trust"},
//		healthCheck: func(d *docker) error {
//			_, err := d.exec("test-kwil-postgres", "pg_isready", "-U", "postgres")
//			return err
//		},
//	},
//	tsndb: containerSpec{
//		name:      "test-tsn-db",
//		image:     "tsn-db:local",
//		tmpfsPath: "/root/.kwild",
//		envVars: []string{
//			"CONFIG_PATH=/root/.kwild",
//			"KWILD_APP_HOSTNAME=test-tsn-db",
//			"KWILD_APP_PG_DB_HOST=test-kwil-postgres",
//			"KWILD_CHAIN_P2P_EXTERNAL_ADDRESS=http://test-tsn-db:26656",
//		},
//		healthCheck: func(d *docker) error {
//			// Wait for the service to be ready
//			time.Sleep(5 * time.Second)
//			_, err := d.exec("test-tsn-db", "ps", "aux")
//			return err
//		},
//	},
//}
//
//// docker provides a simplified interface for docker operations
//type docker struct {
//	t *testing.T
//}
//
//// newDocker creates a new docker helper
//func newDocker(t *testing.T) *docker {
//	return &docker{t: t}
//}
//
//// exec executes a command in a container
//func (d *docker) exec(container string, args ...string) (string, error) {
//	cmdArgs := append([]string{"exec", container}, args...)
//	return d.run(cmdArgs...)
//}
//
//// run executes a docker command
//func (d *docker) run(args ...string) (string, error) {
//	cmd := exec.Command("docker", args...)
//	var out bytes.Buffer
//	cmd.Stdout = &out
//	cmd.Stderr = &out
//	err := cmd.Run()
//	return out.String(), err
//}
//
//// failWithLogsOnError logs container logs and fails the test if err is non-nil.
//func (d *docker) failWithLogsOnError(err error, containerName string) {
//	if err != nil {
//		if logs, logsErr := d.run("logs", containerName); logsErr == nil {
//			d.t.Logf("Logs for %s:\n%s", containerName, logs)
//		}
//		d.t.Fatal(err)
//	}
//}
//
//// pollUntilTrue polls a condition until it returns true or a timeout is reached.
//func pollUntilTrue(ctx context.Context, timeout time.Duration, check func() bool) error {
//	deadline := time.Now().Add(timeout)
//	for time.Now().Before(deadline) {
//		if check() {
//			return nil
//		}
//		time.Sleep(time.Second)
//	}
//	return errors.New("condition not met within timeout")
//}
//
//// startContainer starts a container with the given spec and waits for it to be healthy.
//func (d *docker) startContainer(spec containerSpec) error {
//	args := []string{"run", "--rm", "--name", spec.name, "--network", networkName, "-d"}
//
//	if spec.tmpfsPath != "" {
//		args = append(args, "--tmpfs", spec.tmpfsPath)
//	}
//
//	for _, env := range spec.envVars {
//		args = append(args, "-e", env)
//	}
//
//	if spec.name == "test-tsn-db" {
//		args = append(args,
//			"-p", "50051:50051",
//			"-p", "50151:50151",
//			"-p", "8080:8080",
//			"-p", "8484:8484",
//			"-p", "26656:26656",
//			"-p", "26657:26657",
//			"--entrypoint", "/app/kwild",
//			spec.image,
//			"--autogen",
//			"--app.pg-db-host", "test-kwil-postgres",
//			"--app.hostname", "test-tsn-db",
//			"--chain.p2p.external-address", "http://test-tsn-db:26656",
//		)
//	} else {
//		args = append(args, spec.image)
//	}
//
//	out, err := d.run(args...)
//	if err != nil {
//		return fmt.Errorf("failed to start container %s: %w\nOutput: %s", spec.name, err, out)
//	}
//
//	if spec.healthCheck != nil {
//		err := pollUntilTrue(context.Background(), 10*time.Second, func() bool {
//			return spec.healthCheck(d) == nil
//		})
//		if err != nil {
//			if logs, logsErr := d.run("logs", spec.name); logsErr == nil {
//				d.t.Logf("Container logs for %s:\n%s", spec.name, logs)
//			}
//			return fmt.Errorf("container %s failed to become healthy: %w", spec.name, err)
//		}
//	}
//
//	if spec.name == "test-tsn-db" {
//		err := pollUntilTrue(context.Background(), 30*time.Second, func() bool {
//			out, err := exec.Command("curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "http://localhost:8484/api/v1/health").Output()
//			if err != nil {
//				return false
//			}
//			return strings.TrimSpace(string(out)) == "200"
//		})
//		if err != nil {
//			if logs, logsErr := d.run("logs", spec.name); logsErr == nil {
//				d.t.Logf("Container logs for %s:\n%s", spec.name, logs)
//			}
//			return fmt.Errorf("RPC server in container %s failed to become ready: %w", spec.name, err)
//		}
//	}
//
//	return nil
//}
//
//// stopContainer stops a container
//func (d *docker) stopContainer(name string) error {
//	_, err := d.run("stop", name)
//	if err != nil {
//		return fmt.Errorf("failed to stop container %s: %w", name, err)
//	}
//	d.t.Logf("Stopped container %s", name)
//	return nil
//}
//
//// setupNetwork creates a docker network
//func (d *docker) setupNetwork() error {
//	d.run("network", "rm", networkName)
//	_, err := d.run("network", "create", networkName)
//	return err
//}
//
//// teardownNetwork removes the docker network
//func (d *docker) teardownNetwork() error {
//	_, err := d.run("network", "rm", networkName)
//	return err
//}
//
//// measureSize measures the size of a directory in a container
//func (d *docker) measureSize(container, path string) (int64, error) {
//	out, err := d.exec(container, "du", "-sb", path)
//	if err != nil {
//		return 0, err
//	}
//	return parseDuOutput(out)
//}
//
//// runCommand executes a command and returns its combined output or error.
//func runCommand(name string, args ...string) (string, error) {
//	cmd := exec.Command(name, args...)
//	var out bytes.Buffer
//	cmd.Stdout = &out
//	cmd.Stderr = &out
//	err := cmd.Run()
//	return out.String(), err
//}
//
//// parseDuOutput parses the output of du command and returns the size in bytes
//func parseDuOutput(output string) (int64, error) {
//	fields := strings.Fields(output)
//	if len(fields) < 1 {
//		return 0, fmt.Errorf("unexpected du output: %s", output)
//	}
//	return strconv.ParseInt(fields[0], 10, 64)
//}
//
//// bytesToMB converts bytes to megabytes with 2 decimal places
//func bytesToMB(bytes int64) float64 {
//	return float64(bytes) / (1024 * 1024)
//}
//
//// cleanup removes all docker resources
//func (d *docker) cleanup() {
//	// Get all container IDs
//	out, err := d.run("ps", "-aq")
//	if err == nil && out != "" {
//		containers := strings.Fields(out)
//		if len(containers) > 0 {
//			killArgs := append([]string{"kill"}, containers...)
//			d.run(killArgs...)
//			rmArgs := append([]string{"rm"}, containers...)
//			d.run(rmArgs...)
//		}
//	}
//	// Remove networks
//	d.run("network", "prune", "-f")
//	// Remove volume
//	d.run("volume", "rm", "tsn-config")
//}
//
//func TestStreamStorage(t *testing.T) {
//	ctx := context.Background()
//
//	// Setup docker helper
//	d := newDocker(t)
//
//	// Clean up any existing resources
//	d.cleanup()
//
//	// Create network
//	if err := d.setupNetwork(); err != nil {
//		t.Fatal(err)
//	}
//	defer d.teardownNetwork()
//
//	// Start postgres first
//	if err := d.startContainer(containers.postgres); err != nil {
//		t.Fatal(err)
//	}
//	defer d.stopContainer(containers.postgres.name)
//
//	// Wait for postgres to be healthy
//	for i := 0; i < 10; i++ {
//		if err := containers.postgres.healthCheck(d); err == nil {
//			break
//		}
//		if i == 9 {
//			t.Fatal("postgres failed to become healthy")
//		}
//		time.Sleep(time.Second)
//	}
//
//	// Start tsn-db with autogen
//	t.Log("Starting tsn-db container...")
//	if err := d.startContainer(containers.tsndb); err != nil {
//		// Get logs before failing
//		if out, err := d.run("logs", containers.tsndb.name); err == nil {
//			t.Logf("TSN-DB container logs:\n%s", out)
//		} else {
//			t.Logf("Failed to get TSN-DB logs: %v", err)
//		}
//		// Get container status
//		if status, err := d.run("inspect", "--format", "{{.State.Status}}", containers.tsndb.name); err == nil {
//			t.Logf("TSN-DB container status: %s", status)
//		}
//		t.Fatalf("Failed to start tsn-db container: %v", err)
//	}
//	t.Log("TSN-DB container started successfully")
//
//	// Wait for node to be fully initialized
//	t.Log("Waiting for node to be fully initialized...")
//	for i := 0; i < 30; i++ { // 30 seconds max wait
//		healthCmd := exec.Command("curl", "-s", TestKwilProvider+"/api/v1/health")
//		healthOut, healthErr := healthCmd.CombinedOutput()
//		if healthErr == nil {
//			t.Logf("Health check response: %s", string(healthOut))
//			if strings.Contains(string(healthOut), `"healthy":true`) &&
//				strings.Contains(string(healthOut), `"block_height":1`) {
//				t.Log("Node is healthy and has produced the first block")
//				break
//			}
//		}
//		if i == 29 {
//			t.Fatal("Node failed to become healthy or produce the first block")
//		}
//		time.Sleep(time.Second)
//	}
//
//	// Get initial container logs
//	if out, err := d.run("logs", containers.tsndb.name); err == nil {
//		t.Logf("Initial TSN-DB container logs:\n%s", out)
//	}
//
//	defer d.stopContainer(containers.tsndb.name)
//
//	// Measure initial sizes
//	pgSizeBefore, err := d.measureSize(containers.postgres.name, containers.postgres.tmpfsPath)
//	if err != nil {
//		t.Fatal(err)
//	}
//	tsnSizeBefore, err := d.measureSize(containers.tsndb.name, containers.tsndb.tmpfsPath)
//	if err != nil {
//		t.Fatal(err)
//	}
//	t.Logf("Initial sizes - Postgres: %.2f MB, TSN-DB: %.2f MB", bytesToMB(pgSizeBefore), bytesToMB(tsnSizeBefore))
//
//	// Initialize stream manager
//	t.Log("Creating private key...")
//	pk, err := kwilcrypto.Secp256k1PrivateKeyFromHex(TestPrivateKey)
//	if err != nil {
//		t.Fatalf("Failed to parse private key: %v", err)
//	}
//	t.Log("Successfully created private key")
//
//	t.Log("Creating TN client...")
//	t.Logf("Using provider: %s", TestKwilProvider)
//
//	// Get the Ethereum address from the public key
//	pubKeyBytes := pk.PubKey().Bytes()
//	// Remove the first byte which is the compression flag
//	pubKeyBytes = pubKeyBytes[1:]
//	addr, err := util.NewEthereumAddressFromBytes(crypto.Keccak256(pubKeyBytes)[12:])
//	if err != nil {
//		t.Fatalf("Failed to get address from public key: %v", err)
//	}
//	t.Logf("Using signer with address: %s", addr.Address())
//
//	t.Log("Attempting to create client...")
//	var client *tnclient.Client
//	var lastErr error
//	for i := 0; i < 60; i++ { // 60 seconds max wait
//		t.Logf("Attempt %d/60: Creating client with provider URL %s", i+1, TestKwilProvider)
//
//		// First check if the server is accepting connections
//		cmd := exec.Command("curl", "-s", "-w", "\n%{http_code}", "http://localhost:8484/api/v1/health")
//		out, err := cmd.CombinedOutput()
//		if err != nil {
//			lastErr = fmt.Errorf("health check command failed: %w", err)
//			t.Logf("Health check command failed: %v", err)
//			time.Sleep(time.Second)
//			continue
//		}
//
//		// Split output into response body and status code
//		parts := strings.Split(string(out), "\n")
//		if len(parts) != 2 {
//			lastErr = fmt.Errorf("unexpected health check output format: %s", string(out))
//			t.Logf("Health check output format error: %s", string(out))
//			time.Sleep(time.Second)
//			continue
//		}
//
//		statusCode := strings.TrimSpace(parts[1])
//		t.Logf("Health check response - Status: %s", statusCode)
//
//		if statusCode != "200" {
//			lastErr = fmt.Errorf("health check returned non-200 status: %s", statusCode)
//			t.Logf("Health check failed with status %s", statusCode)
//			time.Sleep(time.Second)
//			continue
//		}
//
//		t.Log("Health check passed, attempting to create client...")
//
//		// Try to create the client now that we know the server is accepting connections
//		client, err = tnclient.NewClient(
//			ctx,
//			TestKwilProvider,
//			tnclient.WithSigner(&auth.EthPersonalSigner{Key: *pk}),
//		)
//		if err != nil {
//			lastErr = fmt.Errorf("failed to create TN client: %w", err)
//			t.Logf("Client creation failed: %v", err)
//			time.Sleep(time.Second)
//			continue
//		}
//
//		// Successfully created client
//		t.Log("Client created successfully")
//		break
//	}
//
//	if client == nil {
//		t.Fatalf("Failed to create client after 60 attempts. Last error: %v", lastErr)
//	}
//
//	sm, err := newStreamManager(ctx, t)
//	if err != nil {
//		t.Fatalf("Failed to create stream manager: %v", err)
//	}
//
//	// Deploy streams
//	streams, err := sm.deployStreams(ctx, numStreams)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Initialize streams
//	if err := sm.initializeStreams(ctx, streams); err != nil {
//		t.Fatal(err)
//	}
//
//	// Measure sizes after creation
//	pgSizeAfter, err := d.measureSize(containers.postgres.name, containers.postgres.tmpfsPath)
//	if err != nil {
//		t.Fatal(err)
//	}
//	tsnSizeAfter, err := d.measureSize(containers.tsndb.name, containers.tsndb.tmpfsPath)
//	if err != nil {
//		t.Fatal(err)
//	}
//	t.Logf("After creation - Postgres: %.2f MB, TSN-DB: %.2f MB", bytesToMB(pgSizeAfter), bytesToMB(tsnSizeAfter))
//
//	// Destroy streams
//	if err := sm.destroyStreams(ctx, numStreams); err != nil {
//		t.Fatal(err)
//	}
//
//	// Measure final sizes
//	pgSizeFinal, err := d.measureSize(containers.postgres.name, containers.postgres.tmpfsPath)
//	if err != nil {
//		t.Fatal(err)
//	}
//	tsnSizeFinal, err := d.measureSize(containers.tsndb.name, containers.tsndb.tmpfsPath)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Log all measurements
//	t.Log("Final measurements:")
//	t.Logf("Postgres (MB): before=%.2f, after=%.2f, final=%.2f",
//		bytesToMB(pgSizeBefore), bytesToMB(pgSizeAfter), bytesToMB(pgSizeFinal))
//	t.Logf("TSN-DB (MB): before=%.2f, after=%.2f, final=%.2f",
//		bytesToMB(tsnSizeBefore), bytesToMB(tsnSizeAfter), bytesToMB(tsnSizeFinal))
//}
