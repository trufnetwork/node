package benchutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerMemoryCollector collects memory usage stats of a Docker container.
type DockerMemoryCollector struct {
	containerName   string
	maxMemoryUsage  uint64
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	mu              sync.Mutex
	errChan         chan error
	firstSampleChan chan struct{}
}

// StartDockerMemoryCollector initializes and starts the memory collector.
func StartDockerMemoryCollector(containerName string) (*DockerMemoryCollector, error) {
	ctx, cancel := context.WithCancel(context.Background())
	collector := &DockerMemoryCollector{
		containerName:   containerName,
		ctx:             ctx,
		cancel:          cancel,
		errChan:         make(chan error, 1),
		firstSampleChan: make(chan struct{}),
	}
	collector.wg.Add(1)
	go collector.collectStats()
	return collector, nil
}

// collectStats collects memory stats using Docker's ContainerStats API.
func (c *DockerMemoryCollector) collectStats() {
	defer c.wg.Done()

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		c.errChan <- fmt.Errorf("error creating Docker client: %w", err)
		close(c.firstSampleChan) // Ensure channel is closed
		return
	}
	defer cli.Close()
	cli.NegotiateAPIVersion(c.ctx)

	containerID, err := c.getContainerID(cli)
	if err != nil {
		c.errChan <- fmt.Errorf("error getting container ID: %w", err)
		close(c.firstSampleChan) // Ensure channel is closed
		return
	}

	stats, err := cli.ContainerStats(c.ctx, containerID, true) // stream=true
	if err != nil {
		c.errChan <- fmt.Errorf("error getting container stats: %w", err)
		close(c.firstSampleChan) // Ensure channel is closed
		return
	}
	defer stats.Body.Close()

	decoder := json.NewDecoder(stats.Body)
	firstSampleReceived := false
	for {
		var v *container.StatsResponse
		if err := decoder.Decode(&v); err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "context canceled") {
				return
			}
			c.errChan <- fmt.Errorf("error decoding stats: %w", err)
			if !firstSampleReceived {
				close(c.firstSampleChan)
			}
			return
		}

		// Calculate memory usage excluding cache
		memoryUsage := v.MemoryStats.Usage - v.MemoryStats.Stats["cache"]

		if memoryUsage > c.maxMemoryUsage {
			atomic.StoreUint64(&c.maxMemoryUsage, memoryUsage)
		}

		if !firstSampleReceived {
			// Signal that the first sample has been received
			close(c.firstSampleChan)
			firstSampleReceived = true
		}
	}
}

// getContainerID retrieves the container ID based on the container name.
func (c *DockerMemoryCollector) getContainerID(cli *client.Client) (string, error) {
	containers, err := cli.ContainerList(c.ctx, container.ListOptions{All: true})
	if err != nil {
		return "", err
	}
	for _, container := range containers {
		for _, name := range container.Names {
			// Trim leading '/' from container names
			if strings.TrimPrefix(name, "/") == c.containerName {
				return container.ID, nil
			}
		}
	}
	return "", fmt.Errorf("container %s not found", c.containerName)
}

// WaitForFirstSample waits until the first stats sample has been received.
func (c *DockerMemoryCollector) WaitForFirstSample() error {
	// Check for errors that might have occurred
	select {
	case err := <-c.errChan:
		return err
	case <-c.firstSampleChan:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

// GetMaxMemoryUsage returns the maximum memory usage observed during the collection period.
func (c *DockerMemoryCollector) GetMaxMemoryUsage() (uint64, error) {
	// Check for errors that might have occurred in the goroutine.
	select {
	case err := <-c.errChan:
		return 0, err
	default:
	}

	return atomic.LoadUint64(&c.maxMemoryUsage), nil
}

// Stop stops the memory collector and waits for it to finish.
func (c *DockerMemoryCollector) Stop() error {
	c.cancel()
	c.wg.Wait()
	// Check for errors that might have occurred in the goroutine.
	select {
	case err := <-c.errChan:
		return err
	default:
	}
	return nil
}
