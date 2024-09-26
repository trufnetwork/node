package benchutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerMemoryCollector collects memory usage stats of a Docker container.
type DockerMemoryCollector struct {
	containerName  string
	maxMemoryUsage uint64
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.Mutex
	errChan        chan error
}

// StartDockerMemoryCollector initializes and starts the memory collector.
func StartDockerMemoryCollector(containerName string) (*DockerMemoryCollector, error) {
	ctx, cancel := context.WithCancel(context.Background())
	collector := &DockerMemoryCollector{
		containerName: containerName,
		ctx:           ctx,
		cancel:        cancel,
		errChan:       make(chan error, 1),
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
		return
	}
	defer cli.Close()
	cli.NegotiateAPIVersion(c.ctx)

	containerID, err := c.getContainerID(cli)
	if err != nil {
		c.errChan <- fmt.Errorf("error getting container ID: %w", err)
		return
	}

	stats, err := cli.ContainerStats(c.ctx, containerID, true) // stream=true
	if err != nil {
		c.errChan <- fmt.Errorf("error getting container stats: %w", err)
		return
	}
	defer stats.Body.Close()

	decoder := json.NewDecoder(stats.Body)
	for {
		var v *container.StatsResponse
		if err := decoder.Decode(&v); err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "context canceled") {
				return
			}
			c.errChan <- fmt.Errorf("error decoding stats: %w", err)
			return
		}

		memoryUsage := v.MemoryStats.Usage - v.MemoryStats.Stats["cache"]

		c.mu.Lock()
		if memoryUsage > c.maxMemoryUsage {
			c.maxMemoryUsage = memoryUsage
		}
		c.mu.Unlock()
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

// GetMaxMemoryUsage returns the maximum memory usage observed during the collection period.
func (c *DockerMemoryCollector) GetMaxMemoryUsage() (uint64, error) {
	// Check for errors that might have occurred in the goroutine.
	select {
	case err := <-c.errChan:
		return 0, err
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxMemoryUsage, nil
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
