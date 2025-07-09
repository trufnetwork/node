# TN Cache Metrics Test Environment

This directory contains a complete test environment for validating the OpenTelemetry metrics implementation in the `tn_cache` extension.

## Overview

The test environment sets up:
- A TrufNetwork node with `tn_cache` extension enabled
- OpenTelemetry Collector to receive metrics
- Prometheus to store metrics
- Grafana with pre-configured dashboard
- Automated tests to generate all metric types

## Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Network access to pull Docker images

## Quick Start

1. Run the test environment:
   ```bash
   ./test_tn_cache_metrics.sh
   ```

2. Access services:
   - **Grafana**: http://localhost:3000 (admin/admin)
   - **Prometheus**: http://localhost:9090
   - **Kwild RPC**: http://localhost:8484

3. View metrics in Grafana:
   - Login with admin/admin
   - Navigate to Dashboards → TN Cache Metrics

## What Gets Tested

The test suite generates:
- **Cache hits and misses** through queries with `use_cache=true`
- **Refresh metrics** via scheduled cron jobs (10-20 second intervals)
- **Error metrics** through invalid queries
- **Circuit breaker events** via high load scenarios
- **Resource metrics** tracking active streams and cache size

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   kwild     │────▶│ OTEL         │────▶│ Prometheus   │
│ (tn_cache)  │     │ Collector    │     │              │
└─────────────┘     └──────────────┘     └──────┬───────┘
                                                 │
┌─────────────┐     ┌──────────────┐            │
│ Test Suite  │     │   Grafana    │◀───────────┘
│   (Go)      │     │              │
└─────────────┘     └──────────────┘
```

## Configuration

### Cache Streams
The test configures 3 streams with different refresh schedules:
- `TEST_STREAM_1`: Refreshes every 10 seconds
- `TEST_STREAM_2`: Refreshes every 15 seconds  
- `TEST_STREAM_3`: Refreshes every 20 seconds

### Metrics Collected
- `tn_cache.hits` - Cache hit counter
- `tn_cache.misses` - Cache miss counter
- `tn_cache.refresh.duration` - Refresh time histogram
- `tn_cache.refresh.errors` - Error counter by type
- `tn_cache.circuit_breaker.state` - Circuit breaker state gauge
- `tn_cache.events.total` - Total cached events

## Development

### Running Tests Only
```bash
./test_tn_cache_metrics.sh test-only
```

### Viewing Logs
```bash
./test_tn_cache_metrics.sh logs
```

### Stopping Services
```bash
./test_tn_cache_metrics.sh stop
```

## Troubleshooting

1. **Services fail to start**: Check Docker daemon is running
2. **Metrics not appearing**: Wait 30-60 seconds for initial data
3. **Port conflicts**: Ensure ports 3000, 8484, 9090, 9464 are free
4. **Build failures**: Ensure you're in the correct directory

## Files

- `test_tn_cache_metrics.sh` - Main orchestration script
- `docker-compose.yml` - Service definitions
- `cache_metrics_test.go` - Test scenarios
- `configs/` - Configuration files for all services
- `schemas/` - Test Kuneiform schemas (embedded in Go test)