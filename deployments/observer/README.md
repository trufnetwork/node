# Observer

Observer is a monitoring system for development and production environments.

## Development

Run the development setup:

```bash
task observer-dev
```

This launches Vector, Prometheus, and Grafana using Docker Compose.

### Components

- **Vector**: Collects host metrics and logs
- **Prometheus**: Scrapes metrics from Vector (dev only)
- **Grafana**: Visualizes metrics from Prometheus (dev only)

### Ports

- **Prometheus**: 9090
- **Grafana**: 3000 (default admin password: `admin`)

## Production

Uses Vector to send metrics and logs directly to Datadog.

### Environment Variables

- `DATADOG_API_KEY`: Datadog API key
- `DATADOG_NAMESPACE`: Datadog namespace
- `DATADOG_ENDPOINT`: Datadog endpoint

### Components

- **Vector**: Collects host metrics and logs, sends them to Datadog

### Logs Collection

- **Journald**: Vector is configured to collect logs from `journald` and forward them to Datadog logs in production, or to console in development.