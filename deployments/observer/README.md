# Observer

Observer is a monitoring system for development and production environments.

## Development

Run the development setup:

```bash
task observer-dev
```

This launches Vector, Prometheus, and Grafana using Docker Compose.

### Components

- Vector: Collects host metrics
- Prometheus: Scrapes metrics from Vector (dev only)
- Grafana: Visualizes metrics from Prometheus (dev only)

### Ports

- Prometheus: 9090
- Grafana: 3000 (default admin password: `admin`)

## Production

Uses Vector to send metrics directly to Datadog.

### Environment Variables

- DATADOG_API_KEY
- DATADOG_NAMESPACE
- DATADOG_ENDPOINT

## Notes

- Metrics are collected every 15 seconds in development
- Production setup uses Datadog for visualization and alerting