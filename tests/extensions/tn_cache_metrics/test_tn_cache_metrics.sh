#!/bin/bash
set -e

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DOCKER_IMAGE="tn-db:metrics-test"
COMPOSE_PROJECT="tn-cache-metrics"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[*]${NC} $1"
}

print_error() {
    echo -e "${RED}[!]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# Function to build Docker image
build_docker_image() {
    print_status "Building Docker image with tn_cache extension..."
    cd "$PROJECT_ROOT"
    
    docker build -t "$DOCKER_IMAGE" -f deployments/Dockerfile . || {
        print_error "Failed to build Docker image"
        exit 1
    }
    
    print_status "Docker image built successfully"
}

# Function to start services
start_services() {
    print_status "Starting services with docker compose..."
    cd "$SCRIPT_DIR"
    
    docker compose -p "$COMPOSE_PROJECT" up -d || {
        print_error "Failed to start services"
        exit 1
    }
    
    print_status "Waiting for services to be ready..."
    
    # Wait for kwild to be ready
    print_status "Waiting for kwild..."
    for i in {1..60}; do
        if docker compose -p "$COMPOSE_PROJECT" logs kwild 2>&1 | grep -q "JSON-RPC server listening"; then
            print_status "kwild is ready"
            break
        fi
        if [ $i -eq 60 ]; then
            print_error "Timeout waiting for kwild to be ready"
            docker compose -p "$COMPOSE_PROJECT" logs kwild
            exit 1
        fi
        sleep 2
    done
    
    # Wait for Grafana to be ready
    print_status "Waiting for Grafana..."
    for i in {1..30}; do
        if curl -s http://localhost:3000/api/health | grep -q "ok"; then
            print_status "Grafana is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "Timeout waiting for Grafana to be ready"
            exit 1
        fi
        sleep 2
    done
    
    print_status "All services are ready"
}

# Function to run migrations
run_migrations() {
    print_status "Running TrufNetwork migrations..."
    cd "$PROJECT_ROOT"
    
    # Run the dev migration
    task action:migrate:dev || {
        print_error "Failed to run migrations"
        return 1
    }
    
    print_status "Migrations completed successfully"
}

# Function to run tests
run_tests() {
    print_status "Running cache metrics tests..."
    cd "$SCRIPT_DIR"
    
    # Run the Go test
    go test -v -run TestCacheMetrics ./... || {
        print_error "Tests failed"
        return 1
    }
    
    print_status "Tests completed successfully"
}

# Function to show service URLs
show_urls() {
    echo ""
    print_status "Services are running at:"
    echo "  - Kwild RPC:    http://localhost:8484"
    echo "  - Prometheus:   http://localhost:9090"
    echo "  - Grafana:      http://localhost:3000 (admin/admin)"
    echo ""
    print_status "To view cache metrics:"
    echo "  1. Open Grafana at http://localhost:3000"
    echo "  2. Login with admin/admin"
    echo "  3. Navigate to Dashboards → TN Cache Metrics"
    echo ""
}

# Function to follow logs
follow_logs() {
    print_status "Following service logs (Ctrl+C to stop)..."
    docker compose -p "$COMPOSE_PROJECT" logs -f
}

# Function to stop services
stop_services() {
    print_status "Stopping services..."
    cd "$SCRIPT_DIR"
    docker compose -p "$COMPOSE_PROJECT" down -v
    print_status "Services stopped and volumes removed"
}

# Cleanup function
cleanup() {
    echo ""
    print_warning "Interrupt received, cleaning up..."
    stop_services
    exit 0
}

# Main script logic
main() {
    case "${1:-}" in
        stop)
            stop_services
            ;;
        logs)
            follow_logs
            ;;
        test-only)
            run_tests
            ;;
        *)
            # Set up trap for cleanup
            trap cleanup INT TERM
            
            # Build and start everything
            build_docker_image
            start_services
            
            # Run migrations
            run_migrations
            
            # Run tests
            print_status "Waiting 10 seconds for initial cache population..."
            sleep 10
            
            run_tests
            
            # Show URLs and wait
            show_urls
            
            print_status "Services are running. Press Ctrl+C to stop and cleanup."
            print_status "Run '$0 logs' in another terminal to see service logs."
            
            # Keep script running
            while true; do
                sleep 1
            done
            ;;
    esac
}

# Run main function
main "$@"