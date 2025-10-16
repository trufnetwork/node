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
DOCKER_IMAGE="ghcr.io/trufnetwork/node:attestation-test"
COMPOSE_PROJECT="tn-attestation-e2e"

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

# Function to clean up containers
cleanup_containers() {
    print_status "Cleaning up any existing containers..."
    cd "$SCRIPT_DIR"
    docker compose -p "$COMPOSE_PROJECT" down --remove-orphans 2>/dev/null || true
}

# Function to build Docker image
build_docker_image() {
    print_status "Building Docker image with tn_attestation extension..."
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
    print_status "Running attestation E2E tests..."
    cd "$SCRIPT_DIR"
    
    # Run the Go tests without cache
    go test -v -count=1 -run TestAttestationE2E ./... || {
        print_error "Tests failed"
        return 1
    }
    
    print_status "Tests completed successfully"
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
            
            # Clean up any existing containers first
            cleanup_containers
            
            # Build and start everything
            build_docker_image
            start_services
            
            # Run migrations
            run_migrations
            
            # Run tests
            print_status "Waiting 5 seconds for attestation extension to initialize..."
            sleep 5
            
            run_tests
            
            # Clean up on success
            print_status "Test completed successfully, cleaning up..."
            stop_services
            ;;
    esac
}

# Run main function
main "$@"

