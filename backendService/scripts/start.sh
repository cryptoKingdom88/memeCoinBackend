#!/bin/bash

# Backend WebSocket Service Startup Script
# This script starts the backend WebSocket service with proper configuration

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Check if Go is installed
check_go() {
    if ! command -v go &> /dev/null; then
        error "Go is not installed. Please install Go 1.23 or later."
        exit 1
    fi
    
    GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
    log "Go version: $GO_VERSION"
}

# Check if required environment variables are set
check_environment() {
    log "Checking environment configuration..."
    
    # Load .env file if it exists
    if [ -f "$PROJECT_DIR/.env" ]; then
        log "Loading environment from .env file"
        export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
    else
        warn "No .env file found, using system environment variables"
    fi
    
    # Check required variables
    REQUIRED_VARS=("KAFKA_BROKERS")
    for var in "${REQUIRED_VARS[@]}"; do
        if [ -z "${!var}" ]; then
            warn "$var is not set, using default value"
        else
            log "$var is set"
        fi
    done
}

# Build the application
build_app() {
    log "Building application..."
    cd "$PROJECT_DIR"
    
    # Generate Swagger documentation
    if command -v swag &> /dev/null; then
        log "Generating Swagger documentation..."
        swag init -g main.go --output ./docs
    else
        warn "swag command not found, skipping documentation generation"
        warn "Install with: go install github.com/swaggo/swag/cmd/swag@latest"
    fi
    
    # Build the application
    go build -o backendService .
    if [ $? -eq 0 ]; then
        success "Application built successfully"
    else
        error "Failed to build application"
        exit 1
    fi
}

# Check if Kafka is accessible
check_kafka() {
    log "Checking Kafka connectivity..."
    
    KAFKA_BROKERS=${KAFKA_BROKERS:-"localhost:9092"}
    IFS=',' read -ra BROKERS <<< "$KAFKA_BROKERS"
    
    for broker in "${BROKERS[@]}"; do
        if timeout 5 bash -c "</dev/tcp/${broker%:*}/${broker#*:}" 2>/dev/null; then
            success "Kafka broker $broker is accessible"
        else
            warn "Kafka broker $broker is not accessible"
        fi
    done
}

# Start the service
start_service() {
    log "Starting backend WebSocket service..."
    
    # Set default values if not provided
    export PORT=${PORT:-8080}
    export LOG_LEVEL=${LOG_LEVEL:-INFO}
    export LOG_FORMAT=${LOG_FORMAT:-json}
    
    log "Configuration:"
    log "  Port: $PORT"
    log "  Log Level: $LOG_LEVEL"
    log "  Log Format: $LOG_FORMAT"
    log "  Kafka Brokers: ${KAFKA_BROKERS:-localhost:9092}"
    
    # Start the service
    cd "$PROJECT_DIR"
    exec ./backendService
}

# Main execution
main() {
    log "Starting Backend WebSocket Service..."
    
    check_go
    check_environment
    build_app
    check_kafka
    start_service
}

# Handle script arguments
case "${1:-}" in
    --build-only)
        check_go
        build_app
        success "Build completed"
        ;;
    --check-only)
        check_go
        check_environment
        check_kafka
        success "All checks passed"
        ;;
    --help|-h)
        echo "Backend WebSocket Service Startup Script"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --build-only    Build the application only"
        echo "  --check-only    Run checks only (Go, environment, Kafka)"
        echo "  --help, -h      Show this help message"
        echo ""
        echo "Environment Variables:"
        echo "  PORT                   Server port (default: 8080)"
        echo "  LOG_LEVEL             Log level (default: INFO)"
        echo "  LOG_FORMAT            Log format (default: json)"
        echo "  KAFKA_BROKERS         Kafka broker addresses (default: localhost:9092)"
        echo "  KAFKA_CONSUMER_GROUP  Kafka consumer group (default: backend-service)"
        ;;
    *)
        main
        ;;
esac