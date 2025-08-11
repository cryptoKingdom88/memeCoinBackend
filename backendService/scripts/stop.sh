#!/bin/bash

# Backend WebSocket Service Shutdown Script
# This script gracefully stops the backend WebSocket service

set -e

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

# Configuration
SERVICE_NAME="backendService"
SHUTDOWN_TIMEOUT=${SHUTDOWN_TIMEOUT:-30}
FORCE_KILL_TIMEOUT=${FORCE_KILL_TIMEOUT:-10}

# Find running processes
find_processes() {
    pgrep -f "$SERVICE_NAME" 2>/dev/null || true
}

# Send graceful shutdown signal
graceful_shutdown() {
    local pids=($1)
    
    if [ ${#pids[@]} -eq 0 ]; then
        log "No running $SERVICE_NAME processes found"
        return 0
    fi
    
    log "Found ${#pids[@]} running $SERVICE_NAME process(es): ${pids[*]}"
    
    # Send SIGTERM for graceful shutdown
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log "Sending SIGTERM to process $pid"
            kill -TERM "$pid"
        fi
    done
    
    # Wait for processes to terminate gracefully
    log "Waiting up to ${SHUTDOWN_TIMEOUT}s for graceful shutdown..."
    local count=0
    while [ $count -lt $SHUTDOWN_TIMEOUT ]; do
        local running_pids=($(find_processes))
        if [ ${#running_pids[@]} -eq 0 ]; then
            success "All processes terminated gracefully"
            return 0
        fi
        
        sleep 1
        count=$((count + 1))
        
        # Show progress every 5 seconds
        if [ $((count % 5)) -eq 0 ]; then
            log "Still waiting... (${count}s/${SHUTDOWN_TIMEOUT}s)"
        fi
    done
    
    return 1
}

# Force kill processes
force_kill() {
    local pids=($(find_processes))
    
    if [ ${#pids[@]} -eq 0 ]; then
        return 0
    fi
    
    warn "Graceful shutdown timeout exceeded, forcing termination"
    
    # Send SIGKILL
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log "Sending SIGKILL to process $pid"
            kill -KILL "$pid"
        fi
    done
    
    # Wait for force kill to complete
    log "Waiting up to ${FORCE_KILL_TIMEOUT}s for force termination..."
    local count=0
    while [ $count -lt $FORCE_KILL_TIMEOUT ]; do
        local running_pids=($(find_processes))
        if [ ${#running_pids[@]} -eq 0 ]; then
            warn "All processes force terminated"
            return 0
        fi
        
        sleep 1
        count=$((count + 1))
    done
    
    error "Failed to terminate all processes"
    return 1
}

# Check service status
check_status() {
    local pids=($(find_processes))
    
    if [ ${#pids[@]} -eq 0 ]; then
        log "Service is not running"
        return 1
    else
        log "Service is running with PID(s): ${pids[*]}"
        
        # Show process details
        for pid in "${pids[@]}"; do
            if ps -p "$pid" -o pid,ppid,cmd --no-headers 2>/dev/null; then
                :
            fi
        done
        return 0
    fi
}

# Health check
health_check() {
    local port=${PORT:-8080}
    
    log "Performing health check on port $port..."
    
    if command -v curl &> /dev/null; then
        if curl -s -f "http://localhost:$port/health" > /dev/null; then
            success "Service is healthy"
            return 0
        else
            warn "Service health check failed"
            return 1
        fi
    else
        warn "curl not available, skipping health check"
        return 0
    fi
}

# Main shutdown function
shutdown_service() {
    log "Initiating shutdown of $SERVICE_NAME..."
    
    local pids=($(find_processes))
    
    if graceful_shutdown "${pids[*]}"; then
        success "Service shutdown completed successfully"
        return 0
    else
        if force_kill; then
            warn "Service shutdown completed with force termination"
            return 0
        else
            error "Failed to shutdown service"
            return 1
        fi
    fi
}

# Handle script arguments
case "${1:-}" in
    --status)
        check_status
        ;;
    --health)
        health_check
        ;;
    --force)
        log "Force shutdown requested"
        SHUTDOWN_TIMEOUT=0
        shutdown_service
        ;;
    --help|-h)
        echo "Backend WebSocket Service Shutdown Script"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --status        Check service status"
        echo "  --health        Perform health check"
        echo "  --force         Force immediate shutdown (no graceful period)"
        echo "  --help, -h      Show this help message"
        echo ""
        echo "Environment Variables:"
        echo "  SHUTDOWN_TIMEOUT      Graceful shutdown timeout in seconds (default: 30)"
        echo "  FORCE_KILL_TIMEOUT    Force kill timeout in seconds (default: 10)"
        echo "  PORT                  Service port for health check (default: 8080)"
        ;;
    *)
        shutdown_service
        ;;
esac