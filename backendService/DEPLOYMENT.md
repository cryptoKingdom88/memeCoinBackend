# Backend WebSocket Service - Deployment Guide

This guide covers deployment, configuration, and operational aspects of the Backend WebSocket Service.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Deployment Options](#deployment-options)
- [Monitoring and Logging](#monitoring-and-logging)
- [Troubleshooting](#troubleshooting)
- [Production Considerations](#production-considerations)

## Quick Start

### Prerequisites

- Go 1.23 or later
- Kafka cluster (for production) or Docker (for development)
- Port 8080 available (configurable)

### Development Setup

1. **Clone and setup**:
   ```bash
   cd backendService
   make dev-setup
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

3. **Start with Docker Compose** (includes Kafka):
   ```bash
   make docker-run
   ```

4. **Or start manually**:
   ```bash
   make start
   ```

5. **Access the service**:
   - WebSocket: `ws://localhost:8080/ws`
   - Health Check: `http://localhost:8080/health`
   - Documentation: `http://localhost:8080/docs`

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `WS_READ_BUFFER_SIZE` | `1024` | WebSocket read buffer size |
| `WS_WRITE_BUFFER_SIZE` | `1024` | WebSocket write buffer size |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP` | `backend-service` | Kafka consumer group ID |
| `MAX_CLIENTS` | `1000` | Maximum concurrent WebSocket clients |
| `MESSAGE_BUFFER_SIZE` | `100` | Message buffer size per client |
| `KAFKA_BATCH_SIZE` | `100` | Kafka consumer batch size |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARN, ERROR, FATAL) |
| `LOG_FORMAT` | `json` | Log format (json, text) |

### Configuration Files

- `.env` - Environment variables (not committed)
- `.env.example` - Example configuration
- `config/config.go` - Configuration loading logic

## Deployment Options

### 1. Docker Deployment (Recommended)

#### Development with included Kafka:
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f backend-websocket

# Stop services
docker-compose down
```

#### Production with external Kafka:
```bash
# Set production environment variables
export KAFKA_BROKERS="prod-kafka-1:9092,prod-kafka-2:9092"
export LOG_LEVEL="WARN"

# Start production services
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### 2. Binary Deployment

#### Build and deploy:
```bash
# Build
make build

# Copy binary and start script to target server
scp backendService scripts/start.sh user@server:/opt/backend-service/

# On target server
cd /opt/backend-service
./start.sh
```

#### Using systemd (Linux):
```bash
# Create systemd service file
sudo tee /etc/systemd/system/backend-websocket.service > /dev/null <<EOF
[Unit]
Description=Backend WebSocket Service
After=network.target

[Service]
Type=simple
User=backend
WorkingDirectory=/opt/backend-service
ExecStart=/opt/backend-service/backendService
ExecStop=/opt/backend-service/scripts/stop.sh
Restart=always
RestartSec=5
Environment=LOG_LEVEL=INFO
Environment=KAFKA_BROKERS=localhost:9092

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
sudo systemctl enable backend-websocket
sudo systemctl start backend-websocket
```

### 3. Kubernetes Deployment

#### Basic deployment:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend-websocket
spec:
  replicas: 3
  selector:
    matchLabels:
      app: backend-websocket
  template:
    metadata:
      labels:
        app: backend-websocket
    spec:
      containers:
      - name: backend-websocket
        image: backend-websocket-service:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_BROKERS
          value: "kafka-service:9092"
        - name: LOG_LEVEL
          value: "INFO"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: backend-websocket-service
spec:
  selector:
    app: backend-websocket
  ports:
  - port: 8080
    targetPort: 8080
  type: LoadBalancer
```

## Monitoring and Logging

### Health Checks

The service provides comprehensive health checking:

```bash
# HTTP health check
curl http://localhost:8080/health

# Command-line health check (for Docker)
./backendService --health-check

# Using scripts
./scripts/stop.sh --health
```

### Logging

The service uses structured JSON logging:

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "message": "WebSocket client connected",
  "service": "backend-websocket-service",
  "component": "websocket-handler",
  "fields": {
    "client_id": "client-123",
    "remote_addr": "192.168.1.100:54321"
  }
}
```

#### Log Levels:
- `DEBUG`: Detailed debugging information
- `INFO`: General operational messages
- `WARN`: Warning conditions
- `ERROR`: Error conditions
- `FATAL`: Fatal errors (causes exit)

### Metrics and Monitoring

The service exposes metrics through the health endpoint:

```json
{
  "status": "healthy",
  "clients": 42,
  "channels": ["dashboard"],
  "hub_status": {
    "running": true,
    "uptime": "2h30m15s"
  },
  "timestamp": "2024-01-15T10:30:45Z"
}
```

## Troubleshooting

### Common Issues

#### 1. Service won't start
```bash
# Check configuration
./scripts/start.sh --check-only

# Check logs
journalctl -u backend-websocket -f  # systemd
docker-compose logs backend-websocket  # Docker
```

#### 2. Kafka connection issues
```bash
# Test Kafka connectivity
telnet kafka-broker 9092

# Check Kafka topics
make kafka-topics

# Create required topics
make kafka-create-topics
```

#### 3. WebSocket connection failures
```bash
# Check if service is listening
netstat -tlnp | grep 8080

# Test WebSocket connection
wscat -c ws://localhost:8080/ws
```

#### 4. High memory usage
- Reduce `MAX_CLIENTS`
- Reduce `MESSAGE_BUFFER_SIZE`
- Check for WebSocket connection leaks

#### 5. High CPU usage
- Reduce `KAFKA_BATCH_SIZE`
- Check Kafka consumer lag
- Monitor client connection patterns

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
./backendService
```

### Performance Tuning

#### For high-throughput scenarios:
```bash
export MAX_CLIENTS=5000
export MESSAGE_BUFFER_SIZE=500
export KAFKA_BATCH_SIZE=1000
export WS_READ_BUFFER_SIZE=4096
export WS_WRITE_BUFFER_SIZE=4096
```

#### For low-latency scenarios:
```bash
export MESSAGE_BUFFER_SIZE=10
export KAFKA_BATCH_SIZE=1
```

## Production Considerations

### Security

1. **Network Security**:
   - Use TLS/SSL for WebSocket connections
   - Implement proper firewall rules
   - Use VPN or private networks for Kafka

2. **Authentication** (if needed):
   - Implement JWT token validation
   - Add rate limiting per client
   - Monitor for suspicious connection patterns

### Scalability

1. **Horizontal Scaling**:
   - Deploy multiple instances behind a load balancer
   - Use sticky sessions for WebSocket connections
   - Ensure Kafka consumer group balancing

2. **Vertical Scaling**:
   - Increase `MAX_CLIENTS` based on memory
   - Tune buffer sizes for throughput
   - Monitor resource usage

### Reliability

1. **High Availability**:
   - Deploy across multiple availability zones
   - Use health checks with load balancers
   - Implement circuit breakers for Kafka

2. **Data Consistency**:
   - Monitor Kafka consumer lag
   - Implement dead letter queues
   - Handle duplicate message scenarios

### Monitoring

1. **Application Metrics**:
   - WebSocket connection count
   - Message throughput rates
   - Error rates and types

2. **Infrastructure Metrics**:
   - CPU and memory usage
   - Network I/O
   - Kafka consumer lag

3. **Alerting**:
   - Service health failures
   - High error rates
   - Resource exhaustion

### Backup and Recovery

1. **Configuration Backup**:
   - Version control all configuration
   - Document environment-specific settings

2. **Disaster Recovery**:
   - Automated deployment procedures
   - Database backup strategies (if applicable)
   - Kafka topic recovery procedures

## Support

For issues and questions:

1. Check the logs with appropriate log level
2. Review this deployment guide
3. Check the main README.md for API documentation
4. Use the provided scripts for common operations

## Makefile Commands

Use `make help` to see all available commands:

```bash
make help              # Show available commands
make dev-setup         # Set up development environment
make build             # Build the application
make test              # Run tests
make docker-run        # Run with Docker Compose
make start             # Start using scripts
make stop              # Stop using scripts
make status            # Check service status
make health            # Perform health check
```