# Design Document

## Overview

The backendService is a lightweight Go-based WebSocket server that bridges Kafka data streams with frontend clients. It follows a simple pub-sub pattern where Kafka consumers feed data into a WebSocket broadcaster that distributes messages to subscribed clients.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka Topics  │───▶│  backendService  │───▶│  Frontend Apps  │
│                 │    │                  │    │                 │
│ • token-info    │    │ ┌──────────────┐ │    │ • Dashboard     │
│ • trade-info    │    │ │ Kafka        │ │    │ • Mobile App    │
│ • aggregate-info│    │ │ Consumers    │ │    │ • Analytics     │
└─────────────────┘    │ └──────────────┘ │    └─────────────────┘
                       │ ┌──────────────┐ │
                       │ │ WebSocket    │ │
                       │ │ Server       │ │
                       │ └──────────────┘ │
                       │ ┌──────────────┐ │
                       │ │ HTTP Server  │ │
                       │ │ (Swagger)    │ │
                       │ └──────────────┘ │
                       └──────────────────┘
```

## Components and Interfaces

### 1. WebSocket Server
- **Port**: 8080 (configurable)
- **Endpoint**: `/ws`
- **Protocol**: WebSocket (RFC 6455)
- **Subscription**: Clients send `{"type": "subscribe", "channel": "dashboard"}` to receive data

### 2. Kafka Consumers
- **Topics**: 
  - `token-info` - New token information
  - `trade-info` - Real-time trade data
  - `aggregate-info` - Aggregated trading metrics
- **Consumer Group**: `backend-service`
- **Auto-commit**: Enabled for simplicity

### 3. HTTP Server
- **Port**: 8080 (same as WebSocket)
- **Endpoints**:
  - `GET /docs` - Swagger UI documentation
  - `GET /health` - Health check endpoint
  - `GET /ws` - WebSocket upgrade endpoint

### 4. Message Broadcaster
- **Pattern**: Fan-out to all subscribed clients
- **Buffer**: In-memory channel with configurable size
- **Error Handling**: Failed sends remove client from subscriber list

## Data Models

### WebSocket Message Format
```go
type WebSocketMessage struct {
    Type      string      `json:"type"`      // "token_info", "trade_data", "aggregate_data"
    Channel   string      `json:"channel"`   // "dashboard"
    Data      interface{} `json:"data"`      // Actual payload
    Timestamp string      `json:"timestamp"` // ISO 8601 format
}
```

### Subscription Message
```go
type SubscriptionMessage struct {
    Type    string `json:"type"`    // "subscribe" or "unsubscribe"
    Channel string `json:"channel"` // "dashboard"
}
```

### Client Connection
```go
type Client struct {
    ID       string          `json:"id"`
    Conn     *websocket.Conn `json:"-"`
    Send     chan []byte     `json:"-"`
    Channels map[string]bool `json:"channels"`
}
```

## Error Handling

### WebSocket Errors
- **Connection Lost**: Remove client from subscriber list
- **Invalid JSON**: Send error message to client
- **Send Buffer Full**: Drop oldest messages (FIFO)

### Kafka Errors
- **Connection Lost**: Retry with exponential backoff
- **Deserialization Error**: Log error and continue processing
- **Consumer Lag**: Monitor and alert if lag exceeds threshold

### HTTP Errors
- **404**: Return standard not found response
- **500**: Log error and return generic error message

## Testing Strategy

### Unit Tests
- WebSocket message formatting
- Client subscription/unsubscription logic
- Kafka message parsing

### Integration Tests
- End-to-end WebSocket communication
- Kafka consumer integration
- Multiple client scenarios

### Performance Tests
- Concurrent client connections (target: 1000+ clients)
- Message throughput (target: 10,000+ messages/second)
- Memory usage under load

## Configuration

### Environment Variables
```bash
# Server Configuration
PORT=8080
WS_READ_BUFFER_SIZE=1024
WS_WRITE_BUFFER_SIZE=1024

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP=backend-service

# Performance Tuning
MAX_CLIENTS=1000
MESSAGE_BUFFER_SIZE=100
KAFKA_BATCH_SIZE=100
```

### Swagger Documentation
- **Framework**: Gin + Swaggo
- **WebSocket Documentation**: Custom documentation for WebSocket endpoints
- **Message Examples**: Sample JSON for all message types
- **Interactive Testing**: WebSocket test client in Swagger UI

## Deployment Considerations

### Docker Support
- Multi-stage build for minimal image size
- Health check endpoint for container orchestration
- Graceful shutdown handling

### Monitoring
- Prometheus metrics for client connections and message rates
- Health check endpoint for load balancer integration
- Structured logging with correlation IDs

### Security
- CORS configuration for web clients
- Rate limiting per client connection
- Input validation for all WebSocket messages