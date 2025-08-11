# Requirements Document

## Introduction

The backendService is a simple WebSocket server that acts as a real-time data bridge between Kafka topics and frontend clients. It consumes data from multiple Kafka topics (token info, trade history, and aggregate data) and broadcasts this information to subscribed WebSocket clients.

## Requirements

### Requirement 1

**User Story:** As a frontend developer, I want to connect to a WebSocket server and subscribe to "dashboard" to receive real-time cryptocurrency data, so that I can display live updates to users.

#### Acceptance Criteria

1. WHEN a client connects to the WebSocket server THEN the connection SHALL be established successfully
2. WHEN a client sends a "dashboard" subscription message THEN the client SHALL be added to the subscriber list
3. WHEN subscribed clients exist THEN all incoming Kafka data SHALL be forwarded to them in real-time

### Requirement 2

**User Story:** As a backend service, I want to consume data from three Kafka topics (token-info, trade-info, aggregate-info), so that I can provide comprehensive real-time data to frontend clients.

#### Acceptance Criteria

1. WHEN the service starts THEN it SHALL connect to all three Kafka topics successfully
2. WHEN new token information is received THEN it SHALL be formatted and sent to subscribed clients
3. WHEN new trade data is received THEN it SHALL be formatted and sent to subscribed clients
4. WHEN new aggregate data is received THEN it SHALL be formatted and sent to subscribed clients

### Requirement 3

**User Story:** As a system administrator, I want the WebSocket server to handle client connections gracefully, so that the service remains stable under various connection scenarios.

#### Acceptance Criteria

1. WHEN a client disconnects THEN the client SHALL be removed from the subscriber list automatically
2. WHEN multiple clients are connected THEN each SHALL receive the same broadcast data
3. WHEN no clients are subscribed THEN Kafka data SHALL still be consumed but not broadcast
4. WHEN the service shuts down THEN all WebSocket connections SHALL be closed gracefully

### Requirement 4

**User Story:** As a frontend client, I want to receive structured JSON messages over WebSocket, so that I can easily parse and display the data.

#### Acceptance Criteria

1. WHEN token info is broadcast THEN it SHALL include message type and token details
2. WHEN trade data is broadcast THEN it SHALL include message type and trade information
3. WHEN aggregate data is broadcast THEN it SHALL include message type and aggregated metrics
4. WHEN any data is sent THEN it SHALL be valid JSON format with consistent structure

### Requirement 5

**User Story:** As a developer, I want comprehensive API documentation for the WebSocket endpoints and message formats, so that I can integrate with the service effectively.

#### Acceptance Criteria

1. WHEN the service is running THEN it SHALL serve Swagger/OpenAPI documentation at /docs endpoint
2. WHEN accessing the documentation THEN it SHALL include WebSocket connection details
3. WHEN viewing message formats THEN it SHALL show all possible message types and their schemas
4. WHEN examining subscription flow THEN it SHALL document the "dashboard" subscription process

