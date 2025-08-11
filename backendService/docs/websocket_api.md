# WebSocket API Documentation

## Overview

The Backend WebSocket Service provides real-time cryptocurrency data streaming through WebSocket connections. Clients can subscribe to the "dashboard" channel to receive live updates about token information, trade data, and aggregated metrics.

## Connection

### Endpoint
```
ws://localhost:8080/ws
```

### Protocol
- WebSocket (RFC 6455)
- JSON message format
- UTF-8 encoding

## Authentication
No authentication required for this service.

## Subscription Flow

### 1. Connect to WebSocket
```javascript
const ws = new WebSocket('ws://localhost:8080/ws');
```

### 2. Subscribe to Dashboard Channel
Send a subscription message after connection is established:

```json
{
  "type": "subscribe",
  "channel": "dashboard"
}
```

### 3. Receive Data
Once subscribed, you'll receive real-time data messages in the following format:

```json
{
  "type": "token_info|trade_data|aggregate_data",
  "channel": "dashboard",
  "data": { /* message-specific data */ },
  "timestamp": "2024-01-01T12:00:00Z"
}
```

## Message Types

### Token Information (`token_info`)
Broadcasted when new token information is available.

```json
{
  "type": "token_info",
  "channel": "dashboard",
  "data": {
    "Token": "0x0000000000000000000000000000000000000001",
    "Symbol": "DOGCOI",
    "Name": "DogeCoin",
    "MetaInfo": "Initial price: $1.234567",
    "TotalSupply": "5000000000",
    "IsAMM": true,
    "CreateTime": "2024-01-01T12:00:00Z"
  },
  "timestamp": "2024-01-01T12:00:00Z"
}
```

### Trade Data (`trade_data`)
Broadcasted for each new trade transaction.

```json
{
  "type": "trade_data",
  "channel": "dashboard",
  "data": {
    "Token": "0x0000000000000000000000000000000000000001",
    "Wallet": "0x0000000000000000000000000000000000000400",
    "SellBuy": "buy",
    "NativeAmount": "1000.000000",
    "TokenAmount": "2500.500000",
    "PriceUsd": "0.400000",
    "TransTime": "2024-01-01T12:00:00Z",
    "TxHash": "0x1234567890abcdef1234567890abcdef12345678"
  },
  "timestamp": "2024-01-01T12:00:00Z"
}
```

### Aggregate Data (`aggregate_data`)
Broadcasted with aggregated trading metrics over time windows.

```json
{
  "type": "aggregate_data",
  "channel": "dashboard",
  "data": {
    "token": "0x0000000000000000000000000000000000000001",
    "symbol": "DOGCOI",
    "name": "DogeCoin",
    "aggregate_data": [
      {
        "time_window": "1min",
        "sell_count": 15,
        "buy_count": 20,
        "total_trades": 35,
        "sell_volume": "25000.000000",
        "buy_volume": "30000.000000",
        "total_volume": "55000.000000",
        "volume_usd": "55000.000000",
        "price_change": 2.5,
        "open_price": "0.400000",
        "close_price": "0.410000",
        "timestamp": "2024-01-01T12:01:00Z"
      }
    ],
    "generated_at": "2024-01-01T12:01:00Z",
    "version": "1.0"
  },
  "timestamp": "2024-01-01T12:01:00Z"
}
```

## Error Handling

### Invalid Subscription
If you send an invalid subscription message, you'll receive an error:

```json
{
  "error": "Invalid subscription channel",
  "code": 400,
  "message": "Only 'dashboard' channel is supported"
}
```

### Connection Errors
- **Connection Lost**: Client will be automatically removed from subscribers
- **Invalid JSON**: Error message sent to client
- **Send Buffer Full**: Oldest messages are dropped (FIFO)

## Client Implementation Examples

### JavaScript/Browser
```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

ws.onopen = function() {
    console.log('Connected to WebSocket');
    // Subscribe to dashboard
    ws.send(JSON.stringify({
        type: 'subscribe',
        channel: 'dashboard'
    }));
};

ws.onmessage = function(event) {
    const message = JSON.parse(event.data);
    console.log('Received:', message.type, message.data);
    
    switch(message.type) {
        case 'token_info':
            handleTokenInfo(message.data);
            break;
        case 'trade_data':
            handleTradeData(message.data);
            break;
        case 'aggregate_data':
            handleAggregateData(message.data);
            break;
    }
};

ws.onerror = function(error) {
    console.error('WebSocket error:', error);
};

ws.onclose = function() {
    console.log('WebSocket connection closed');
};
```

### Node.js
```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080/ws');

ws.on('open', function() {
    console.log('Connected to WebSocket');
    ws.send(JSON.stringify({
        type: 'subscribe',
        channel: 'dashboard'
    }));
});

ws.on('message', function(data) {
    const message = JSON.parse(data);
    console.log('Received:', message.type, message.data);
});
```

### Python
```python
import asyncio
import websockets
import json

async def client():
    uri = "ws://localhost:8080/ws"
    async with websockets.connect(uri) as websocket:
        # Subscribe to dashboard
        await websocket.send(json.dumps({
            "type": "subscribe",
            "channel": "dashboard"
        }))
        
        # Listen for messages
        async for message in websocket:
            data = json.loads(message)
            print(f"Received: {data['type']}", data['data'])

asyncio.run(client())
```

## Rate Limits
- No explicit rate limits on connections
- Message buffer size: 100 messages per client
- Maximum concurrent clients: 1000 (configurable)

## Monitoring
- Health check endpoint: `GET /health`
- Metrics include active client count and subscribed channels