# WebSocket Test Connector

Test connector for verifying persistent WebSocket connections in Appmixer. Follows the same architectural pattern as the Kafka connector (live connection pool, sync job, shared connection per flow).

## Architecture

```
┌─── Flow ─────────────��────────────────────────────────────┐
│                                                           │
│  NewMessage (trigger)              SendMessage (action)   │
│  - Creates WS connection           - Reuses the same     │
│    on flow start                     connection           │
│  - Receives incoming messages      - Sends messages       │
│                                                           │
│         ▼                     ▼                           │
│  ┌─────────────────────────────────────────┐             │
│  │  Shared Connection: ws-{flowId}          │             │
���  │  One WebSocket per flow (bidirectional)  │             ��
│  └─���───────────────────────────────────────┘             │
│         ▲                     │                           │
│         │ incoming messages   │ outgoing messages         │
└─────────┼───────────────��─────┼───────────────��───────────┘
          │                     │
          ▼                     ▼
    ┌───────────���─────────────────────┐
    │        WebSocket Server         │
    └───────────���─────────────────────┘
```

## Key Design Decisions

- **Shared connection per flow**: Both NewMessage and SendMessage use the same WebSocket connection (identified by `ws-{flowId}`). This maps to real-world scenarios like WhatsApp where one account = one bidirectional connection.
- **Connection pool on `process` object**: Survives Node.js `require` cache clearing that Appmixer performs.
- **Sync job (every minute)**: Ensures connections are recreated on all cluster nodes and cleaned up when flows stop.
- **Auto-reconnect**: If a connection is lost, `sendMessage` automatically reconnects from persisted state.

## Setup

### 1. Upload the connector

Upload the `websockettest` bundle to your Appmixer instance.

### 2. Start a WebSocket echo server

You need a WS server to connect to. Simplest option:

```bash
npx ws --port 8080
```

Or use a public echo server: `wss://echo.websocket.events`

### 3. Authenticate

In Appmixer, create a new WebSocket Test account:
- **Connection Name**: any descriptive name
- **WebSocket URL**: `ws://localhost:8080` or `wss://echo.websocket.events`

### 4. Create and start a flow

Create a flow with the **NewMessage** trigger component. Start the flow.  
When the flow starts, it opens a persistent WebSocket connection to the configured server.

## Testing with Postman

### Verify the connection is open

```
GET https://<appmixer-api>/plugins/appmixer/websockettest/connections
Authorization: Bearer <your-token>
```

Response:
```json
{
    "count": 1,
    "ids": ["ws-<flowId>"]
}
```

### Send a message through the connection

```
POST https://<appmixer-api>/plugins/appmixer/websockettest/push-message
Authorization: Bearer <your-token>
Content-Type: application/json

{
    "connectionId": "ws-<flowId>",
    "message": "Hello from Postman"
}
```

This sends the message through the existing WebSocket connection to the server.  
If the server is an echo server, it will send the message back on the same connection, and the **NewMessage** trigger will receive it and output it.

### Expected result

After sending via Postman → echo server returns the message → NewMessage outputs:
```json
{
    "message": "Hello from Postman",
    "timestamp": "2026-04-20T12:00:00.000Z"
}
```

## Testing with SendMessage component

Alternatively, add a **SendMessage** component in the same flow:
1. Connect any trigger (e.g., a manual trigger or timer) to SendMessage's input
2. SendMessage derives the connection ID from the flowId automatically
3. It sends the message through the shared connection
4. Echo server returns it → NewMessage receives it

## Plugin Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/connections` | List all open connections on this node |
| POST | `/connections` | Create a new WS connection (used by NewMessage.start) |
| POST | `/push-message` | Send a message through an existing connection |
| DELETE | `/connections/{connectionId}` | Close and remove a connection |

## Troubleshooting

- **Connection not listed in GET /connections**: Flow might not be running, or engine was restarted and sync job hasn't run yet (wait up to 1 minute).
- **"not found in state" error on push-message**: The connectionId doesn't match. Check GET /connections for the correct ID.
- **"failed to reconnect" error**: The WebSocket server might be down or unreachable.
- **Messages not arriving at NewMessage**: Make sure you're using an echo server that sends messages back on the same connection.
