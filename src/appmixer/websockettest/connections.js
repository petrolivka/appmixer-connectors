'use strict';

const WebSocket = require('ws');

// Same pattern as Kafka - use process object to survive require cache clearing.
let WS_CONNECTOR_OPEN_CONNECTIONS;
if (process.WS_CONNECTOR_OPEN_CONNECTIONS) {
    WS_CONNECTOR_OPEN_CONNECTIONS = process.WS_CONNECTOR_OPEN_CONNECTIONS;
} else {
    process.WS_CONNECTOR_OPEN_CONNECTIONS = WS_CONNECTOR_OPEN_CONNECTIONS = {};
}

const addConnection = async (context, url, flowId, componentId, connId) => {

    const connectionId = connId || `ws:${flowId}:${componentId}:${Math.random().toString(36).substring(7)}`;

    await context.service.stateSet(connectionId, {
        url, flowId, componentId
    });

    const ws = new WebSocket(url);

    await new Promise((resolve, reject) => {
        ws.on('open', () => {
            context.log('info', `[WS-TEST] WebSocket connection opened (${connectionId}) to ${url}.`);
            resolve();
        });
        ws.on('error', (err) => {
            reject(new Error(`WebSocket connection failed: ${err.message}`));
        });
    });

    WS_CONNECTOR_OPEN_CONNECTIONS[connectionId] = ws;

    ws.on('message', async (data) => {
        let message;
        try {
            message = data.toString();
        } catch (e) {
            message = data;
        }

        await context.triggerComponent(
            flowId,
            componentId,
            { message, timestamp: new Date().toISOString() },
            { enqueueOnly: 'true' }
        );
    });

    ws.on('close', async (code, reason) => {
        await context.log('info', `[WS-TEST] WebSocket closed (${connectionId}). Code: ${code}, Reason: ${reason}.`);
        delete WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
    });

    ws.on('error', async (err) => {
        await context.log('error', `[WS-TEST] WebSocket error (${connectionId}): ${err.message}.`);
        delete WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
    });

    return connectionId;
};

const sendMessage = async (context, connectionId, message) => {

    let ws = WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Reconnect from persisted state (same pattern as Kafka producer).
        const connection = await context.service.stateGet(connectionId);
        if (!connection) {
            throw new Error(`WebSocket connection ${connectionId} not found in state.`);
        }
        await context.log('info', `[WS-TEST] Reconnecting WebSocket ${connectionId} to ${connection.url}.`);
        await addConnection(context, connection.url, connection.flowId, connection.componentId, connectionId);
        ws = WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            throw new Error(`WebSocket connection ${connectionId} failed to reconnect.`);
        }
    }
    ws.send(typeof message === 'string' ? message : JSON.stringify(message));
};

const removeConnection = async (context, connectionId) => {

    await context.log('info', `[WS-TEST] Removing connection ${connectionId}.`);
    await context.service.stateUnset(connectionId);
    const ws = WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
    if (!ws) return;

    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        ws.close();
    }
    delete WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
};

const listConnections = () => { return WS_CONNECTOR_OPEN_CONNECTIONS; };

module.exports = {
    addConnection,
    sendMessage,
    removeConnection,
    listConnections
};
