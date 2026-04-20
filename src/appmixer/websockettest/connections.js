'use strict';

const WebSocket = require('ws');

// Same pattern as Kafka - use process object to survive require cache clearing.
let WS_CONNECTOR_OPEN_CONNECTIONS;
if (process.WS_CONNECTOR_OPEN_CONNECTIONS) {
    WS_CONNECTOR_OPEN_CONNECTIONS = process.WS_CONNECTOR_OPEN_CONNECTIONS;
} else {
    process.WS_CONNECTOR_OPEN_CONNECTIONS = WS_CONNECTOR_OPEN_CONNECTIONS = {};
}

/**
 * Build a deterministic connection ID for a flow. This ensures all components
 * in the same flow share the same WebSocket connection.
 */
const buildConnectionId = (flowId) => `ws-${flowId}`;

/**
 * Opens a shared WebSocket connection for a flow. If a connection for this flow
 * already exists and is open, returns the existing connectionId.
 * @param {Object} context Plugin context
 * @param {string} url WebSocket server URL
 * @param {string} flowId Flow ID
 * @param {string} receiverComponentId Component ID that should receive incoming messages (trigger)
 * @param {string} [connId] Optional explicit connection ID
 */
const addConnection = async (context, url, flowId, receiverComponentId, connId) => {

    const connectionId = connId || buildConnectionId(flowId);

    // If connection already exists and is open, just update the state (receiver might change).
    const existingWs = WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
    if (existingWs && existingWs.readyState === WebSocket.OPEN) {
        await context.log('info', `[WS-TEST] Connection ${connectionId} already open. Updating state.`);
        await context.service.stateSet(connectionId, { url, flowId, receiverComponentId });
        return connectionId;
    }

    await context.service.stateSet(connectionId, {
        url, flowId, receiverComponentId
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

        // Deliver incoming messages to the receiver component (NewMessage trigger).
        const connState = await context.service.stateGet(connectionId);
        if (connState && connState.receiverComponentId) {
            await context.triggerComponent(
                flowId,
                connState.receiverComponentId,
                { message, timestamp: new Date().toISOString() },
                { enqueueOnly: 'true' }
            );
        }
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

/**
 * Send a message through an existing connection. Reconnects automatically if needed.
 */
const sendMessage = async (context, connectionId, message) => {

    let ws = WS_CONNECTOR_OPEN_CONNECTIONS[connectionId];
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Reconnect from persisted state (same pattern as Kafka producer).
        const connection = await context.service.stateGet(connectionId);
        if (!connection) {
            throw new Error(`WebSocket connection ${connectionId} not found in state.`);
        }
        await context.log('info', `[WS-TEST] Reconnecting WebSocket ${connectionId} to ${connection.url}.`);
        await addConnection(context, connection.url, connection.flowId, connection.receiverComponentId, connectionId);
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
    buildConnectionId,
    addConnection,
    sendMessage,
    removeConnection,
    listConnections
};
