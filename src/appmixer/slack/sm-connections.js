'use strict';

const { SocketModeClient } = require('@slack/socket-mode');

// Persistent connection pool on process object (same pattern as Kafka/websockettest).
// Key: app-level token hash (one connection per Slack app).
let SM_CONNECTIONS;
if (process.SLACK_SM_CONNECTIONS) {
    SM_CONNECTIONS = process.SLACK_SM_CONNECTIONS;
} else {
    process.SLACK_SM_CONNECTIONS = SM_CONNECTIONS = {};
}

// Listener registry: { connectionId: [{ flowId, componentId, eventType, filter }] }
let SM_LISTENERS;
if (process.SLACK_SM_LISTENERS) {
    SM_LISTENERS = process.SLACK_SM_LISTENERS;
} else {
    process.SLACK_SM_LISTENERS = SM_LISTENERS = {};
}

/**
 * Build a deterministic connection ID from the app-level token.
 * We use a short hash to avoid storing the full token as a key.
 */
const buildConnectionId = (appToken) => {
    const crypto = require('crypto');
    const hash = crypto.createHash('sha256').update(appToken).digest('hex').substring(0, 12);
    return `slack-sm-${hash}`;
};

/**
 * Open a Socket Mode connection. If already open, returns existing connectionId.
 */
const openConnection = async (context, appToken) => {

    const connectionId = buildConnectionId(appToken);

    const existing = SM_CONNECTIONS[connectionId];
    if (existing && existing.connected) {
        await context.log('info', `[SLACK-SM] Connection ${connectionId} already open.`);
        return connectionId;
    }

    await context.log('info', `[SLACK-SM] Opening Socket Mode connection ${connectionId}.`);

    const client = new SocketModeClient({
        appToken,
        autoReconnectEnabled: true,
        clientPingTimeout: 30000
    });

    // Debug: log connection state changes.
    client.on('connected', async () => {
        await context.log('info', `[SLACK-SM] Connection ${connectionId} CONNECTED.`);
    });

    client.on('connecting', async () => {
        await context.log('info', `[SLACK-SM] Connection ${connectionId} CONNECTING...`);
    });

    client.on('authenticated', async (event) => {
        await context.log('info', `[SLACK-SM] Connection ${connectionId} AUTHENTICATED. Info: ${JSON.stringify(event)}.`);
    });

    // Route Events API events to registered listeners.
    // @slack/socket-mode emits by envelope type: 'events_api', 'slash_commands', 'interactive'.
    const eventHandler = async (args) => {

        try {
            const { ack, body } = args;
            const event = args.event || body?.event;

            // Acknowledge immediately (Slack requires ACK within 3 seconds).
            if (ack) await ack();

            await context.log('info', `[SLACK-SM] eventHandler fired. Args keys: ${Object.keys(args).join(', ')}. Event type: ${event?.type}.`);

            const eventType = event?.type;
            if (!eventType) {
                await context.log('info', `[SLACK-SM] Event without type. Body keys: ${Object.keys(body || {}).join(', ')}.`);
                return;
            }

            const teamId = body?.team_id;
            await context.log('info', `[SLACK-SM] Event received: ${eventType}, team: ${teamId}, channel: ${event?.channel}.`);

            // Find matching listeners and trigger their components.
            const listeners = SM_LISTENERS[connectionId] || [];
            await context.log('info', `[SLACK-SM] Listeners for ${connectionId}: ${listeners.length}. Looking for eventType: ${eventType}.`);

            for (const listener of listeners) {
                await context.log('info', `[SLACK-SM] Checking listener: eventType=${listener.eventType}, componentId=${listener.componentId}, filter=${JSON.stringify(listener.filter)}.`);

                if (listener.eventType !== eventType) {
                    await context.log('info', `[SLACK-SM] Skipping: eventType mismatch (listener=${listener.eventType}, event=${eventType}).`);
                    continue;
                }

                // Apply optional filter (e.g., channel filter).
                if (listener.filter) {
                    if (listener.filter.channelId && event.channel && listener.filter.channelId !== event.channel) {
                        await context.log('info', `[SLACK-SM] Skipping: channel mismatch (filter=${listener.filter.channelId}, event=${event.channel}).`);
                        continue;
                    }
                    if (listener.filter.teamId && teamId && listener.filter.teamId !== teamId) {
                        await context.log('info', `[SLACK-SM] Skipping: team mismatch.`);
                        continue;
                    }
                }

                await context.log('info', `[SLACK-SM] Triggering component ${listener.componentId} in flow ${listener.flowId}.`);
                await context.triggerComponent(
                    listener.flowId,
                    listener.componentId,
                    { event, body },
                    { enqueueOnly: 'true' }
                );
                await context.log('info', `[SLACK-SM] Component ${listener.componentId} triggered successfully.`);
            }
        } catch (err) {
            await context.log('error', `[SLACK-SM] eventHandler error: ${err.message}. Stack: ${err.stack}.`);
        }
    };

    // Listen on both possible event names.
    client.on('events_api', eventHandler);
    client.on('slack_event', eventHandler);

    client.on('disconnect', async () => {
        await context.log('info', `[SLACK-SM] Connection ${connectionId} disconnected. Auto-reconnect will handle it.`);
    });

    await client.start();
    SM_CONNECTIONS[connectionId] = client;

    // Persist connection state for cluster sync.
    await context.service.stateSet(connectionId, { appToken });

    await context.log('info', `[SLACK-SM] Connection ${connectionId} established.`);
    return connectionId;
};

/**
 * Register a listener for a specific event type on a connection.
 */
const addListener = async (context, connectionId, flowId, componentId, eventType, filter) => {

    if (!SM_LISTENERS[connectionId]) {
        SM_LISTENERS[connectionId] = [];
    }

    // Prevent duplicate registration.
    const exists = SM_LISTENERS[connectionId].find(
        l => l.flowId === flowId && l.componentId === componentId && l.eventType === eventType
    );
    if (exists) return;

    SM_LISTENERS[connectionId].push({ flowId, componentId, eventType, filter });

    // Persist listeners for cluster sync.
    await context.service.stateSet(`listeners:${connectionId}`, SM_LISTENERS[connectionId]);

    await context.log('info', `[SLACK-SM] Listener added: ${eventType} for ${componentId} in flow ${flowId}.`);
};

/**
 * Remove a listener.
 */
const removeListener = async (context, connectionId, flowId, componentId, eventType) => {

    if (!SM_LISTENERS[connectionId]) return;

    SM_LISTENERS[connectionId] = SM_LISTENERS[connectionId].filter(
        l => !(l.flowId === flowId && l.componentId === componentId && l.eventType === eventType)
    );

    await context.service.stateSet(`listeners:${connectionId}`, SM_LISTENERS[connectionId]);

    await context.log('info', `[SLACK-SM] Listener removed: ${eventType} for ${componentId} in flow ${flowId}.`);

    // If no listeners left, close connection.
    if (SM_LISTENERS[connectionId].length === 0) {
        await closeConnection(context, connectionId);
    }
};

/**
 * Close a Socket Mode connection.
 */
const closeConnection = async (context, connectionId) => {

    await context.log('info', `[SLACK-SM] Closing connection ${connectionId}.`);
    await context.service.stateUnset(connectionId);
    await context.service.stateUnset(`listeners:${connectionId}`);

    const client = SM_CONNECTIONS[connectionId];
    if (client) {
        try {
            await client.disconnect();
        } catch (err) {
            await context.log('error', `[SLACK-SM] Error disconnecting ${connectionId}: ${err.message}.`);
        }
        delete SM_CONNECTIONS[connectionId];
    }

    delete SM_LISTENERS[connectionId];
};

const listConnections = () => SM_CONNECTIONS;
const listListeners = () => SM_LISTENERS;

module.exports = {
    buildConnectionId,
    openConnection,
    addListener,
    removeListener,
    closeConnection,
    listConnections,
    listListeners
};
