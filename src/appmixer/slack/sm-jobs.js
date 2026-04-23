'use strict';

const smConnections = require('./sm-connections');

let isSyncInProgress = false;

module.exports = async (context) => {

    const schedule = context.config.smSyncSchedule || '0 */1 * * * *';

    await context.scheduleJob('syncSmConnectionsJob', schedule, async () => {

        if (isSyncInProgress) {
            await context.log('info', '[SLACK-SM] Sync job already in progress. Skipping.');
            return;
        }

        const appToken = context.config.appToken;
        if (!appToken) {
            // Socket Mode not configured, skip.
            return;
        }

        isSyncInProgress = true;

        try {
            const connectionId = smConnections.buildConnectionId(appToken);
            const openConnections = smConnections.listConnections();
            const localListeners = smConnections.listListeners();

            // Check if connection should exist (has persisted state).
            const connectionState = await context.service.stateGet(connectionId);
            const persistedListeners = await context.service.stateGet(`listeners:${connectionId}`);

            if (!connectionState && !persistedListeners?.length) {
                // No connection desired. Clean up if locally open.
                if (openConnections[connectionId]) {
                    await context.log('info', `[SLACK-SM] Connection ${connectionId} locally open but not desired. Closing.`);
                    await smConnections.closeConnection(context, connectionId);
                }
                return;
            }

            // Connection is desired but not open locally — open it.
            if (!openConnections[connectionId] || !openConnections[connectionId].connected) {
                await context.log('info', `[SLACK-SM] Connection ${connectionId} not locally open. Opening.`);
                await smConnections.openConnection(context, appToken);
            }

            // Restore listeners from persisted state if local listeners are empty.
            if (persistedListeners?.length && (!localListeners[connectionId] || localListeners[connectionId].length === 0)) {
                await context.log('info', `[SLACK-SM] Restoring ${persistedListeners.length} listeners for ${connectionId}.`);
                for (const listener of persistedListeners) {
                    // Verify the flow is still running.
                    const flow = await context.db.coreCollection('flows').findOne({
                        flowId: listener.flowId,
                        stage: 'running'
                    });
                    if (flow) {
                        await smConnections.addListener(
                            context, connectionId,
                            listener.flowId, listener.componentId,
                            listener.eventType, listener.filter
                        );
                    } else {
                        await context.log('info', `[SLACK-SM] Flow ${listener.flowId} no longer running. Skipping listener.`);
                    }
                }
            }

            // Clean up listeners for flows that are no longer running.
            const currentListeners = localListeners[connectionId] || [];
            for (const listener of currentListeners) {
                const flow = await context.db.coreCollection('flows').findOne({
                    flowId: listener.flowId,
                    stage: 'running'
                });
                if (!flow) {
                    await context.log('info', `[SLACK-SM] Flow ${listener.flowId} stopped. Removing listener.`);
                    await smConnections.removeListener(
                        context, connectionId,
                        listener.flowId, listener.componentId,
                        listener.eventType
                    );
                }
            }

        } catch (error) {
            await context.log('error', `[SLACK-SM] Sync job error: ${error.message}.`);
        } finally {
            isSyncInProgress = false;
        }
    });
};
