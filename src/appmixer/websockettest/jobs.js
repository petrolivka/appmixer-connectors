'use strict';

const connections = require('./connections');

let isConnectionSyncInProgress = false;

module.exports = async (context) => {

    const config = require('./config')(context);

    await context.scheduleJob('syncWsConnectionsJob', config.syncConnectionsJob.schedule, async () => {

        if (isConnectionSyncInProgress) {
            await context.log('info', '[WS-TEST] connections sync job is already in progress. Skipping...');
            return;
        }

        isConnectionSyncInProgress = true;

        try {
            // All registered connections throughout the cluster.
            const registeredConnections = await context.service.loadState();
            // Live connections on this specific node.
            const openConnections = connections.listConnections();

            await context.log('info', [
                '[WS-TEST] Syncing WebSocket connections.',
                '# of open Connections: ' + Object.keys(openConnections).length,
                '# of registered Connections: ' + registeredConnections.length
            ].join('; '));

            for (const conn of registeredConnections) {
                const connectionId = conn.key;
                const connectionParameters = conn.value;

                // Check that the flow still exists and is running.
                const flow = await context.db.coreCollection('flows').findOne({
                    flowId: connectionParameters.flowId,
                    stage: 'running'
                });
                if (!flow) {
                    await context.log('info', `[WS-TEST] Flow ${connectionParameters.flowId} does not exist or is not running. Removing connection ${connectionId}.`);
                    await connections.removeConnection(context, connectionId);
                    continue;
                }

                if (!openConnections[connectionId]) {
                    // Connection not open on this node but desired in cluster - recreate it.
                    const stillNeeded = await context.service.stateGet(connectionId);
                    if (stillNeeded) {
                        await context.log('info', `[WS-TEST] Connection not locally open but desired in cluster. Creating connection ${connectionId}.`);
                        await connections.addConnection(
                            context,
                            connectionParameters.url,
                            connectionParameters.flowId,
                            connectionParameters.receiverComponentId,
                            connectionId
                        );
                    }
                }
            }

            // Remove connections that are live locally but no longer desired in cluster.
            for (const connectionId of Object.keys(openConnections)) {
                const conn = await context.service.stateGet(connectionId);
                if (!conn) {
                    await context.log('info', `[WS-TEST] Connection locally open but not desired in cluster. Removing connection ${connectionId}.`);
                    await connections.removeConnection(context, connectionId);
                }
            }

        } catch (error) {
            await context.log('error', `[WS-TEST] Error during connection sync job: ${error.message}.`);
        } finally {
            isConnectionSyncInProgress = false;
        }
    });
};
