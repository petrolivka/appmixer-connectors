'use strict';

const smConnections = require('./sm-connections');

module.exports = (context) => {

    context.http.router.register({
        method: 'POST',
        path: '/sm/open',
        options: {
            handler: async (req) => {

                const appToken = context.config.appToken;
                if (!appToken) {
                    throw new Error('[SLACK-SM] Missing appToken in connector configuration.');
                }
                const connectionId = await smConnections.openConnection(context, appToken);
                return { connectionId };
            }
        }
    });

    context.http.router.register({
        method: 'POST',
        path: '/sm/listeners',
        options: {
            handler: async (req) => {

                const { connectionId, flowId, componentId, eventType, filter } = req.payload;
                await smConnections.addListener(context, connectionId, flowId, componentId, eventType, filter);
                return { ok: true };
            }
        }
    });

    context.http.router.register({
        method: 'DELETE',
        path: '/sm/listeners',
        options: {
            handler: async (req) => {

                const { connectionId, flowId, componentId, eventType } = req.payload;
                await smConnections.removeListener(context, connectionId, flowId, componentId, eventType);
                return { ok: true };
            }
        }
    });

    context.http.router.register({
        method: 'GET',
        path: '/sm/status',
        options: {
            handler: async () => {

                const connections = smConnections.listConnections();
                const listeners = smConnections.listListeners();
                const connectionIds = Object.keys(connections);
                return {
                    connections: connectionIds.map(id => ({
                        id,
                        connected: connections[id]?.connected || false
                    })),
                    listeners: Object.fromEntries(
                        Object.entries(listeners).map(([connId, list]) => [
                            connId,
                            list.map(l => ({
                                flowId: l.flowId,
                                componentId: l.componentId,
                                eventType: l.eventType
                            }))
                        ])
                    )
                };
            }
        }
    });
};
