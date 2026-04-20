'use strict';

const connections = require('./connections');

module.exports = (context) => {

    context.http.router.register({
        method: 'POST',
        path: '/connections',
        options: {
            handler: async (req) => {

                const { flowId, receiverComponentId, url } = req.payload;
                const connectionId = await connections.addConnection(context, url, flowId, receiverComponentId);
                return { connectionId };
            }
        }
    });

    context.http.router.register({
        method: 'POST',
        path: '/send',
        options: {
            handler: async (req) => {

                const { connectionId, message } = req.payload;
                await connections.sendMessage(context, connectionId, message);
                return {};
            }
        }
    });

    context.http.router.register({
        method: 'DELETE',
        path: '/connections/{connectionId}',
        options: {
            handler: async (req) => {

                const { connectionId } = req.params;
                await connections.removeConnection(context, connectionId);
                return {};
            }
        }
    });

    context.http.router.register({
        method: 'GET',
        path: '/connections',
        options: {
            handler: async () => {

                const open = connections.listConnections();
                return { count: Object.keys(open).length, ids: Object.keys(open) };
            }
        }
    });
};
