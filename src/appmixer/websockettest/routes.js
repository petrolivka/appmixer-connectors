'use strict';

const connections = require('./connections');

module.exports = (context) => {

    context.http.router.register({
        method: 'POST',
        path: '/connections',
        options: {
            handler: async (req) => {

                const { flowId, componentId, url } = req.payload;
                const connectionId = await connections.addConnection(context, url, flowId, componentId);
                return { connectionId };
            }
        }
    });

    context.http.router.register({
        method: 'POST',
        path: '/connections/{connectionId}/send',
        options: {
            handler: async (req) => {

                const { connectionId } = req.params;
                const { message } = req.payload;
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
};
