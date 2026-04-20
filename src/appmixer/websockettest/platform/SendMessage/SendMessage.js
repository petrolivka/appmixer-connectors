'use strict';

module.exports = {

    async receive(context) {

        if (context.messages.in) {
            let connectionId = await context.stateGet('connectionId');

            if (!connectionId) {
                const { componentId, flowId } = context;
                const url = context.auth.url;
                const response = await context.callAppmixer({
                    endPoint: '/plugins/appmixer/websockettest/connections',
                    method: 'POST',
                    body: {
                        url,
                        componentId,
                        flowId
                    }
                });
                connectionId = response.connectionId;
                if (connectionId) {
                    await context.stateSet('connectionId', connectionId);
                } else {
                    throw new Error('Failed to create WebSocket connection. Response: ' + JSON.stringify(response));
                }
            }

            const message = context.messages.in.content.message;

            await context.callAppmixer({
                endPoint: `/plugins/appmixer/websockettest/connections/${connectionId}/send`,
                method: 'POST',
                body: { message }
            });

            return context.sendJson({ message, sent: true }, 'out');
        }
    },

    async stop(context) {

        const connectionId = await context.stateGet('connectionId');
        if (connectionId) {
            return context.callAppmixer({
                endPoint: `/plugins/appmixer/websockettest/connections/${connectionId}`,
                method: 'DELETE'
            });
        }
    }
};
