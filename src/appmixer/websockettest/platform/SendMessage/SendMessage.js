'use strict';

module.exports = {

    async start(context) {

        const { componentId, flowId } = context;
        const url = context.auth.url;

        const { connectionId } = await context.callAppmixer({
            endPoint: '/plugins/appmixer/websockettest/connections',
            method: 'POST',
            body: {
                url,
                componentId,
                flowId
            }
        });
        return context.stateSet('connectionId', connectionId);
    },

    async stop(context) {

        const connectionId = await context.stateGet('connectionId');
        if (connectionId) {
            return context.callAppmixer({
                endPoint: `/plugins/appmixer/websockettest/connections/${connectionId}`,
                method: 'DELETE'
            });
        }
    },

    async receive(context) {

        if (context.messages.in) {
            const connectionId = await context.stateGet('connectionId');
            const message = context.messages.in.content.message;

            await context.callAppmixer({
                endPoint: `/plugins/appmixer/websockettest/connections/${connectionId}/send`,
                method: 'POST',
                body: { message }
            });

            return context.sendJson({ message, sent: true }, 'out');
        }
    }
};
