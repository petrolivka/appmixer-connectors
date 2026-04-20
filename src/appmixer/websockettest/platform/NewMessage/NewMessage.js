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

        if (context.messages.webhook) {
            return context.sendJson(context.messages.webhook.content.data, 'out');
        }
    }
};
