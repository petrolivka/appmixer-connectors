'use strict';

module.exports = {

    async start(context) {

        const componentName = context.flowDescriptor[context.componentId].label || 'New Channel Message (Socket Mode)';

        const appToken = context.config?.appToken;
        if (!appToken) {
            throw new Error(`Missing Slack configuration for component: ${componentName}. Please configure the "appToken" (App-Level Token) in the connector configuration.`);
        }

        // Open Socket Mode connection (or reuse existing).
        const response = await context.callAppmixer({
            endPoint: '/plugins/appmixer/slack/sm/open',
            method: 'POST',
            body: {}
        });

        const connectionId = response.connectionId;
        if (!connectionId) {
            throw new Error(`${componentName}: No connectionId returned from /sm/open. Response: ${JSON.stringify(response)}`);
        }

        // Register as listener for 'message' events.
        const filter = {};
        if (context.properties.channelId) {
            filter.channelId = context.properties.channelId;
        }

        await context.callAppmixer({
            endPoint: '/plugins/appmixer/slack/sm/listeners',
            method: 'POST',
            body: {
                connectionId,
                flowId: context.flowId,
                componentId: context.componentId,
                eventType: 'message',
                filter
            }
        });

        await context.stateSet('connectionId', connectionId);
    },

    async stop(context) {

        const connectionId = await context.stateGet('connectionId');
        if (connectionId) {
            await context.callAppmixer({
                endPoint: '/plugins/appmixer/slack/sm/listeners',
                method: 'DELETE',
                body: {
                    connectionId,
                    flowId: context.flowId,
                    componentId: context.componentId,
                    eventType: 'message'
                }
            });
        }
    },

    async receive(context) {

        if (context.messages.webhook) {
            const { event } = context.messages.webhook.content.data;

            if (context.properties.ignoreBotMessages && event.subtype === 'bot_message') {
                return;
            }

            await context.sendJson(event, 'message');
        }
    }
};
