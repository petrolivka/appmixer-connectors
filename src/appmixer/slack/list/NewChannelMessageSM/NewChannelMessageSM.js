'use strict';

module.exports = {

    async start(context) {

        const componentName = context.flowDescriptor[context.componentId].label || 'New Channel Message (Socket Mode)';

        const appToken = context.config?.appToken;
        if (!appToken) {
            throw new Error(`Missing Slack configuration for component: ${componentName}. Please configure the "appToken" (App-Level Token) in the connector configuration.`);
        }

        await context.log('info', `[SLACK-SM] ${componentName} start() called. Opening SM connection.`);

        // Open Socket Mode connection (or reuse existing).
        let connectionId;
        try {
            const response = await context.callAppmixer({
                endPoint: '/plugins/appmixer/slack/sm/open',
                method: 'POST',
                body: {}
            });
            await context.log('info', `[SLACK-SM] /sm/open response: ${JSON.stringify(response)}.`);
            connectionId = response.connectionId;
        } catch (err) {
            await context.log('error', `[SLACK-SM] ${componentName} failed to open SM connection: ${err.message}.`);
            throw err;
        }

        if (!connectionId) {
            throw new Error(`${componentName}: No connectionId returned from /sm/open.`);
        }

        // Register as listener for 'message' events.
        const filter = {};
        if (context.properties.channelId) {
            filter.channelId = context.properties.channelId;
        }

        try {
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
            await context.log('info', `[SLACK-SM] ${componentName} listener registered for connection ${connectionId}.`);
        } catch (err) {
            await context.log('error', `[SLACK-SM] ${componentName} failed to register listener: ${err.message}.`);
            throw err;
        }

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
