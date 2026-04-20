'use strict';

const { buildConnectionId } = require('../../connections');

module.exports = {

    async receive(context) {

        if (context.messages.in) {
            const { flowId } = context;
            const connectionId = buildConnectionId(flowId);
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
