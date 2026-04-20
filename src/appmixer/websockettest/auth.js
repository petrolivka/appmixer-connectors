'use strict';

const WebSocket = require('ws');

module.exports = {

    type: 'apiKey',

    definition: () => {

        return {

            tokenType: 'authentication-token',

            accountNameFromProfileInfo: 'connectionName',

            auth: {
                connectionName: {
                    type: 'text',
                    name: 'Connection Name',
                    tooltip: 'A descriptive name for this WebSocket connection.'
                },
                url: {
                    type: 'text',
                    name: 'WebSocket URL',
                    tooltip: 'The WebSocket server URL. Example: wss://echo.websocket.org or ws://localhost:8080.'
                }
            },

            requestProfileInfo: async context => {

                return { connectionName: context.connectionName };
            },

            validate: async context => {

                const ws = new WebSocket(context.url);
                return new Promise((resolve, reject) => {
                    const timeout = setTimeout(() => {
                        ws.close();
                        reject(new Error('Connection timeout'));
                    }, 10000);
                    ws.on('open', () => {
                        clearTimeout(timeout);
                        ws.close();
                        resolve(true);
                    });
                    ws.on('error', (err) => {
                        clearTimeout(timeout);
                        reject(new Error(`WebSocket validation failed: ${err.message}`));
                    });
                });
            }
        };
    }
};
