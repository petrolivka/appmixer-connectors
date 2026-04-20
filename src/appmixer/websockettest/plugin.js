'use strict';

module.exports = async context => {
    context.log('info', '[WS-TEST] Initializing WebSocket Test plugin.');

    require('./routes')(context);

    context.log('info', '[WS-TEST] Scheduling WebSocket sync jobs.');
    await require('./jobs')(context);

    context.log('info', '[WS-TEST] WebSocket Test plugin initialized.');
};
