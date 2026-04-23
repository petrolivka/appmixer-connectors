'use strict';

module.exports = async context => {

    require('./routes')(context);

    require('./routes-tasks.js')(context);

    await require('./jobs')(context);
    context.log('info', '[SLACK] Scheduling Slack jobs.');

    // Socket Mode support. Routes are always registered; appToken is checked at runtime.
    context.log('info', '[SLACK] Initializing Socket Mode routes.');
    require('./sm-routes')(context);
    await require('./sm-jobs')(context);
    context.log('info', '[SLACK] Socket Mode routes initialized.');
};
