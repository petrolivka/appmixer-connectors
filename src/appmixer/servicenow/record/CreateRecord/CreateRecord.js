/* eslint-disable camelcase */
'use strict';

const lib = require('../../lib');

module.exports = {

    async receive(context) {

        const { sysparm_fields } = context.messages.in.content;
        const { tableName, generateInspector, generateOutputPortOptions } = context.properties;

        if (generateOutputPortOptions) {
            const columns = await lib.getColumns(context, { tableName });
            return lib.toOutputScheme(context, columns, sysparm_fields);
        }

        if (generateInspector) {
            const columns = await lib.getColumns(context, { tableName });
            return lib.toInspector(context, {
                columns,
                fields: sysparm_fields,
                schema: {
                    properties: {
                        sysparm_fields: { type: 'string' }
                    },
                    required: []
                }
            });
        }

        const inputs = context.messages.in.content;

        const { data } = await lib.callEndpoint(context, {
            method: 'POST',
            action: `table/${tableName}`,
            data: inputs,
            params: {
                // explicitly includes the sys_id param
                sysparm_fields: sysparm_fields ? `${sysparm_fields},sys_id` : sysparm_fields
            }
        });

        return context.sendJson(data?.result, 'out');
    }
};
