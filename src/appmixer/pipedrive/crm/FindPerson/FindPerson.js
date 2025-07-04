'use strict';

const searchOutput = require('../../searchOutput');

const outputPortName = 'out';

/**
 * FindPerson action.
 * @extends {Component}
 */
module.exports = {

    async receive(context) {

        const { generateOutputPortOptions } = context.properties;
        const { term, exactMatch, orgId, outputType } = context.messages.in.content;

        if (generateOutputPortOptions) {
            return this.getOutputPortOptions(context, outputType);
        }

        const queryParams = {
            term,
            exact_match: exactMatch,
            organization_id: orgId,
            limit: outputType === 'first' ? 1 : 100
        };

        const { data } = await context.httpRequest({
            method: 'GET',
            url: 'https://api.pipedrive.com/v1/persons/search',
            headers: {
                'x-api-token': `${context.auth.apiKey}`
            },
            params: queryParams
        });

        if (data.length === 0) {
            return context.sendJson({}, 'notFound');
        }

        const responseData = data.data.items.map((item) => {
            return {
                ...item.item
            };
        });

        return await searchOutput.sendArrayOutput({ context, outputPortName, outputType, records: responseData });
    },

    getOutputPortOptions(context, outputType) {
        switch (outputType) {
            case 'object':
            case 'first':
                return context.sendJson([
                    { label: 'Current Page Index', value: 'index', schema: { type: 'integer' } },
                    { label: 'Pages Count', value: 'count', schema: { type: 'integer' } },
                    { label: 'Person ID', value: 'id' },
                    { label: 'Type', value: 'type' },
                    { label: 'Name', value: 'name' },
                    { label: 'Phones', value: 'phones', schema: { type: 'array', items: [] } },
                    { label: 'Emails', value: 'emails', schema: { type: 'array', items: [] } },
                    { label: 'Primary Email', value: 'primary_email' },
                    { label: 'Visible To', value: 'visible_to' },
                    { label: 'Owner', value: 'owner', schema: { type: 'object', properties: { id: { type: 'number', title: 'Owner ID' } } } },
                    { label: 'Organization', value: 'organization' },
                    { label: 'Custom Fields', value: 'custom_fields', schema: { type: 'array', items: [] } },
                    { label: 'Notes', value: 'notes', schema: { type: 'array', items: [] } },
                    { label: 'Update Time', value: 'update_time' }
                ], outputPortName);

            case 'array':
                return context.sendJson([
                    { label: 'Pages Count', value: 'count', schema: { type: 'integer' } },
                    {
                        label: 'Persons',
                        value: 'records',
                        schema: {
                            type: 'array',
                            items: {
                                type: 'object',
                                properties: {
                                    id: { label: 'Person ID', value: 'id' },
                                    type: { label: 'Type', value: 'type' },
                                    name: { label: 'Name', value: 'name' },
                                    phones: { label: 'Phones', value: 'phones', schema: { type: 'array', items: [] } },
                                    emails: { label: 'Emails', value: 'emails', schema: { type: 'array', items: [] } },
                                    primary_email: { label: 'Primary Email', value: 'primary_email' },
                                    visible_to: { label: 'Visible To', value: 'visible_to' },
                                    owner: { label: 'Owner', value: 'owner', schema: { type: 'object', properties: { id: { type: 'number', title: 'Owner ID' } } } },
                                    organization: { label: 'Organization', value: 'organization' },
                                    custom_fields: { label: 'Custom Fields', value: 'custom_fields', schema: { type: 'array', items: [] } },
                                    notes: { label: 'Notes', value: 'notes', schema: { type: 'array', items: [] } },
                                    update_time: { label: 'Update Time', value: 'update_time' }
                                }
                            }
                        }
                    }
                ], outputPortName);

            case 'file':
                return context.sendJson([
                    { label: 'File ID', value: 'fileId', schema: { type: 'string', format: 'appmixer-file-id' } },
                    { label: 'Pages Count', value: 'count', schema: { type: 'integer' } }
                ], outputPortName);

            default:
                return context.cancelError('Unsupported outputType ' + outputType);
        }
    }
};
