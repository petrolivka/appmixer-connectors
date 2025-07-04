const lib = require('../lib.generated');
const { personSchema } = require('./../schemas');

module.exports = {
    async receive(context) {
        const { query, outputType } = context.messages.in.content;

        if (context.properties.generateOutputPortOptions) {
            return lib.getOutputPortOptions(context, outputType, personSchema, { label: 'results', value: 'result' });
        }

        const { data } = await context.httpRequest({
            method: 'GET',
            url: 'https://people.googleapis.com/v1/people:listDirectoryPeople',
            headers: {
                'Authorization': `Bearer ${context.auth.accessToken}`
            },
            params: {
                query,
                readMask: 'addresses,ageRanges,biographies,birthdays,calendarUrls,clientData,coverPhotos,emailAddresses,events,externalIds,genders,imClients,interests,locales,locations,memberships,metadata,miscKeywords,names,nicknames,occupations,organizations,phoneNumbers,photos,relations,sipAddresses,skills,urls,userDefined',
                sources: 'DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE'
            }
        });

        if (!Array.isArray(data.people) || !data.people.length) {
            return context.sendJson({}, 'notFound');
        }

        const records = data.people.map((contact) => {
            return {
                id: contact.resourceName.split('/')[1],
                etag: contact.etag,
                updateTime: contact.metadata.sources[0].updateTime,
                displayName: contact.names[0].displayName,
                givenName: contact.names[0].givenName,
                displayNameLastFirst: contact.names[0].displayNameLastFirst,
                unstructuredName: contact.names[0].unstructuredName,
                photoUrl: contact.photos[0].url,
                memberships: contact.memberships
            };
        });

        return lib.sendArrayOutput({ context, records, outputType });
    }
};
