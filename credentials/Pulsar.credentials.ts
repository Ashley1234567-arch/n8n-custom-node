import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class Pulsar implements ICredentialType {
	name = 'pulsar';

	displayName = 'Pulsar';

	documentationUrl = 'pulsar';

	properties: INodeProperties[] = [
		{
			displayName: 'Service URL',
			name: 'serviceURL',
			type: 'string',
			default: '',
			placeholder: 'pulsar1:6650',
			required: true
		},
		{
			displayName: 'Authentication',
			name: 'authentication',
			type: 'boolean',
			default: false,
		},
		{
			displayName: 'Token',
			name: 'token',
			type: 'string',
			displayOptions: {
				show: {
					authentication: [true],
				},
			},
			default: '',
			description: 'Optional token if authenticated is required',
		}
	];
}
