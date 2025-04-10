import type {
	ITriggerFunctions,
	IDataObject,
	INodeType,
	INodeTypeDescription,
	ITriggerResponse
} from 'n8n-workflow';
import { NodeConnectionType, NodeOperationError } from 'n8n-workflow';
import { AuthenticationToken, Client, ClientConfig, LogLevel } from "pulsar-client";

export class PulsarTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Pulsar Trigger',
		name: 'pulsarTrigger',
		icon: { light: 'file:pulsar.svg', dark: 'file:pulsar.svg' },
		group: ['trigger'],
		version: [1, 1.1],
		description: 'Consume messages from a Pulsar topic',
		defaults: {
			name: 'Pulsar Trigger',
		},
		inputs: [],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'pulsar',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Topic',
				name: 'topic',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'topic-name',
				description: 'Name of the queue of topic to consume from',
			},
			{
				displayName: 'Subscription',
				name: 'subscription',
				type: 'string',
				required: true,
				placeholder: 'subscription',
				default: '',
				description: 'Subscription id of the client',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				default: {},
				placeholder: 'Add option',
				options: [
					{
						displayName: 'Read Messages From Beginning',
						name: 'fromBeginning',
						type: 'boolean',
						default: true,
						description: 'Whether to read message from beginning',
					},
					{
						displayName: 'JSON Parse Message',
						name: 'jsonParseMessage',
						type: 'boolean',
						default: false,
						description: 'Whether to try to parse the message to an object',
					},
					{
						displayName: 'Only Message',
						name: 'onlyMessage',
						type: 'boolean',
						displayOptions: {
							show: {
								jsonParseMessage: [true],
							},
						},
						default: false,
						description: 'Whether to return only the message property',
					},
					{
						displayName: 'Session Timeout',
						name: 'sessionTimeout',
						type: 'number',
						default: 30000,
						description: 'The time to await a response in ms',
						hint: 'Value in milliseconds',
					},
				],
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const topic = this.getNodeParameter('topic') as string;

		const subscription = this.getNodeParameter('subscription') as string;

		const credentials = await this.getCredentials('pulsar');

		const serviceURL = credentials.serviceURL as string;

		const options = this.getNodeParameter('options', {}) as IDataObject;

		options.nodeVersion = this.getNode().typeVersion;

		const config: ClientConfig = {
			serviceUrl: `pulsar://${serviceURL}`,
			operationTimeoutSeconds: options.sessionTimeout as number ?? 30000,
			logLevel: LogLevel.ERROR
		};

		if (credentials.authentication === true) {
			if (!credentials.token) {
				throw new NodeOperationError(
					this.getNode(),
					'Token is required for authentication',
				);
			}
			config.authentication = new AuthenticationToken({
				token: credentials.token as string
			});
		}

		const pulsar = new Client(config);
		const consumer = await pulsar.subscribe({
			topic,
			subscription,
			subscriptionInitialPosition: options.fromBeginning ? "Earliest" : "Latest"
		});

		// The "closeFunction" function gets called by n8n whenever
		// the workflow gets deactivated and can so clean up.
		async function closeFunction() {
			await consumer.close();
		}

		const startConsumer = async () => {
			const message = await consumer.receive();
			let data: IDataObject = {};
			let value = message.getData()?.toString() as string;
			if (options.jsonParseMessage) {
				try {
					value = JSON.parse(value);
				} catch (error) {}
			}
			data.message = value;
			data.topic = topic;
			if (options.onlyMessage) {
				//@ts-ignore
				data = value;
			}
			await consumer.acknowledge(message);
			this.emit([this.helpers.returnJsonArray([data])]);
		};

		if (this.getMode() !== 'manual') {
			await startConsumer();
			return { closeFunction };
		} else {
			// The "manualTriggerFunction" function gets called by n8n
			// when a user is in the workflow editor and starts the
			// workflow manually. So the function has to make sure that
			// the emit() gets called with similar data like when it
			// would trigger by itself so that the user knows what data
			// to expect.
			async function manualTriggerFunction() {
				await startConsumer();
			}

			return {
				closeFunction,
				manualTriggerFunction,
			};
		}
	}
}
