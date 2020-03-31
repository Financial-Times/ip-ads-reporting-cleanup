import logger from '@financial-times/n-serverless-logger';
import { getData } from '../../lib/awsUtils';

const jsforce = require('jsforce');
const username = process.env.SANDBOX_CRED;
const password = process.env.SANDBOX_PASS;

const deletion = async (context, event, callback) => {

	try {

		logger.setContext({
			awsRequestId: context.awsRequestId,
			functionName: context.functionName,
			systemCode: 'ip-ads-reporting-cleanup',
		});

		if (event.Records) {

			logger.info({
				event: '[REPORTING DELETE]',
				message: `Received event:,${ JSON.stringify(event, null, 2)}, ${decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '))}` });

			const bucket = event.Records[0].s3.bucket.name;
			const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
			const params = {
				Bucket: bucket,
				Key: key,
			};

			if (params.Bucket && params.Key) {

				const lineItems = await getData(params);

				if (lineItems && lineItems.length > 0) {


					const oauth2 = new jsforce.OAuth2({
						loginUrl: process.env.SANDBOX_URL,
						clientId: process.env.CONSUMER_KEY,
						clientSecret: process.env.CONSUMER_SECRET,
						redirectUri: process.env.SANDBOX_REDIRECT_URL,
					});

					const conn = new jsforce.Connection(oauth2);

					if (!username || !password) {
						throw new Error('Invalid credentials, unable to proceed.');
					}

					let userInfo = await conn.login(username, password);

					if (!userInfo) {
						throw new Error('Unble to establish a connection for the user');
					}

					conn.maxRequest = 300;

					let h;
					for (const p of lineItems) {
						h = await conn.sobject('Advertising_Revenue_Schedule__c').find(
							p
							, 'Id, Name, CreatedDate');
						let del = await conn.sobject('Advertising_Revenue_Schedule__c').destroy(h[0].Id);
						logger.info({
							event: '[REPORTING DELETE]',
							message: `Schedule not found:, ${del}` });
						console.log(del);
					}
				} else {
					logger.info({
						event: '[REPORTING DELETE]',
						message: 'No line items found' });
				}
			}
		}
	} catch (err) {
		logger.error({
			event: '[REPORTING DELETE]', msg: err.message });
	}

	finally {
		callback(null, event);
	}
};
export { deletion };
