
import { getSoapClient } from '../../lib/adbook-api-client/getSoapClient';
import { getDQTReport } from '../../lib/adbook-api-client/getDQTReport';
import logger from '@financial-times/n-serverless-logger';

const handle = async (event, context, callback) => {
	let end = false;

	try {
		var result = event.hasOwnProperty('extract') ? event.extract : { result : {processedRows: 0, importedRows: 0, errors: [] }};
		console.log(result);

		logger.setContext({
			awsRequestId: context.awsRequestId,
			functionName: context.functionName,
			systemCode: 'ip-ads-reporting-cleanup',
		});

		logger.info({
			event: '[DELETE EXTRACT]',
			message: JSON.stringify({ msg: 'Reporting extract kicks-off.' }) });


		let client = await getSoapClient();

		if (client) {

			let response = await getDQTReport(client, result);
			console.log('result from call', response);
			event['extract'] = response;
			console.log('my event', event);
			callback(null, event);
		} else {
			logger.error({
				event: '[DELETE EXTRACT]',
				message: 'Unable to access the remote client' });
		}

	} catch (err) {
		logger.error({
			event: '[REPORTING EXTRACT]',
			message: JSON.stringify({ msg: `In Extract Handler :${err ? `${err.message}`: ''}` }) });

		console.log('Error stopped progress', err);
		end = true;
		event['extract'] = {
			'result':
            { 'finished': end } };
		callback(null, event);
	}

};
export { handle };
