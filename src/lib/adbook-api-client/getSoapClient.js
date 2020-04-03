const soap = require('soap');
import logger from '@financial-times/n-serverless-logger';

const getSoapClient = () => {
	let url = process.env.ADBOOK_API_URL;

	if (!url) {
		throw new Error('[SOAPCLIENT]', 'Required parameter api endpoint was not specified.');
	}

	let options = {
		passwordType: 'PasswordText'
	};
	let wsSecurity = new soap.WSSecurity(`${process.env.ADBOOK_USERNAME}`, `${process.env.ADBOOK_PASSWORD}`, options);

	let wsdlOptions = {
		envelopeKey: 'soapenv',
		timeout: 5000
	};

	return soap.createClientAsync(url, wsdlOptions).then((client) => {
		client.setSecurity(wsSecurity);
		logger.info({
			event: '[SOAPCLIENT]',
			message: JSON.stringify({ msg: 'Obtained Adbook Soap Client'})});
		return client;
	   }).catch ( err => {
		logger.error({
			event: '[SOAPCLIENT]',
			message: JSON.stringify({ msg: `In getSoapClient :${err.message}`})});
	   });
};

const getApiClient = () => {
	
	let url = process.env.ADBOOK_API_URL;

	if (!url) {
		throw new Error('[SOAPCLIENT]', 'Required parameter api endpoint was not specified.');
	}

	let options = {
		passwordType: 'PasswordText'
	};
	let wsSecurity = new soap.WSSecurity(`${process.env.ADBOOK_USERNAME}`, `${process.env.ADBOOK_PASSWORD}`, options);
	let version = {
		
			'api:Version': 26,

	}
	let wsdlOptions = {
		envelopeKey: 'soapenv',
		timeout: 5000
	};
	

	return soap.createClientAsync(url, wsdlOptions).then((client) => {
		client.addSoapHeader(version)
		client.setSecurity(wsSecurity);
		
		logger.info({
			event: '[SOAPCLIENT]',
			message: JSON.stringify({ msg: 'Obtained Adbook Soap Client'})});
		return client;
	   }).catch ( err => {
		logger.error({
			event: '[SOAPCLIENT]',
			message: JSON.stringify({ msg: `In getSoapClient :${err.message}`})});
	   });
};
export { getApiClient, getSoapClient };
