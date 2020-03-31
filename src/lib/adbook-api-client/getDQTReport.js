const csvparser = require('csv-parser');
const DOMImplementation = require('xmldom').DOMImplementation;
const DOMParser = require('xmldom').DOMParser;
const { Parser } = require('json2csv');
import logger from '@financial-times/n-serverless-logger';
const moment = require('moment');
const request = require('request-promise');
import { saveResult } from '../s3-client';
const util = require('util');
const XMLSerializer = require('xmldom').XMLSerializer;

const currentEnv = process.env.NODE_ENV;
let startDate;
let endDate;
const batchSize = 200;


//Reads the file using the URL and calls the s3 client to save
const downloadReport = (jobId, download_url, local_filename, client, params, reject, resolve) => {
	console.log('download params', params);

	const ADS_CLEANUP_FOLDER = process.env.ADS_CLEANUP_FOLDER;
	const ADS_SEARCH_FOLDER = process.env.ADS_SEARCH_FOLDER;

	let idArray = [];
	let returnValue;
	let end = false;

	if (!ADS_CLEANUP_FOLDER || !ADS_SEARCH_FOLDER) {
		return reject(new Error({ event: '[DOWNLOAD REPORT]:', message: 'Required parameters bucket or storage folder not found' }));
	}

	logger.info({
		event: '[DELETE EXTRACT]',
		message: JSON.stringify({ msg: 'Downloading Report', jobId: jobId }) });

	request({ uri: download_url }, function (error, response, body) {

		logger.info({
			event: '[DELETE EXTRACT]',
			message: JSON.stringify({ msg: 'Saving the AdBook Report Extract in s3', jobId: jobId }) });


		const saveParamsExtract = { filename: local_filename, key: ADS_CLEANUP_FOLDER, buffer: body };
		saveResult(saveParamsExtract)
			.catch((err)=>{
				logger.error({
					event: '[DELETE EXTRACT]',
					message: JSON.stringify({ msg: `In downloadReport adbookExtract s3 Save Err :${err}`, jobId: jobId }) });
				return reject(new Error('Error in Saving results in s3: ' + err.message));
			});

		logger.info({
			event: '[EXTRACT]',
			message: JSON.stringify({ msg: 'Filtering the contents of AdBook Report Extract', jobId: jobId }) });
		request.get(download_url)
			.pipe(csvparser())
			.on('data', (data) => {

				const map = new Map();
				if (!map.has(data['Drop ID'])) { //probably don't need map
					map.set(data['Drop ID'], true);
					if (moment(data['Start Date'], 'DD/MM/YYYY') >= moment(startDate, 'DD/MM/YYYY')) {
						idArray.push({ id: data['Drop ID'] } );
					}
				}

				let i = params.result.processedRows; //pass from handle extract
				let filteredRows = [];
				let bodyBuffer;
				let count = i + batchSize;

				if (params.result.importedRows !== 0 && count > params.result.importedRows) {
					count = params.result.importedRows;
				}

				for (i ; i < count; i++) {
					filteredRows.push(idArray[i]);
				}

				const parser = new Parser();
				let csv = parser.parse(filteredRows);

				if (filteredRows) {
					bodyBuffer = Buffer.from(csv, 'utf8');
				}

				const saveParamsFiltered = { filename: `${new Date().toISOString().substring(0, 10)}/middleware_${i}.csv`,
					key: ADS_SEARCH_FOLDER,
					buffer: bodyBuffer };

				saveResult(saveParamsFiltered).then( () => console.log('done')).
					catch((err) => {
						throw new Error ('Eror in save:::', err);
					});

				if (i === params.result.importedRows) {
					console.log('Ending', i);
					end = true;
				}

				returnValue = { 'results':
				{ 'finished': end,
					'processedRows': i,
					'importedRows': idArray.length,
				},
				};
			})
			.on('end', async () => {
				logger.info({
					event: '[EXTRACT]',
					message: JSON.stringify({ msg: 'Extract complete' }) });
			})
			.on('error', (e) => {
				logger.error({
					event: '[DELETE EXTRACT]',
					message: JSON.stringify({ msg: `In clean-up downloadReport CsvParser Err :${e.message}`, jobId: jobId }) });
				return reject(new Error(`Error reading the file from the url ${download_url} with err ${e.message}`));
			});
	})
		.catch((err)=> {
			logger.error({
				event: '[EXTRACT]',
				message: JSON.stringify({ msg: `In downloadReport Download Report Err :${err.message}`, jobId: jobId }) });
			return reject(new Error(`Error in Downloading the file from the url ${download_url} with err ${err.message}`));
		});
	return resolve(returnValue);
};

const getDQTReport = (client, params) => {
	return new Promise((resolve, reject) => {

		logger.info({
			event: '[EXTRACT]',
			message: JSON.stringify({ msg: 'GetSavedReportList' }) });

		client.GetSavedReportList(function (err, result, rawResponse, soapHeader, rawRequest){
			if (err) {
				logger.error({
					event: '[EXTRACT]',
					message: JSON.stringify({ msg: `In getDQTReport for  GetSavedReportQuery :${err.message}` }) });
				return reject(new Error('Error in GetSavedReportQuery: ' + err.message));
			}

			let savedReportId;

			result.GetSavedReportListResult.SavedReport.map((report) => {
				if (report.Name === process.env.REPORTNAME) {
					savedReportId = report.SavedReportID;
				}
			});

			logger.debug({
				event: '[EXTRACT]',
				message: JSON.stringify({ savedReportId: savedReportId }) });

			const xmldocNew = new DOMImplementation().createDocument('http://www.FatTail.com/api', 'api:GetSavedReportQuery', null);
			let savedReport = xmldocNew.createElement('api:savedReportId');
			savedReport.appendChild(xmldocNew.createTextNode(savedReportId));
			xmldocNew.documentElement.appendChild(savedReport);
			let xmlString = new XMLSerializer().serializeToString(xmldocNew);

			logger.info({
				event: '[EXTRACT]',
				message: JSON.stringify({ msg: 'GetSavedReportQuery #' + savedReportId }) });

			//gets the report query obj using the reportId
			client.GetSavedReportQuery({ _xml: xmlString }, function (err, result, rawResponse, soapHeader, rawRequest){
				if (err) {
					logger.error({
						event: '[EXTRACT]',
						message: JSON.stringify({ msg: `In getDQTReport for  GetSavedReportQuery :${err.message}` }) });
					return reject(new Error('Error in GetSavedReportQuery: ' + err.message));
				}
				//Parse the raw response and select the ReportQuery elements and all its children
				const doc = new DOMParser().parseFromString(rawResponse);
				const reportQueryNode = doc.getElementsByTagName('ReportQuery')[0];
				reportQueryNode.setAttribute('xmlns:i', 'http://www.w3.org/2001/XMLSchema-instance');

				//Create a new document with the basic soap envelope for runReport job
				const xmldocNew = new DOMImplementation().createDocument('http://www.FatTail.com/api', 'api:RunReportJob', null);
				let node = xmldocNew.createElement('api:reportJob');
				xmldocNew.documentElement.appendChild(node);

				//Add the ReportQuery and children from rawResponse to the above newly created doc
				let parent = xmldocNew.getElementsByTagName('api:reportJob')[0];
				parent.appendChild(reportQueryNode);

				//Added the date range before running the DQT
				startDate = moment().subtract(2, 'months').startOf('month').format('MM/DD/YYYY');
				endDate = moment().add(14, 'months').endOf('month').format('MM/DD/YYYY');

				console.log('Start-End Date: ' + startDate + '-' + endDate);

				let startDateNode = xmldocNew.getElementsByTagName('ParameterValue')[0];
				let startDateNodeValue = startDateNode.childNodes[0];
				startDateNodeValue.data = startDate;

				let endDateNode = xmldocNew.getElementsByTagName('ParameterValue')[1];
				let endDateNodeValue = endDateNode.childNodes[0];
				endDateNodeValue.data = endDate;

				//deserialize
				let xmlString = new XMLSerializer().serializeToString(xmldocNew);

				//Add the soap envelope
				client.wsdl.xmlnsInEnvelope = 'xmlns:api="http://www.FatTail.com/api" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays" xmlns:sys="http://schemas.datacontract.org/2004/07/System.Collections.Generic"';

				logger.info({
					event: '[EXTRACT]',
					message: JSON.stringify({ msg: 'RunReportJob' }) });

				//Call to RunReport job
				client.RunReportJob({ _xml: xmlString }, function (err, result, rawResponse, soapHeader, rawRequest){
					if (err) {
						logger.error({
							event: '[EXTRACT]',
							message: JSON.stringify({ msg: `In getDQTReport for  RunReportJob :${err.message}` }) });
						return reject(new Error('Error in RunReportJob: ' + err.message));
					}

					let jobId = result.RunReportJobResult.ReportJobID;
					let status = result.RunReportJobResult.Status;

					logger.info({
						event: '[EXTRACT]',
						message: JSON.stringify({ msg: 'Check Report If Complete', jobId: jobId }) });

					//Until Done, Keep checking the report status
					try {
						if (status !== 'Done'){
							setTimeout(function () {
								checkReportCompleted(jobId, params, reject, resolve);
							}, 100);
						}
					} catch (err){
						logger.error({
							event: '[EXTRACT]',
							message: JSON.stringify({ msg: `In getDQTReport for CheckReportCompleted :${err.message}`, jobId: jobId }) });
						return reject(new Error(`Error checking the report completion:  ${err.message}, ${rawRequest}`));
					}
				});
			});
		});
	});

	//Checks the status of the reportJobId until Done or Failed
	function checkReportCompleted (jobId, params, reject, resolve) {

		const timestamp = new Date().getTime();
		let reportJobId = jobId;
		let result;
		console.log('Trying to get report #' + reportJobId);

		logger.info({
			event: '[DELETE EXTRACT]',
			message: JSON.stringify({ msg: `GetReportJob :${reportJobId}`, jobId: reportJobId }) });

		client.GetReportJob({ 'api:reportJobId': reportJobId }, function (err, result, rawResponse, soapHeader, rawRequest){
			if (err) {
				if (currentEnv && currentEnv.toLowerCase() === 'development') {
					logger.info({ event: '[DELETE EXTRACT]', message: JSON.stringify({ msg: `Error in GetReportJob:  ${err.message} ${rawRequest}` }) });
				}

				logger.error({
					event: '[DELETE EXTRACT]',
					message: JSON.stringify({ msg: `In checkReportCompleted for GetReportJob :${err.message}`, jobId: reportJobId }) });
				return reject(new Error('Error in GetReportJob: ' + err.message));
			}

			let jobId = result.GetReportJobResult.ReportJobID;
			let status = result.GetReportJobResult.Status;

			logger.debug({
				event: '[DELETE EXTRACT]',
				message: JSON.stringify({ msg: `In checkReportCompleted Status of running report. # ${status}`, jobId: reportJobId }) });

			if (status === 'Done') {
				logger.info({
					event: '[DELETE EXTRACT]',
					message: JSON.stringify({ msg: 'GetReportDownloadURL', jobId: reportJobId }) });

				client.GetReportDownloadURL({ 'api:reportJobId': jobId }, function (err, result, rawResponse, soapHeader, rawRequest){
					if (err) {
						logger.error({
							event: '[DELETE EXTRACT]',
							message: JSON.stringify({ msg: `In checkReportCompleted for GetReportDownloadURL :${err.message}`, jobId: reportJobId }) });
						return reject(new Error('Error in GetReportDownloadURL: ' + err.message));
					}
					console.log('Downloading report from ' + JSON.stringify(result));

					if (currentEnv && currentEnv.toLowerCase() === 'development') {
						logger.info({
							event: '[DELETE EXTRACT]',
							message: JSON.stringify({ msg: 'Raw request:', rawRequest }) });
					}
					try {
						result = downloadReport(reportJobId, result.GetReportDownloadURLResult, `${new Date().toISOString().substring(0, 10)}/downloaded_report_${jobId}_${timestamp}.csv`, client, params, reject, resolve);
					} catch (err) {
						logger.error({ event: '[GET_DQT_REPORT]:', message: err });
					}
				});
			}

			if (status === 'Error') {
				let error = new Error('Report Failed');
				logger.error({
					event: '[DELETE EXTRACT]',
					message: JSON.stringify({ msg: `In checkReportCompleted Report Failed :${error.message}`, jobId: reportJobId }) });
				throw error;
			}

			if (status === 'Running' || jobId === 'Pending') {
				setTimeout(function () {
					checkReportCompleted(jobId, params, reject, resolve);
				}, 100);
			}
		});
		return resolve(result);
	}
};
export { getDQTReport };
