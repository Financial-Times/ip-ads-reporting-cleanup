import logger from '@financial-times/n-serverless-logger';
import { getData, saveResult } from '../../lib/awsUtils';
const { Parser } = require('json2csv');

const moment = require('moment');

var jsforce = require('jsforce');
const username = process.env.SANDBOX_CRED;
const password = process.env.SANDBOX_PASS;

const ADS_DELETE_FOLDER = process.env.ADS_DELETE_FOLDER;
let startDate = '';
let conn;

const saveFile = async (file) => {

	try {
		return await saveResult(file);
	} catch (err) {
		throw new Error('Did not save');
	}
};

const search = async (event, context, callback) => {

	let lineItems;

	try {
		logger.setContext({
			awsRequestId: context.awsRequestId,
			functionName: context.functionName,
			systemCode: 'ip-ads-reporting-cleanup',
		});

		logger.info({
			event: '[REPORTING TRANSFORM]',
			message: JSON.stringify({ msg: 'Reporting delete kicks-off.' }) });

		startDate = moment().subtract(2, 'months').startOf('month').format('MM/DD/YYYY');

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


				const result = [];
				let adrevenue;
				let newData;
				let dataStore = [];
				let schedules = {};
				let children = [];
				let toDelete = [];
				let bodyBuffer;

				lineItems = await getData(params);

				const oauth2 = new jsforce.OAuth2({
					loginUrl: process.env.SANDBOX_URL,

					clientId: process.env.CONSUMER_KEY,
					clientSecret: process.env.CONSUMER_SECRET,
					redirectUri: process.env.SANDBOX_REDIRECT_URL,

				});
				conn = new jsforce.Connection(oauth2);


				if (!username || !password) {
					throw new Error('Invalid credentials, unable to proceed.');
				}

				let userInfo = await conn.login(username, password);

				if (!userInfo) {
					throw new Error('Unble to establish a connection for the user');
				}

				conn.maxRequest = 300;
				//filter out drop id's
				const map = new Map();
				for (const item of lineItems) {
					if (!map.has(item['Drop ID']))	{
						map.set(item['Drop ID'], true); //gather unique ids

						if (moment(item['Start Date'], 'DD/MM/YYYY') >= moment(startDate, 'DD/MM/YYYY')) {
							//taking care of campaigns that have already started
							result.push({
								id: item['Drop ID'],
							});
						}
					}
				}

				console.log('Processing...', result.length);
				for (const dropid of result) {

					adrevenue = lineItems.filter(i => i['Drop ID'] === dropid.id );

					newData = {};
					schedules = {};
					children = [];
					let item = adrevenue[0];

					newData['Ad_Number__c'] = `AB${item['Drop ID'].trim()}`;

					for (const item of adrevenue) {
						const thisMonth = moment().month(item['Month']).format('MMM');

						schedules['Unique_Reference__c'] = `AB${item['Drop ID'].trim()}-${thisMonth}-${item['Year']}`;

						schedules['Advertising_Revenue_Record__c']	= newData['Ad_Number__c'];

						children.push(schedules);
						schedules = {};
					}
					dataStore.push(newData);

					let parent_result = dataStore.map(a => a.Ad_Number__c);

					let myMapArray;

					//pool parent response
					for (const p of parent_result) {
						let sho = await conn.sobject('Advertising_Revenue__c')
							.find(
								{ 'Ad_Number__c': p }, //find parent to get id
							);
						if (sho.length > 0) {
							let offspring = await conn.sobject('Advertising_Revenue_Schedule__c')
								.find(
									{ 'Advertising_Revenue_Record__c': sho[0].Id },
								);

							myMapArray = children.map(c=> c.Unique_Reference__c);

							for (const schedule of offspring) {

								if (!myMapArray.includes(schedule.Unique_Reference__c)) {
									logger.info({
										event: '[REPORTING TRANSFORM]',
										message: JSON.stringify({ msg: `A search did not find this schedule, ${schedule.Unique_Reference__c}` }) });
									console.log(`I did not find that child, ${schedule.Unique_Reference__c}`);
									toDelete.push({ 'Unique_Reference__c': { $eq: schedule.Unique_Reference__c },
									});
								}
							}
						}
					}

					dataStore = [];

				}
				if (toDelete && toDelete.length > 0) {
					const parser = new Parser();
					let csv = parser.parse(toDelete);

					if (csv) {
						bodyBuffer = Buffer.from(csv, 'utf8');
					}
					await saveFile({ filename: `${new Date().toISOString().substring(0, 10)}/middleware_${Math.floor(Math.random() * 9000) + 1000}_${new Date().getTime()}.csv`,
						key: ADS_DELETE_FOLDER,
						buffer: bodyBuffer });
				}

				event['search'] = { end: 'done' };
				console.log('to delete is :', toDelete, toDelete.length);

			}
		}

	} catch (err) {
		logger.error({ event: '[REPORTING SEARCH]', message: JSON.stringify({ msg: err }) });
	}

	finally {
		if (conn) {
			conn.logout(function (err) {
				if (err) { event['error'] = { message: err }; }
				console.log( 'Session is closed.');
			});
		}
		callback(null, event);
	}

};

export { search };
