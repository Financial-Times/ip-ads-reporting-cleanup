const AWS = require('aws-sdk');
import * as csv from 'csvtojson';
const s3 = new AWS.S3();
const bucketName = process.env.BUCKET_NAME;

const getData = async (params) => {
	const s3 = new AWS.S3();
	let s3stream = s3.getObject(params).createReadStream();
	s3stream.on('error', (err) => {
		throw new Error(err);
	});
	return csv().fromStream(s3stream);
};

const saveResult = async (params) => {
	const { key, buffer, filename } = params;

	if (!bucketName) {
		throw new Error('Required bucket name not found');
	}

	return new Promise((resolve, reject) => {
		s3.putObject({
			Bucket: bucketName,
			Key: `${key}/${filename}`,
			Body: buffer,
		}, (err, data) => {
			if (err) {
				return reject(`[S3Client: ] Save result ${err}`);
			} else {
				console.log('Successfully saved results to s3 ✍️:' + key);
				return resolve(data);
			}
		});
	});
};

export { getData, saveResult };
