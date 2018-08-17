const https = require('https');
const path = require('path');
const url = require('url');
const fs = require('fs');
const { spawn } = require('child_process');
const AWS = require('aws-sdk/global');
const DynamoDB = require('aws-sdk/clients/dynamodb');
const SQS = require('aws-sdk/clients/sqs');
const S3 = require('aws-sdk/clients/s3');

const httpOptions = {
    agent: new https.Agent({
        rejectUnauthorized: false,
        keepAlive: true
    })};

const db = new DynamoDB({
    apiVersion: '2012-08-10',
    httpOptions: httpOptions
});

var sqs = new SQS({
    apiVersion: '2012-11-05',
    httpOptions: httpOptions
});

var s3 = new AWS.S3 ({
    apiVersion: '2006-03-01',
    httpOptions: httpOptions
});

const exec = async (command, args) => {
    return new Promise((resolve, reject) => {
		const process = spawn(command, args);

		let out = '', err = '';

		process.on('exit', (code, signal) => {
			const f = code === 0 ? resolve : reject;

			f({ out: out, err: err });
		});

		process.stdout.on('data', (data) => { out += data; });
		process.stderr.on('data', (data) => { err += data; });
    });
}

const execToS3 = async (command, args, bucket, key) => {
    return new Promise((resolve, reject) => {
    	let processComplete = false,
    		processSuccess = false,
    		uploadComplete = false,
    		uploadSuccess = false,
    		versionId = null;

		const deleteObjectAndReject = () => {
			let params = { Bucket: bucket, Key: key };
			if (versionId)
				params.VersionId = versionId;

			s3.deleteObject(params, (err, data) => {
				reject(processErr);
			})
		}

		const process = spawn(command, args);

		const upload = s3.upload({ Bucket: bucket, Key: key, Body: process.stdout }, 
			(err, data) => {
				uploadComplete = true;

				if (err) {
					if (err.code === 'RequestAbortedError') {
						reject(processErr);
					} else {
						if (!processComplete) {
							process.kill();
						}

						reject(err);
					}
				} else {
					// need the version id to delete the object if the process fails
					versionId = data.VersionId;

					uploadSuccess = true;

					if (processSuccess) {
						resolve(processErr);
					} else if (processComplete) {
						deleteObjectAndReject();
					}
				}
			});

		process.on('exit', (code, signal) => {
			processComplete = true;
			if (code === 0) {
				processSuccess = true;

				if (uploadSuccess) {
					resolve(processErr);
				}
			} else {
				if (uploadSuccess) {
					deleteObjectAndReject();
				} else if (!uploadComplete) {
					upload.abort();
				}
			}
		});

		let processErr = '';

		process.stderr.on('data', (data) => { 
			// keep the first 2k of the stderr
			if (processErr.length + data.length <= 2048) {
				processErr += data;
			}
		});
    });
}

const getS3Object = async (bucket, key, os) => {
    return new Promise((resolve, reject) => {
    	const req = s3.getObject({ Bucket: bucket, Key: key });

		let headers = null;
		req.on('httpHeaders', (err, data) => {
			headers = data;
		});

        const is = req.createReadStream();

        is.on('error', (err) => {
            os.destroy();
            reject(err);
        });

        os.on('error', (err) => {
            is.destroy();
            reject(err);
        });

        os.on('close', () => { resolve(headers) } );

        is.pipe(os);
    });
}

const parseS3Filename = (fileName) => {
	const parsed = new url.URL(fileName);

	return { bucket: parsed.host, key: parsed.pathname.substring(1) };
}

const processFile = async (itemID, fileName) => {
	console.log('Processing: ', itemID, fileName);

	const s3Obj = parseS3Filename(fileName);
	const local = '/tmp/asset' + path.extname(s3Obj.key);

	try {
		await getS3Object(s3Obj.bucket, s3Obj.key, fs.createWriteStream(local));
	} catch (e) {
		console.error('GetObjectFailed', e);
		return;
	}

		//const tags = '/tmp/tags.txt';
		//const ret = await exec('exiftool', [local, '-json', '-g1', '-s']);
		//await s3.upload({ Bucket: s3Obj.bucket, Key: keyTags, Body: ret.out }).promise();

	try {
		const keyTags = ".media/" + itemID + "/tags.txt";
		await execToS3('exiftool', [local, '-json', '-g1', '-s'], s3Obj.bucket, keyTags);
	} catch (e) {
		console.error('GetTagsFailed', e);
	}

	try {
		const keyThumb = ".media/" + itemID + "/thumb.png";
		await execToS3('convert', ['-quiet', '(', '-auto-orient', '-resize', '250x250', local, ')', '-strip', 'png:-'], s3Obj.bucket, keyThumb);
	} catch (e) {
		console.error('CreateThumbFailed', e);
	}

	console.log('Processed: ', itemID, fileName);
}

const processQueue = async () => {
	// get queue URL
	const queueName = 'MediaTranscoder';

	const queueURL = await sqs.getQueueUrl({ QueueName: queueName })
		.promise()
		.then((data) => data.QueueUrl)
		.catch((e) => {
			console.error(e);
			return null;
		});

	if (queueURL == null) {
		console.error("GetQueueURLFailed", queueName);
		return null;
	}

	// process messages until none found
	while (true) {
		const msg = await sqs.receiveMessage({
			AttributeNames: ["SentTimestamp"],
			MaxNumberOfMessages: 1,
			QueueUrl: queueURL,
			VisibilityTimeout: 20,
			WaitTimeSeconds: 0
			})
			.promise()
			.then((data) => {
				if (data.Messages) {
					return data.Messages[0];
				}

				return null;
			})
			.catch((err) => {
				console.error(err);

				return null;
			});

		if (msg === null) {
			console.log("No more messages");
			break;
		}
	
		const body = JSON.parse(msg.Body);
		
		await processFile(body.itemID, body.fileName);

		sqs.deleteMessage({ QueueUrl: queueURL, ReceiptHandle: msg.ReceiptHandle })
			.promise()
			.then((data) => {
				console.log('');
			})
			.catch((e) => {
				console.error('DeleteMsgFailed', e);
			});	
	}
}

processQueue().then(() => { console.log("Task complete"); });
