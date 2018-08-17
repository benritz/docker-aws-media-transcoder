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

		let out = '';

		process.on('exit', (code, signal) => {
			if (code === 0)
				resolve(out)
			else
				reject();
		});

		process.stdout.on('data', (data) => { out += data; });
    });
}

const downloadFile = async (bucket, key, os) => {
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
	const s3Obj = parseS3Filename(fileName);
	const basename = path.basename(s3Obj.key);

	// download file
	const local = '/tmp/' + basename;

	try {
		await downloadFile(s3Obj.bucket, s3Obj.key, fs.createWriteStream(local));
		const tags = await exec('exiftool', [local, '-json', '-g1', '-s']);
	
		console.log(tags);
		
	} catch (e) {
		console.error(e);
	}
}

const processQueue = async () => {
	console.log("Getting Queue URL...");

	// get queue URL
	const queueURL = await sqs.getQueueUrl({ QueueName: 'MediaTranscoder' })
		.promise()
		.then((data) => data.QueueUrl)
		.catch((e) => {
			console.error(e);
			return null;
		});

	console.log("Queue URL:" + queueURL);
	
	if (queueURL != null) {
		// process messages until none found
		while (true) {
			console.log("Getting message...");
	
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
		
			console.log(msg);
		
			const body = JSON.parse(msg.Body);

			console.log(body.fileName);
			console.log(body.itemID);
			
			await processFile(body.itemID, body.fileName);

			// 			sqs.deleteMessage({ QueueUrl: queueURL, ReceiptHandle: msg.ReceiptHandle })
			// 				.promise()
			// 				.then((data) => {
			// 					console.log(data);
			// 				})
			// 				.catch((err) => {
			// 					console.error(err);
			// 				});	
		}
	}
}

processQueue().then(() => { console.log("finished"); });
