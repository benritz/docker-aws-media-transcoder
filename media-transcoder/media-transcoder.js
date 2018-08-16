const https = require('https');
const path = require('path');
const url = require('url');
const fs = require('fs');
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

const downloadFile = async (bucket, key, os) => {
    return new Promise((resolve, reject) => {
    	console.log(1);
        const is = s3.getObject({ Bucket: bucket, Key: key }).createReadStream();

		console.log(is);
		console.log(2);

        is.on('error', (err) => {
        	console.log("is error");
        	console.log(err);
            os.destroy();
            reject(err);
        });

		console.log(3);
        
        os.on('error', (err) => {
        	console.log("os error");
        	console.log(err);
            is.destroy();
            reject(err);
        });

		console.log(4);
        
        os.on('close', resolve);

		console.log(5);
        
        is.pipe(os);

		console.log(6);
    });
}

const processFile = async (itemID, fileName) => {
	// parse s3 file name
	const parsed = new url.URL(fileName);
	const bucket = parsed.host;
	const key = parsed.pathname.substring(1);

	const basename = path.basename(key);
	
	console.log(bucket);
	console.log(key);
	console.log(basename);

	// download file
	const local = '/tmp/' + basename;

	console.log(local);

	try {
		const os = fs.createWriteStream(local);
	
		console.log(os);
	
		await downloadFile(bucket, key, os);
		
		console.log("downloaded");
	
		const stats = fs.statSync(local);
	
		console.log(stats);
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
