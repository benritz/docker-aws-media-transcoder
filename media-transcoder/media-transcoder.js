const https = require('https');
const AWS = require('aws-sdk/global');
const DynamoDB = require('aws-sdk/clients/dynamodb');
const SQS = require('aws-sdk/clients/sqs');

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

const processQueue = async () => {
	console.log("Getting Queue URL...");

	// start media transcode
	const queueURL = await sqs.getQueueUrl({ QueueName: 'MediaTranscoder' })
		.promise()
		.then((data) => data.QueueUrl);

	console.log("Queue URL:" + queueURL);

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

processQueue().then(() => { console.log("finished"); });
