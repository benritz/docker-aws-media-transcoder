const https = require('https');
const path = require('path');
const url = require('url');
const fs = require('fs');
const { spawn } = require('child_process');
const AWS = require('aws-sdk/global');
const DynamoDB = require('aws-sdk/clients/dynamodb');
const SQS = require('aws-sdk/clients/sqs');
const S3 = require('aws-sdk/clients/s3');
const CloudWatch = require('aws-sdk/clients/cloudwatch');

// setup AWS services
https.globalAgent.options.keepAlive = true;

const db = new DynamoDB({ apiVersion: '2012-08-10' });
var sqs = new SQS({ apiVersion: '2012-11-05' });
var s3 = new S3 ({ apiVersion: '2006-03-01' });
var cloudwatch = new CloudWatch({ apiVersion: '2010-08-01' });

// inputs
const queueURL = process.env.QUEUE_URL
if (!queueURL)
	throw "MissingQueueURL"

const maxIdleSeconds = process.env.MAX_IDLE_SECONDS || 60;		// default 1 minute 


const getVisibleMesssages = async () => {
	const endTime = new Date(), startTime = new Date(endTime.getTime() - (60 * 60 * 1000));

	const params = {
		MetricDataQueries: [{
			Id: 'mediaTranscoderVisibleMessages',
			MetricStat: {
				Metric: {
					Dimensions: [{
						Name: 'QueueName',
						Value: 'MediaTranscoder'
					}, ],
					MetricName: 'ApproximateNumberOfMessagesVisible',
					Namespace: 'AWS/SQS'
				},
				Period: 300,
				Stat: 'Average',
				Unit: 'Count'
			},
			ReturnData: true
		}, ],
		StartTime: startTime.toISOString(),
		EndTime: endTime.toISOString(),
		ScanBy: 'TimestampAscending'
	};

	cloudwatch.getMetricData(params)
		.promise()
		.then((data) => {
			const result = data.MetricDataResults.find((result) => result.Id === "mediaTranscoderVisibleMessages");
			if (result) {
				console.log(result);
				console.log(result.Values);
				console.log(result.Timestamps);
			}
		})
		.catch((e) => {
			console.error("GetVisibleMessagesFailed", e);
		});
}

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
	console.time("processFile");
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
		
		console.log("Tags extracted");
	} catch (e) {
		console.error('GetTagsFailed', e);
	}

	try {
		const keyThumb = ".media/" + itemID + "/thumb.png";
		await execToS3('convert', ['-quiet', '(', '-auto-orient', '-resize', '250x250', local, ')', '-strip', 'png:-'], s3Obj.bucket, keyThumb);

		console.log("Thumb generated");
	} catch (e) {
		console.error('CreateThumbFailed', e);
	}

	console.timeEnd("processFile");
}

const processQueue = async () => {
	/*
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
	*/
	
	const visibilityTimeoutSeconds = 20, 
			visibilityTimeoutThreshold = 5, 
			waitTimeSeconds = Math.min(20, maxIdleSeconds);

	let lastProcessedTime = new Date().getTime();

	while (true) {
		const msgs = await sqs.receiveMessage({
			AttributeNames: ["SentTimestamp"],
			MaxNumberOfMessages: 10,
			QueueUrl: queueURL,
			VisibilityTimeout: visibilityTimeoutSeconds,
			WaitTimeSeconds: waitTimeSeconds
			})
			.promise()
			.then((data) => {
				return data.Messages;
			})
			.catch((err) => {
				console.error(err);
				return [];
			});

		const startTime = new Date().getTime();

		if (msgs) {
			// process messages in series using Array.reduce, the previous message's 
			// async result is passed as the accumulator and the final message's async 
			// result is returned
			await msgs.reduce(async (prevAsync, msg) => {
				// wait for previous async result
				await prevAsync;

				// abandon processing message if outside of the visibility timeout
				const runningTime = Math.ceil((new Date().getTime() - startTime) / 1000);
				
				if (runningTime > visibilityTimeoutSeconds - visibilityTimeoutThreshold) {
					console.log("Visibility timeout, ignoring message", msg.MessageId);
					return;
				}

				// process + delete msg
				const body = JSON.parse(msg.Body);

				await processFile(body.itemID, body.fileName);

				await sqs.deleteMessage({ QueueUrl: queueURL, ReceiptHandle: msg.ReceiptHandle })
					.promise()
					.catch((e) => {
						console.error('DeleteMsgFailed', e);
					});
			}, Promise.resolve());

			lastProcessedTime = new Date().getTime();
		} else {
			const idleSeconds = Math.floor((startTime - lastProcessedTime) / 1000);

			console.log("No messages", "Idle seconds=", idleSeconds);

			if (idleSeconds >= maxIdleSeconds) {
				console.log("Exceeded maximum idle seconds");
				break;
			}
		}
	}
}

processQueue()
	.then(() => {
		console.log("Task complete");
	})
	.catch((e) => {
		console.error('TaskFailed', e);
	});
