const redisClient = require('./redisClient');
const { da_query } = require('./utils/db.connection');
const moment = require('moment');
const path = require('path');
const fs = require('fs').promises;
const { isFileCompletelyAvailable } = require('./utils/helper');
/**
 * Retrieves sensor data including id_sensor and local_spool.
 * @type {string}
*/
const querySensors = `SELECT id_sensor, local_spool FROM sensors`;

const clearClientsFromRedis = async () => {
	console.log("Clear clients list in Redis");
	await redisClient.lTrim('clients', 1, 0);
}


/**
 * Fetches sensor data from the database and stores it in Redis hash - sensors.
 * Logs the process and any errors.
 * @returns {Promise<void>} - Resolves when data is stored or rejects on error.
 */
async function fetchSensors() {
	try {
		// Fetch sensor data from the database
		const results = await da_query(querySensors);

		// Prepare data for Redis hash
		const sensorData = {};
		results.forEach(row => {
			if (row.id_sensor && row.local_spool) {
				sensorData[row.id_sensor] = row.local_spool;
			}
		});

		// Log the prepared sensor data
		console.debug('Prepared sensor data:', sensorData);

		await redisClient.hSet('sensors', sensorData);
		console.log('All sensor data from DA DB has been stored in Redis hash');

	} catch (error) {
		console.error('Error fetching and storing sensor data from DA DB:', error);
	}
}

const BACKFILL_MINUTES = parseInt(process.env.PAST_DATA_TO_FETCH, 10) || 10;
const CDR_QUEUE_MAX_LENGTH = parseInt(process.env.CDR_QUEUE_THRESHOLD, 10) || 60;
const TIME_LIST_KEY = 'timestampQueue';
const TIME_SET_KEY = 'timestampQueueSet';
const CDR_QUEUE = 'cdrQueue';
const PENDING_LIST = 'pendingList';
// fetch cdr data from DA DB and store in cdrQueue
async function fetchCdrDetails(time, id_sensor) {
	try {
		const fetchedTime = moment(time, "YYYY-MM-DD HH:mm:ss").format("YYYY-MM-DD HH:mm:ss");
		const nextFetchedTime = moment(fetchedTime, "YYYY-MM-DD HH:mm:ss").add(1, 'minutes').format("YYYY-MM-DD HH:mm:ss");
		const results = await da_query(`SELECT cdr.id as cdr_id, cdr.calldate as call_date, cdr.callend as call_end, cdr.caller, cdr.called, cdr.id_sensor, cdr_next.fbasename,
			GROUP_CONCAT(cdr_tar_part.pos ORDER BY cdr_tar_part.pos ASC SEPARATOR ',') as tarPositions FROM cdr 
			INNER JOIN cdr_next ON cdr.id = cdr_next.cdr_ID AND cdr.calldate = cdr_next.calldate  
			LEFT JOIN cdr_tar_part ON cdr.id = cdr_tar_part.cdr_id and cdr.calldate = cdr_tar_part.calldate 
			AND   cdr_tar_part.calldate >='${fetchedTime}' AND cdr_tar_part.calldate <'${nextFetchedTime}'  
			WHERE cdr.calldate >='${fetchedTime}' AND cdr.calldate <'${nextFetchedTime}' 
			AND id_sensor=${id_sensor}         
			GROUP BY cdr.id,cdr.calldate, cdr.caller, cdr.called,cdr.id_sensor,cdr_next.fbasename, cdr.connect_duration         
			ORDER BY cdr.id asc `
		);

		console.debug("Results from DA DB:", results);
		return results;
	} catch (error) {
		console.error('Error fetching CDR details From DA DB:', error.message);
		throw error;
	}
}
async function addToCdrQueue(entry) {
	const { date, id_sensor, sensor } = entry;
	const minute = date.slice(0, 16);
	try {
		const results = await fetchCdrDetails(date, id_sensor);
		if (results.length == 0) {
			console.log(`No CDR records found for ${sensor} at ${date} in DA DB. Pushing back to pending list.`);
			await redisClient.rPush(PENDING_LIST, JSON.stringify(entry));
			return;
		}
		for (let i = 0; i < results.length; i++) {
			results[i].call_date = moment(results[i].call_date).format("YYYY-MM-DD HH:mm:ss");
			results[i].call_end = moment(results[i].call_end).format("YYYY-MM-DD HH:mm:ss");
			results[i].sensor_path = sensor;
		}
		let data = { minute, sensor_path: sensor, records: results, id_sensor: id_sensor };
		await redisClient.rPush(CDR_QUEUE, JSON.stringify(data));
	} catch (error) {
			await redisClient.rPush(PENDING_LIST, JSON.stringify(entry));
			console.error(`Failed to add CDR records for ${sensor} at ${date} to CDR Queue. Moved to pending list. Error:`, error.message);
	}
}

// Function to process the next entry in the timestamp queue
async function processNextTimestampEntry() {
	// Check if CDR queue has space
	const cdrQueueLength = await redisClient.lLen(CDR_QUEUE);
	if (cdrQueueLength >= CDR_QUEUE_MAX_LENGTH) {
		console.warn(`CDR Queue is full (${cdrQueueLength}/${CDR_QUEUE_MAX_LENGTH}).`);
		setTimeout(processNextTimestampEntry, 60 * 1000); // Retry after 1 minute
		return;
	}

	// Pull next entry only if CDR queue has space
	const rawEntry = await redisClient.lPop(TIME_LIST_KEY);
	if (!rawEntry) {
		console.log("No entries in timestamp queue. Waiting for new entries...");
		setTimeout(processNextTimestampEntry, 10 * 3000); // Retry after 30 seconds
		return;
	}
	await redisClient.sRem(TIME_SET_KEY, rawEntry);
	const entry = JSON.parse(rawEntry);
	const { date, id_sensor, sensor } = entry;
	const [datePart, timePart] = date.split(' ');
	const [hour, minute] = timePart.split(':');

	const filePath = path.join(sensor, datePart, hour, minute, `SIP`, `sip_${datePart}-${hour}-${minute}.tar.gz`);
	console.debug(`Processing file: ${filePath}`);
	const currentTime = moment().format('YYYY-MM-DD HH:mm:ss');
	entry.checked_at = currentTime;
	try {
		await fs.access(filePath); // check if file exists and is accessible

		const isAvailable = await isFileCompletelyAvailable(filePath);
		if (isAvailable) {
			await addToCdrQueue(entry);
			console.log(`File found and ready: ${filePath}`);
		} else {
			await redisClient.rPush(PENDING_LIST, JSON.stringify(entry));
			console.debug(`File still writing. Moved to pending list: ${filePath}`);
		}
	} catch (err) {
		await redisClient.rPush(PENDING_LIST, JSON.stringify(entry));
		console.debug(`File missing or inaccessible. Moved to pending list: ${filePath}`);
	}
	processNextTimestampEntry();
}
async function addToTimestampQueue(date, key, sensor) {
	const entry = JSON.stringify({ date, id_sensor: key, sensor });
	const exists = await redisClient.sIsMember(TIME_SET_KEY, entry);
	if (!exists) {
		await redisClient.rPush(TIME_LIST_KEY, entry);    // Add to list
		await redisClient.sAdd(TIME_SET_KEY, entry);      // Track in set to avoid duplicates
		console.debug(`Added: ${entry}`);
	} else {
		console.log(`Duplicate skipped: ${entry}`);
	}
}
// Backfill the timestamp queue with past data
async function backfillTimestampQueue(sensors) {
	const now = moment().startOf('minute');
	for (let i = BACKFILL_MINUTES; i > 0; i--) {
		const minute = now.clone().subtract(i, 'minutes').seconds(0).format('YYYY-MM-DD HH:mm:ss');
		for (const key in sensors) {
			await addToTimestampQueue(minute, key, sensors[key]);
		}
	}
}
// Populate the timestamp queue with the current minute
async function populateCurrentMinute() {
	const minute = moment().startOf('minute').subtract(1, 'minutes').seconds(0).format('YYYY-MM-DD HH:mm:ss');
	let sensors = await redisClient.hGetAll('sensors');
	for (key in sensors) {
		await addToTimestampQueue(minute, key, sensors[key]);
	}
}

async function retryPendingListEntries() {
	const PENDING_LIST_CDR_QUEUE_MAX_LENGTH = parseInt(process.env.PENDING_LIST_CDR_QUEUE_MAX_LENGTH, 10) || 120;
	const pendingLength = await redisClient.lLen(PENDING_LIST);
	const cdrQueueLength = await redisClient.lLen(CDR_QUEUE);
	if (pendingLength === 0 || cdrQueueLength >= PENDING_LIST_CDR_QUEUE_MAX_LENGTH) {
		setTimeout(retryPendingListEntries, (parseInt(process.env.MINUTUES_AFTER_RECHECK, 10) || 1) * 60 * 1000); // Retry after 1 minute
		return;
	}

	const entriesToRetry = await redisClient.lRange(PENDING_LIST, 0, pendingLength - 1);
	const successfullyProcessed = [];

	for (const rawEntry of entriesToRetry) {
		const entry = JSON.parse(rawEntry);
		const { date, id_sensor, sensor, checked_at } = entry;
		const [datePart, timePart] = date.split(' ');
		const [hour, minute] = timePart.split(':');

		const filePath = path.join(sensor, datePart, hour, minute, `SIP`, `sip_${datePart}-${hour}-${minute}.tar.gz`);
		console.debug(`Retrying file: ${filePath}`);

		const handleUnavailable = async () => {
			if (checked_at) {
				const lastChecked = moment(checked_at, "YYYY-MM-DD HH:mm:ss");
				const now = moment();
				const diffMinutes = now.diff(lastChecked, 'minutes');
				const maxRetry = parseInt(process.env.MAX_RETRY_MINUTES, 10) || 5;

				if (diffMinutes >= maxRetry) {
					const checkDB = await da_query(
						`SELECT id FROM cdr 
				 WHERE calldate >= '${date}' 
				   AND calldate < '${moment(date, "YYYY-MM-DD HH:mm:ss").add(1, 'minutes').format("YYYY-MM-DD HH:mm:ss")}' 
				   AND id_sensor = ${id_sensor} 
				 LIMIT 1`
					);
					if (checkDB.length === 0) {
						console.warn(`Max retry time exceeded. Removing entry: ${filePath} from pending list.`);
						successfullyProcessed.push(rawEntry);
					}
				} else {
					console.debug(`File still being written. Skipping retry for now: ${filePath}`);
				}
			}
		};

		try {
			await fs.access(filePath); // Check if file exists
			const isAvailable = await isFileCompletelyAvailable(filePath); // Check if file is stable
			if (isAvailable) {
				console.log(`File ready on retry. Processing: ${filePath}`);
				successfullyProcessed.push(rawEntry);
				await addToCdrQueue(entry);
				console.debug(`Recovered file found. Pushed to CDR Queue: ${filePath}`);
			} else {
				await handleUnavailable();
			}
		} catch {
			await handleUnavailable();
		}
	}
	// Remove successfully processed entries from pending list
	for (const entry of successfullyProcessed) {
		await redisClient.lRem(PENDING_LIST, 1, entry);
	}

	setTimeout(retryPendingListEntries, (parseInt(process.env.MINUTUES_AFTER_RECHECK, 10) || 1) * 60 * 1000); //Schedule next retry only after current execution
}


/**
 * Manages CDR fetching and processing based on the current queue length and threshold.
 * @returns {Promise<void>} - Resolves when the operation is complete or rejects on error.
 */
async function manageCdrQueue() {
	try {
		await fetchSensors();
		let sensors = await redisClient.hGetAll('sensors');
		console.log(sensors);

		backfillTimestampQueue(sensors);

		// // Then every minute add current minute entries
		setInterval(populateCurrentMinute, 60 * 1000);
		processNextTimestampEntry();
		retryPendingListEntries();
		setInterval(async () => {
			try {
				const length = await redisClient.lLen('outQueue'); // print outQueue length after every 10 seconds
				console.log(`Out Queue length: ${length}`);
			} catch (err) {
				console.error('Failed to get Redis Out Queue length:', err.message);
			}
		}, 10 * 1000);
	} catch (error) {
		console.error('Error managing CDR queue:', error.message);
		setTimeout(manageCdrQueue, 10000);
		return;
	}
}
module.exports = { clearClientsFromRedis, manageCdrQueue }