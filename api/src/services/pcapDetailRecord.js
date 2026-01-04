const path = require("path");
const fs = require('fs');
const db = require(path.join(__dirname, "../utils/db.connection"));
const moment = require("moment");
const helper = require('../utils/helper');
require("dotenv").config({ path: __dirname + "/./../../.env" });

// difference between two entries in the MP DB
const cdrDiff = parseInt(process.env.MP_DB_OFFSET) || 3;

/**
 * @namespace CDR
 */

/**
 * BaseQuery which extracts all the CDR related Data from DB.
 * @type {string}
 */
const baseQuery =
  `SELECT 
CONVERT(c.id, CHAR) AS massId, 
CONVERT(c.cdr_id, CHAR) AS cdrId,
c.caller,
c.called,
DATE_FORMAT(c.call_date, '%Y-%m-%dT%H:%i:%s%+0400') AS calldate,
DATE_FORMAT(c.call_end, '%Y-%m-%dT%H:%i:%s%+0400') as callend,
CONVERT(c.batch_id, CHAR) as batchId,
c.call_id as callId,
CONVERT(c.conversion_status, CHAR) AS pcapConversionStatus, 
DATE_FORMAT(c.conversion_date, '%Y-%m-%dT%H:%i:%s%+0400') AS pcapConversionDate, 
c.storage_path as storagePath, 
c.error_msg as errorMsg
FROM conversion_log c `;

const pcapStatusQuery =
  `SELECT
c.conversion_status,
c.storage_path,
c.error_msg,
c.cdr_id,

FROM conversion_log c
WHERE c.id = ?`;

/**
 * This API endpoint return data against a specific PCAP ID.
 *
 * @memberOf CDR
 * @param {string} Id - The PCAP ID for which record needs to be fetched
 * @returns {object} - Details about that specific PCAP ID.
 */
async function getPcapStatus(massID) {
  let cdr = await db.query(
    baseQuery +
    ` where c.id = ?`,
    [massID]
  );

  // Check if the query returned a result
  if (cdr.length > 0) {
    cdr = cdr.map(record => ({
      ...record,
      pcapConversionStatus: record.pcapConversionStatus === '0'
        ? 'not converted'
        : 'converted'
    }));
    console.debug("UPDATED CDR being sent to controller: ", cdr);
    return { pcapInfo: cdr };
  }
  else {
    return null;
  }
}


/**
 * This API endpoint return data against a specific PCAP ID.
 *
 * @memberOf CDR
 * @param {string} Id - The PCAP ID for which record needs to be fetched
 * @returns {object} - Details about that specific PCAP ID.
 */

async function getPcapStatusByCDRId(cdrId) {
  let cdr = await db.query(
    baseQuery +
    ` where c.cdr_id = ?`,
    [cdrId]
  );

  // Check if the query returned a result
  if (cdr.length > 0) {
    cdr = cdr.map(record => ({
      ...record,
      pcapConversionStatus: record.pcapConversionStatus === '0'
        ? 'not converted'
        : 'converted'
    }));
    console.debug("UPDATED CDR being sent to controller: ", cdr);
    return { pcapInfo: cdr };
  }
  else {
    return null;
  }
}

/**
 * This API endpoint return data against a specific PCAP ID.
 *
 * @memberOf CDR
 * @param {string} batchId - The Batch ID for which record needs to be fetched
 * @returns {object} - Details about that specific Batch ID.
 */

async function getPcapBatchInfo(batchId) {
  let batch = await db.query(
    `SELECT CONVERT(batch_id,CHAR) as batchId, DATE_FORMAT(call_date, '%Y-%m-%dT%H:%i:%s%+0400') AS calldate, sensor_id as id_sensor, file_path, CONVERT(mass_start_id, CHAR) as startMassId, CONVERT(mass_end_id, CHAR) as endMassId, DATE_FORMAT(conversion_date, '%Y-%m-%dT%H:%i:%s%+0400') AS pcapConversionDate 
      FROM batch_tracker
      WHERE batch_id = ?`,
    [batchId]
  );

  // Check if the query returned a result
  if (batch.length > 0) {

    console.debug("UPDATED batch being sent to controller: ", batch);
    return { batchInfo: batch };
  }
  else {
    return null;
  }
}

/**
 * This API endpoint returns data for a batch of PCAP IDs.
 *
 * @memberOf CDR
 * @param {number} offset - The starting point for fetching records
 * @param {number} size - The number of records to fetch
 * @returns {object} - Details about the batch of PCAP IDs.
 */
async function getBatchPCAPsStatus(offset, size) {
  let cdr = await db.query(
    baseQuery +
    `where c.id >= ? order by id limit 0,?`,
    [offset, size]
  );
  console.debug("CDR: ", cdr);
  if (cdr.length == 0) {
    return { pcapInfo: [], status: 404 };
  }
  else if (cdr.length == size) {
    return { pcapInfo: cdr, status: 200 }; // Partial Content
  }
  else {
    console.log(`Returning ${cdr.length} CDRs with 206 status code, and last Mass ID is ${cdr[cdr.length - 1].massId}`);
    return { pcapInfo: cdr, status: 206 };
  }
}

/**
 * This API endpoint returns PCAP against a specific Mass ID.
 *
 * @memberOf CDR
 * @param {string} massId - The Mass ID for which record needs to be fetched
 * @returns {object} - PCAP for that specific PCAP ID.
 */
async function getPCAP(massId) {
  try {
    // Check conversion status for the Mass ID
    const conversionStatus = await db.query(pcapStatusQuery, [massId]);

    // Check if massId exists
    if (conversionStatus.length === 0) {
      return {
        statusCode: 404,
        stderr: "No record found for the provided Mass ID.",
        success: false
      };
    }

    const { conversion_status, error_msg, storage_path } = conversionStatus[0];

    // Check conversion status (ensuring proper type comparison)
    if (parseInt(conversion_status) !== 1) {
      const fallbackError =
        error_msg && error_msg !== 'null' ? error_msg : "PCAP conversion failed for the given Mass ID.";
      return {
        statusCode: 404,
        stderr: fallbackError,
        success: false
      };
    }
    // Check if storage path is available
    if (!storage_path) {
      return {
        statusCode: 404,
        stderr: "No storage path found in the database for the provided Mass ID.",
        success: false
      };
    }

    //Check if PCAP exists and is not empty
    try {
      const fileStats = fs.statSync(storage_path);

      // Check if the file size is 0
      if (fileStats.size === 0) {
        return {
          statusCode: 404,
          stderr: "PCAP file is empty for the provided Mass ID.",
          success: false
        };
      }
    } catch (fsError) {
      return {
        statusCode: 404,
        stderr: "PCAP file for the provided Mass ID is missing from storage.",
        success: false
      };
    }

    // Return the prepared PCAP metadata
    return {
      statusCode: 200,
      pcapPath: storage_path,
      pcapName: path.basename(storage_path),
      success: true
    };
  } catch (error) {
    console.error(`Error while retrieving PCAP file: ${error.message}`);
    return {
      statusCode: 500,
      stderr: "An error occurred while retrieving the PCAP file.",
      error: error.message,
      success: false
    };
  }
}
async function addToTimestampQueue(date, key, sensor, redisClient) {
  const TIME_LIST_KEY = 'timestampQueue';
  const TIME_SET_KEY = 'timestampQueueSet';
  const entry = JSON.stringify({ date, id_sensor: key, sensor });
  const exists = await redisClient.sIsMember(TIME_SET_KEY, entry);
  if (!exists) {
    await redisClient.lPush(TIME_LIST_KEY, entry);    // Add to start of list
    await redisClient.sAdd(TIME_SET_KEY, entry);      // Track in set to avoid duplicates
    console.debug(`Added: ${entry}`);
  } else {
    console.debug(`Duplicate skipped: ${entry}`);
  }
}
async function fillTimestampQueue(sensors, startTime, endTime, redisConn) {
  let start = moment(startTime, "YYYY-MM-DDTHH:mm", true).seconds(0);
  let end = moment(endTime, "YYYY-MM-DDTHH:mm", true).seconds(0);
  console.log(`Filling timestamp queue from ${start.format("YYYY-MM-DDTHH:mm:ss")} to ${end.format("YYYY-MM-DDTHH:mm:ss")}`);
  // Loop from end to start to maintain chronological order in the list
  for (let m = end.clone(); m.isSameOrAfter(start); m.subtract(1, "minute")) {
    const minute = m.format("YYYY-MM-DD HH:mm:ss");
    for (const key in sensors) {
      await addToTimestampQueue(minute, key, sensors[key], redisConn);
    }
  }
}
async function cdrsReprocess(startTime, endTime) {
  // Validate the date-time format
  const validator = helper.dateTimeValidator(startTime, endTime);
  console.log(`Reprocessing CDRs from ${startTime} to ${endTime}`);
  if (!validator || !validator.status) {
    console.error(validator.stderr || "Invalid date-time format. Please use 'YYYY-MM-DDTHH:mm'.");
    return {
      statusCode: 400,
      stderr: validator.stderr || "Invalid date-time format. Please use 'YYYY-MM-DDTHH:mm'.",
      success: false
    };
  }
  const redis = require("../utils/redisClient");
  let redisConn;
  try {
    redisConn = await redis.connect();
  } catch (error) {
    console.error(`Error connecting to Redis: ${error.message}`);
    return {
      statusCode: 500,
      stderr: `Error connecting to Redis`,
      success: false
    };
  }
  try {
    let sensors = await redisConn.hGetAll('sensors');
    console.log(`Sensors fetched from Redis: ${JSON.stringify(sensors)}`);
    await fillTimestampQueue(sensors, startTime, endTime, redisConn);
    return {
      message: `Timestamp queue filled successfully from ${startTime} to ${endTime}.`,
      success: true
    };
  } catch (error) {
    console.error(`Error while filling timestamp queue: ${error.message}`);
    return {
      statusCode: 500,
      stderr: `Error while filling timestamp queue`,
      success: false
    };
  } finally {
    console.log(`Reprocessing CDRs from ${startTime} to ${endTime} completed.`);

    // Close the Redis connection
    if (redisConn)
      await redis.closeConnection();
  }
}

module.exports = {
  getPcapStatus,
  getBatchPCAPsStatus,
  getPCAP,
  getPcapStatusByCDRId,
  getPcapBatchInfo,
  cdrsReprocess
};