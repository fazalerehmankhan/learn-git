const { mp_query } = require('./db.connection');
const redisClient = require('../redisClient');
require('dotenv').config();

let isProcessing = false;
const BATCH_SIZE = parseInt(process.env.OUT_QUEUE_BATCH_SIZE, 10) || 1000;
// Redis-based lock key and expiry time
const LOCK_KEY = 'lock:writeToMPDB';
const LOCK_EXPIRE_SECONDS = 60; // to auto-release if something goes wrong
const LOCK_REFRESH_INTERVAL_MS = 10000;

// Lock helper functions
const acquireLock = async () => {
  const result = await redisClient.set(LOCK_KEY, 'locked', {
    NX: true,
    EX: LOCK_EXPIRE_SECONDS,
  });
  return result === 'OK';
};

const releaseLock = async () => {
  await redisClient.del(LOCK_KEY);
};
const refreshLockTTL = async () => {
  const success = await redisClient.expire(LOCK_KEY, LOCK_EXPIRE_SECONDS);
  if (success === 1) {
    console.debug('Lock TTL refreshed');
  } else {
    console.warn('Lock TTL could not be refreshed (possibly lost lock)');
  }
};
// Get last batch_id and increment atomically
const getNextBatchId = async () => {

  // get current last_batch_id
  const rows = await mp_query('SELECT max(batch_id) as batch_id FROM batch_tracker');
  let lastBatchId = parseInt(rows[0]?.batch_id || 0, 10);

  const nextBatchId = lastBatchId + 1;
  console.debug(`Next batch_id to use: ${nextBatchId}`);

  return nextBatchId;
};
const writeToMPDB = async () => {
  /*if (isProcessing) {
    console.log('writeToMPDB is already running. Skipping...');
    return;
  }

  isProcessing = true;*/
  const lockAcquired = await acquireLock();
  if (!lockAcquired) {
    console.log('writeToMPDB is already running in another worker. Skipping...');
    return;
  }
  console.log('Lock acquired. Starting writeToMPDB...');
  //console.log('Starting writeToMPDB...');
  let refreshTimer;
  try {
    // Start TTL refresher
    refreshTimer = setInterval(refreshLockTTL, LOCK_REFRESH_INTERVAL_MS);
    while (true) {
      const result = await redisClient.lRange('outQueue', 0, 0);
      const entry = result[0];

      if (!entry) {
        console.debug('No more entries in Redis outQueue');
        break;
      }

      let records;
      let parsed;
      try {
        parsed = JSON.parse(entry);
        records = parsed.records;
      } catch (error) {
        console.error('Failed to parse JSON entry from Redis:', error);
        await redisClient.lTrim('outQueue', 1, -1);
        continue;
      }

      if (!records || records.length === 0) {
        console.debug('Empty records array in entry');
        await redisClient.lTrim('outQueue', 1, -1);
        continue;
      }
      let batchId = await getNextBatchId();
      const lastRecord = await mp_query(`SELECT max(id) as id FROM conversion_log`);
      await mp_query(`INSERT INTO batch_tracker (batch_id, call_date, sensor_id, file_path, mass_start_id, mass_end_id, conversion_date) VALUES (${batchId}, "${parsed.minute}", "${parsed.id_sensor}", "${parsed.storage_path}", ${lastRecord[0].id + 1}, ${"NULL"}, "${parsed.currentDate}")`);
      // Insert in batches
      console.log(`Inserting ${records.length} records of calldate: ${records[0].call_date} in batches of ${BATCH_SIZE}...`);
      for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        // const values = batch.map(record => {
        //   const {
        //     cdr_id,
        //     call_date,
        //     call_end,
        //     called,
        //     caller,
        //     call_id,
        //     conversion_status,
        //     conversion_date,
        //     storage_path,
        //     error_msg,
        //     client_id,
        //   } = record;

        //   return `(${cdr_id},
        //     "${call_date}",
        //     "${call_end}",
        //     "${caller}",
        //     "${called}",
        //     "${call_id}",
        //     ${conversion_status},
        //     ${conversion_date ? `"${conversion_date}"` : "NULL"},
        //     ${storage_path ? `"${storage_path}"` : "NULL"},
        //     "${error_msg}",
        //     ${client_id ? `"${client_id}"` : "NULL"},
        //     ${batchId})`;
        // }).join(', ');

        // const insertStatement = `
        //   INSERT INTO conversion_log (cdr_id, call_date, call_end, caller, called, call_id, conversion_status, conversion_date, storage_path, error_msg, client_id, batch_id)
        //   VALUES ${values};
        // `;
        const valuesPlaceholders = batch.map(() =>
          `(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).join(', ');

        const insertStatement = `
          INSERT INTO conversion_log
          (cdr_id, call_date, call_end, caller, called, call_id, conversion_status,
          conversion_date, storage_path, error_msg, client_id, batch_id)
          VALUES ${valuesPlaceholders};
        `;

        const params = [];
        for (const record of batch) {
          const {
            cdr_id,
            call_date,
            call_end,
            called,
            caller,
            call_id,
            conversion_status,
            conversion_date,
            storage_path,
            error_msg,
            client_id,
          } = record;

          params.push(
            cdr_id,
            call_date,
            call_end,
            caller,
            called,
            call_id,
            conversion_status,
            conversion_date ?? null,
            storage_path ?? null,
            error_msg ?? "null",
            client_id ?? null,
            batchId
          );
        }
        try {
          console.debug('MP DB Query:', insertStatement, params);
          await mp_query(insertStatement, params);
        } catch (dbError) {
          console.error('DB Insertion failed for a batch. Entry not removed from Redis:', dbError);
          return; // stop processing to avoid data loss
        }
      }
      const lastID = await mp_query(`SELECT max(id) as id FROM conversion_log`);
      await mp_query(`UPDATE batch_tracker SET mass_end_id = ${lastID[0].id} WHERE batch_id = ${batchId}`);
      // After all batches for this entry are inserted, remove the Redis entry
      await redisClient.lTrim('outQueue', 1, -1);
      console.log(`Entry ${batchId} inserted and removed from Redis`);
    }
  } finally {
    clearInterval(refreshTimer);
    await releaseLock();
    console.log('writeToMPDB finished and lock released');
  }
};

module.exports = { writeToMPDB };