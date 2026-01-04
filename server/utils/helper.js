const path = require("path");
const { promisify } = require("util");
const { exec } = require("child_process");
const execAsync = promisify(exec);
const moment = require("moment");
const { access } = require("fs/promises");
const fs = require('fs');
const redisClient = require('../redisClient');

/**
 * By default time stored in the database is with GMT+0 time, but when results are returned they are returned with exact call timestamps. This function calculates/converts the GMT+0 time to that timestamp.
 * @memberOf HELPERS
 * @param {object} rows - Object containing array of CDR objects.
 * @returns {object} - Object containing array of CDR objects with updated timestamps.
 */
function timeConverter(row) {
  if (!row) return {};
  else {
    let callDate = null;
    let callPath = null;

    callDate = moment(row.calldate);
    callPath = {
      fullDate: callDate.format("YYYY-MM-DD"),
      hour: callDate.format("HH"),
      minute: callDate.format("mm"),
    };
    row.calldate = row.calldate && callDate.format("YYYY-MM-DDTHH:mm:ssZZ");
    return { callPath };
  }
}
const acquireLockWithRetry = async (key, ttl = 30, retries = 10, delayMs = 200) => {
	for (let i = 0; i < retries; i++) {
		const acquired = await redisClient.set(key, '1', 'NX', 'EX', ttl);
		if (acquired === 'OK') return true;
		await new Promise(resolve => setTimeout(resolve, delayMs));
	}
	return false;
};
function getCurrentDateTime() {
  const now = new Date();

  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0'); // Months are zero-indexed
  const day = String(now.getDate()).padStart(2, '0');
  const hours = String(now.getHours()).padStart(2, '0');
  const minutes = String(now.getMinutes()).padStart(2, '0');
  const seconds = String(now.getSeconds()).padStart(2, '0');

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function getFormattedDate(daysBefore, time = 0) {
  const date = new Date();
  date.setDate(date.getDate() - daysBefore);

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are 0-based
  const day = String(date.getDate()).padStart(2, '0');
  if (time == 1) {
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }
  else
    return `${year}-${month}-${day}`;
}

/**
 * Commands executed in NodeJS are async meaning the second command would be executed without waiting for the first one to complete. Inorder to avoid race conditions, sequential execution function executes all the calls in sequence.
 * @memberOf HELPERS
 * @param  {string} commands - Commands which need to be executed on the host machine.
 */
const sequentialExecution = async (...commands) => {
  if (commands.length === 0) {
    return 0;
  }

  let child,
    { stderr } = await ({ child } = execAsync(commands.shift(), {
      maxBuffer: 10486750,
    }));
  if (stderr && child.exitCode !== 0) {
    console.error("Error while decoding audio: ", stderr);
  }

  return sequentialExecution(...commands);
}

function isFileCompletelyAvailable(filePath, interval = 500, retries = 3) {
  return new Promise((resolve, reject) => {
    let previousSize = -1;
    let attempts = 0;

    const checkSize = () => {
      fs.stat(filePath, (err, stats) => {
        if (err) return reject(err);

        if (stats.size === previousSize) {
          return resolve(true); // File size is stable
        }

        previousSize = stats.size;
        attempts++;

        if (attempts >= retries) {
          return resolve(false); // File is still changing
        }

        setTimeout(checkSize, interval);
      });
    };

    checkSize();
  });
}
module.exports = {
  timeConverter,
  sequentialExecution,
  getCurrentDateTime,
  getFormattedDate,
  isFileCompletelyAvailable,
  acquireLockWithRetry
};
