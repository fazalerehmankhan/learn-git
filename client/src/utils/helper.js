//helper.js
const { access, constants } = require('fs/promises');
const path = require("path");
const { promisify } = require("util");
const { exec, ChildProcess } = require("child_process");
const execAsync = promisify(exec);
const { execSync } = require('child_process');
const moment = require("moment");
const { isMainThread, parentPort } = require('worker_threads');
const crypto = require('crypto');

let activeProcesses = new Map();

/**
 * Get the MD5 hash of a given string.
 * @param {string} str - Input string to hash.
 * @returns {string} - MD5 hash as a 32-character hexadecimal string.
 */
function getStringMD5(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

function getCompressionSettings(compressionType, compressionLevel, compressionThreads) {

  let archiveExt, compressCommand;

  switch (compressionType) {
    case "zstd":
      if (!compressionLevel || compressionLevel < 1 || compressionLevel > 22) {
        compressionLevel = 3; // Default compression level if not provided or invalid
      }
      archiveExt = "tar.zst";
      compressCommand = `tar --use-compress-program="zstd -T${compressionThreads} -${compressionLevel}" -cf`;
      break;

    case "lz4":
      if (!compressionLevel || compressionLevel < 1 || compressionLevel > 12) {
        compressionLevel = 1; // Default compression level if not provided or invalid
      }
      archiveExt = "tar.lz4";
      compressCommand = `tar --use-compress-program="lz4 -${compressionLevel} -T${compressionThreads}" -cf`;
      break;

    default:
      if (!compressionLevel || compressionLevel < 1 || compressionLevel > 12) {
        compressionLevel = 1; // Default compression level if not provided or invalid
      }
      archiveExt = "tar.lz4";
      compressCommand = `tar --use-compress-program="lz4 -${compressionLevel} -T${compressionThreads}" -cf`;
  }

  return { archiveExt, compressCommand };
}
async function compressFolder(dir, fileName, compressionType, compressionLevel, compressionThreads = 1) {
  const { archiveExt, compressCommand } = getCompressionSettings(compressionType, compressionLevel, compressionThreads);
  const folderName = path.basename(dir);
  const outputFile = path.join(path.dirname(dir), `${fileName}.${archiveExt}`);
  const command = `${compressCommand} ${outputFile} -C ${path.dirname(dir)}/${folderName} .`;

  console.log(`Running compression command: ${command}`);
  try {
    await sequentialExecution(command);
  } catch (error) {
    console.error(`Error compressing folder: ${error.message}`);
    throw error;
  }
  return outputFile;
}
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


/**
 * Kill a process and all its children
 * @param {number} pid - Process ID to kill
 * @param {string} signal - Signal to send (default: SIGTERM)
 */
function killProcessTree(pid, signal = 'SIGTERM') {
  try {
    // On Linux, we can use pkill to kill the process group
    // First try to get all child PIDs
    const childPids = getChildPids(pid);
    const allPids = [pid, ...childPids];

    console.log(`Killing process tree for PID ${pid}: [${allPids.join(', ')}]`);

    // Kill all processes in the tree
    for (const targetPid of allPids) {
      try {
        process.kill(targetPid, signal);
      } catch (e) {
        if (e.code !== 'ESRCH') { // Process doesn't exist
          console.warn(`Failed to kill PID ${targetPid}:`, e.message);
        }
      }
    }

    // Alternative: Use process group kill (more reliable)
    try {
      // Kill the entire process group
      process.kill(-pid, signal);
    } catch (e) {
      // This might fail if process isn't a group leader
    }
  } catch (error) {
    console.error(`Error killing process tree for PID ${pid}:`, error);
  }
}

/**
 * Get all child PIDs of a parent process
 * @param {number} parentPid - Parent process ID
 * @returns {number[]} Array of child PIDs
 */
function getChildPids(parentPid) {
  try {
    // Use pgrep to find child processes
    const result = execSync(`pgrep -P ${parentPid}`, { encoding: 'utf8' });
    return result.trim().split('\n').filter(pid => pid).map(Number);
  } catch (error) {
    // pgrep returns non-zero if no children found
    return [];
  }
}






/**
 * Commands executed in NodeJS are async meaning the second command would be executed without waiting for the first one to complete. Inorder to avoid race conditions, sequential execution function executes all the calls in sequence.
 * @memberOf HELPERS
 * @param  {string} commands - Commands which need to be executed on the host machine.
 */

const sequentialExecution = async (command, abortSignal) => {
  if (abortSignal && abortSignal.aborted) {
    return -1;
  }

  return new Promise((resolve) => {
    const child = exec(command, { maxBuffer: 10486750, detached: true });

    let stderrData = '';
    let stdoutData = '';

    // Capture output early
    child.stdout?.on('data', (data) => (stdoutData += data));
    child.stderr?.on('data', (data) => (stderrData += data));

    // Handle abort signal
    if (abortSignal) {
      const abortHandler = () => {
        console.log(`Killing process tree for PID ${child.pid} due to abort signal`);
        killProcessTree(child.pid, 'SIGTERM');
        setTimeout(() => killProcessTree(child.pid, 'SIGKILL'), 1000);
      };
      abortSignal.addEventListener('abort', abortHandler);
      child.on('exit', () => abortSignal.removeEventListener('abort', abortHandler));
    }

    child.on('close', (code) => {
      if (abortSignal?.aborted) {
        console.warn(`Command aborted: ${command}`);
        return resolve(-1);
      }

      if (code === 0) {
        return resolve(0);
      } else {
        console.error(`Command failed [${command}] with code ${code}\n${stderrData}`);
        return resolve(-1);
      }
    });

    child.on('error', (err) => {
      console.error(`Error executing command [${command}]: ${err.message}`);
      resolve(-1);
    });
  });
};



function getDateTimeComponents(dateTimeString) {
  const date = new Date(dateTimeString);

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are zero-indexed
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');

  return { year, month, day, hours, minutes, seconds };
}



/**
 * This function checks if the file SIP and RTP file exists on the file system.
 * @memberOf HELPERS
 * @param {object} data - Contains pcap related data such as file paths, calldate, callId etc. This is used to verify if files exist on the server and whether if RTP for that file exsits.
 * @returns {object} - Returns an object containing the status of the command and whether file exists.
 */

async function fileChecker(path) {
  // async function fileChecker({ rows, callPath, pcapTars }) {
  // const checkFile = async (filePath) => {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch (err) {
    return false;
  }
}


module.exports = {
  timeConverter,
  sequentialExecution,
  fileChecker,
  getDateTimeComponents,
  getCurrentDateTime,
  compressFolder,
  activeProcesses,
  killProcessTree,
  getStringMD5
};
