#!/usr/bin/env node
//index.js
const net = require('net');
const helper = require('./src/utils/helper');
const { activeProcesses } = helper;
const pcapProcessor = require('./src/services/pcapProcessor')
const { deleteEmptyFolders } = require('./src/utils/cleanTmp');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { Mutex } = require('async-mutex');
require('./src/utils/logging')
const fs = require('fs').promises;
const xfvm = require('./src/utils/xfvm');
const serverIP = process.env.SERVER_IP || "localhost";
const serverPort = process.env.SERVER_PORT || 8000;
const clientIP = process.env.CLIENT_IP || "localhost";
const clientPort = process.env.CLIENT_PORT || 8080;
const numWorkers = process.env.WORKERS || 5; // Adjust this based on your requirements
const storagePath = process.env.PCAP_STORAGE_PATH || "/tmp";
const clientId = process.env.CLIENT_ID || -1;
const completedTasksThreshold = process.env.OUT_QUEUE_SIZE || 50; // Threshold for sending completed tasks to the server
const compressionType = process.env.COMPRESSION_TYPE || "zstd"; // Default compression type
const compressionLevel = process.env.COMPRESSION_LEVEL;
const compressionThreads = process.env.COMPRESSION_THREADS || 1; // Default number of threads for compression
const moment = require("moment");
const path = require('path');

if (clientId < 0) {
  console.error("Please write client id in .env file");
  return;
}

// Task queues in memory
let incomingTasksQueue = [];
let completedTasksQueue = [];
let preloadedTasksQueue = [];
let hasCleanedUpAfterDisconnect = true;
let appStartState = true;
let isShuttingDown = false;
let preloadController = null; // AbortController for preload tasks


// Mutex for managing access to the queues
const queueMutex = new Mutex();
const completedTasksMutex = new Mutex();

// Track worker states
const workerStates = new Array(numWorkers).fill(false); // false = idle, true = busy

// TCP client setup
let client;

// Create worker threads
const workers = [];
function createWorker(workerId) {
  const worker = new Worker(__filename, {
    workerData: { id: workerId } // Pass worker ID to worker thread
  });

  worker.on('message', async (tasks) => {
    if (tasks.type === 'assignTaskToXFVM') {
      console.log("Assigning task to XFVM");

      xfvm.assignTaskToXFVM(tasks.payload); // Call in main thread where `client` exists
    } else if (tasks.type === 'REGISTER_PROCESS') {
      // console.log(`[Worker Thread] added process ${tasks.pid} --> ${tasks.command} to activeProcesses set`)
      activeProcesses.set(tasks.pid, {
        command: tasks.command,
        startTime: Date.now(),
      })

    } else if (tasks.type === 'UNREGISTER_PROCESS') {
      // console.log(`[Worker Thread] Deleted ${tasks.pid} -->${tasks.command} from active Processes`)
      activeProcesses.delete(tasks.pid)
    } else {
      console.debug(`Worker ${workerId} processed a tasks:`, tasks);
      const [datePart, timePart] = tasks.currentDate.split(' ');
      const [hour, minute] = timePart.split(':');
      const [cdrDate, cdrTime] = tasks.minute.split(' ');  // "2024-10-01" and "09:40"
      const [cdrHour, cdrMinute] = cdrTime.split(':');
      const tempDirPathSIP = `${process.env.TEMP_STORAGE_PATH || "/media"}/SIP/client_node_${process.env.CLIENT_ID}/worker_${tasks.worker_id}/*`;
      const tempDirPathRTP = `${process.env.TEMP_STORAGE_PATH || "/media"}/RTP/client_node_${process.env.CLIENT_ID}/worker_${tasks.worker_id}/*`;
      await Promise.allSettled([
        helper.sequentialExecution(`rm -f ${tasks.localFilePath}`).catch(error =>
          console.debug("Failed to delete rtp tar file", error)
        ),
        helper.sequentialExecution(`rm -rf ${tempDirPathSIP}`).catch(error =>
          console.debug("Failed to delete temp SIP directory", error)
        ),
        helper.sequentialExecution(`rm -rf ${tempDirPathRTP}`).catch(error =>
          console.debug("Failed to delete temp RTP directory", error)
        )
      ]);
      workerStates[workerId] = false;

      // Assign a new task to the worker if available
      assignTaskToWorker(workerId);
      if (tasks.records.length > 0) {
        const id_sensor = tasks.records[0].id_sensor;
        const mergedDir = `${process.env.PCAP_STORAGE_PATH || "/isilon"}/client_node_${process.env.CLIENT_ID}/${cdrDate}/${cdrHour}/${cdrMinute}/`;
        const temp_mergedDir = `${process.env.TEMP_STORAGE_PATH || "/media"}/client_node_${process.env.CLIENT_ID}/${cdrDate}/${cdrHour}/${cdrMinute}/${id_sensor}`;
        const tarFileName = `${cdrDate}-${cdrHour}-${cdrMinute}-${id_sensor}`;
        try {
          // Compress the folder
          const compressedArchive = await helper.compressFolder(temp_mergedDir, tarFileName, compressionType, compressionLevel, compressionThreads);
          console.log(`cp -r ${path.dirname(temp_mergedDir)}/${tarFileName}* ${mergedDir}`);
          await helper.sequentialExecution(`cp -r ${path.dirname(temp_mergedDir)}/${tarFileName}* ${mergedDir}`);
          console.log(`compression and copy of task ${cdrDate} ${cdrHour}:${cdrMinute} and id_sensor ${id_sensor} finished`);
        } catch (error) {
          console.debug(`Failed to copy files from ${temp_mergedDir} to ${mergedDir}:`, error);
        } finally {
          setTimeout(() => {
            helper.sequentialExecution(`rm -r ${temp_mergedDir} ${path.dirname(temp_mergedDir)}/${tarFileName}*`).catch(error =>
              console.debug(`Failed to delete ${temp_mergedDir} after delay:`, error)
            );
          }, 10 * 60 * 1000); // 10 minutes in milliseconds

        }
      }
      // Push the status update to the completed tasks queue
      let shouldSend = false;
      await completedTasksMutex.runExclusive(async () => {
        completedTasksQueue.push(tasks);
        console.log(`Worker ${workerId} pushed ${tasks.minute} to local memory queue`);
        // Check if the completed tasks queue length threshold is met
        if (completedTasksQueue.length >= completedTasksThreshold) {
          shouldSend = true;
        }
      });
      if (shouldSend) {
        await sendCompletedTasksToServer(); // ✅ Safe and clean
        console.log("SEND TASK TO SERVER FINISHED");
      }
      console.log("Completed task queue length: ", completedTasksQueue.length);
      // Mark worker as idle
    }
  });

  worker.on('error', (error) => {
    console.error(`Worker ${workerId} error: ${error}`);
  });

  worker.on('exit', (code) => {
    if (code !== 0) {
      console.error(`Worker ${workerId} stopped with exit code ${code}`);
      // If a worker exits with an error, create a new worker to replace it
      createWorker(workerId);
    }
  });

  workers.push(worker);
}
// Function to stop all workers gracefully
function stopWorkers() {
  return Promise.all(workers.map(worker => {
    return new Promise((resolve, reject) => {
      worker.terminate().then(resolve).catch(reject);
    });
  }));
}
// Function to assign tasks to workers
async function assignTaskToWorker(workerId) {
  // Exit early if worker is busy
  if (workerStates[workerId]) {
    //        console.log("WORKER BUSY");
    setTimeout(() => assignTaskToWorker(workerId), 1000);
    return;
  }
  //      console.log("WORKER FREE");
  await queueMutex.runExclusive(async () => {
    // Double check inside mutex in case state changed during await
    if (!workerStates[workerId] && preloadedTasksQueue.length > 0) {
      const task = preloadedTasksQueue.shift();
      task.worker_id = workerId;
      workerStates[workerId] = true;
      workers[workerId].postMessage(task);
    }
  });
  setTimeout(() => assignTaskToWorker(workerId), 1000);
}

async function copyRTPFiles(task, abortSignal, maxRetries = 3, delayMs = 2000) {
  const [cdrDate, cdrTime] = task.minute.split(' ');
  const [cdrHour, cdrMinute] = cdrTime.split(':');
  const rtpPath = `${task.sensor_path}/${cdrDate}/${cdrHour}/${cdrMinute}/RTP/rtp_${cdrDate}-${cdrHour}-${cdrMinute}.tar`;
  const destinationPath = `${process.env.TEMP_STORAGE_PATH}/RTP/client_node_${process.env.CLIENT_ID}/${task.sensor_path}/${cdrDate}/${cdrHour}/${cdrMinute}/`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    if (abortSignal && abortSignal.aborted) {
      throw new Error('Operation aborted');
    }

    try {
      await fs.mkdir(destinationPath, { recursive: true });

      // Use abortable execution
      const flag = await helper.sequentialExecution(
        `scp -oCiphers=aes128-ctr -oCompression=no -oTCPRcvBufPoll=yes ${rtpPath} ${destinationPath}rtp_${cdrDate}-${cdrHour}-${cdrMinute}.tar`,
        abortSignal
      );

      if (flag === -1) {
        throw new Error(`scp failed with flag -1`);
      }

      console.log(`FILE ${destinationPath}rtp_${cdrDate}-${cdrHour}-${cdrMinute}.tar copied`);
      task.localFilePath = `${destinationPath}rtp_${cdrDate}-${cdrHour}-${cdrMinute}.tar`;
      return task;

    } catch (err) {
      if (err.message === 'Operation aborted') {
        throw err;
      }

      if (err.code === 'ENOENT') {
        console.error(`File not found for task ${task.minute}, not retrying.`);
        break;
      }

      console.warn(`Attempt ${attempt} failed for task ${task.minute}: ${err.message}`);

      if (attempt < maxRetries && !abortSignal?.aborted) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      } else {
        console.error(`All ${maxRetries} attempts failed for task ${task.minute}`);
      }
    }
  }

  task.localFilePath = null;
  return task;
}

async function preloadFilesForTasks() {
  while (!isShuttingDown) {  // Check shutdown flag
    await queueMutex.runExclusive(async () => {
      if (isShuttingDown) return; // Early exit if shutting down

      if (preloadedTasksQueue.length >= numWorkers || incomingTasksQueue.length === 0) {
        return;
      }

      const tasksToPreload = [];
      console.log(`STARTED COPYING RTP FROM ISILON TO CACHE: `, moment().format("YYYY-MM-DD HH:mm:ss"));

      while (tasksToPreload.length < numWorkers && incomingTasksQueue.length > 0) {
        tasksToPreload.push(incomingTasksQueue.shift());
      }

      // Store the promise so we can track it
      preloadController = new AbortController();

      try {
        const copiedTasks = await Promise.all(
          tasksToPreload.map(task => copyRTPFiles(task, preloadController.signal))
        );

        if (!isShuttingDown) {
          console.log(`COMPLETED COPYING RTP FROM ISILON TO CACHE: `, moment().format("YYYY-MM-DD HH:mm:ss"));
          preloadedTasksQueue.push(...copiedTasks);
        }
      } catch (error) {
        if (error.message === 'Operation aborted') {
          console.log('Preload tasks aborted due to shutdown');
        } else {
          console.error('Error in preload tasks:', error);
        }
      }
    });

    if (!isShuttingDown) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  console.log('Preload task loop exited due to shutdown');
}


// Start preloading process
preloadFilesForTasks();


// Function to send completed tasks to the server
async function sendCompletedTasksToServer() {
  await completedTasksMutex.runExclusive(async () => {
    console.log(`SEND TASK TO SERVER STARTED`);

    while (completedTasksQueue.length > 0) {
      const tasksToSend = completedTasksQueue.slice(0, process.env.OUT_QUEUE_SIZE);
      const data = JSON.stringify(tasksToSend) + '\n';

      const sendResult = await new Promise((resolve) => {
        client.write(data, (err) => {
          if (err) {
            console.error('Failed to send completed tasks to server');
            return resolve(false);
          }

          let buffer = '';
          const timeout = setTimeout(() => {
            client.removeListener('data', onData);
            console.error('No ACK received from server (timeout)');
            resolve(false);
          }, 20000); // 20-second timeout

          const onData = (chunk) => {
            buffer += chunk.toString();

            let newlineIndex;
            while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
              const line = buffer.slice(0, newlineIndex).trim();
              buffer = buffer.slice(newlineIndex + 1);

              if (!line) continue;

              try {
                const ack = JSON.parse(line);
                if (ack.type === 'ack' && ack.context === 'cdr_recv') {
                  clearTimeout(timeout);
                  client.removeListener('data', onData);
                  console.log('Received ACK from server');
                  return resolve(true);
                }
              } catch {
                // Ignore malformed lines
              }
            }
          };

          client.on('data', onData);
        });
      });

      if (sendResult) {
        completedTasksQueue.splice(0, tasksToSend.length); // Remove only on successful send
      } else {
        console.warn('Stopping further sends due to error or no ACK');
        break; // Exit loop if ACK failed to avoid endless retries
      }
    }
  });
}

let rejectionRetryCount = 0;
let reconnectRetryCount = 0;
const MAX_REJECTION_RETRIES = 5;
const RETRY_INTERVAL_MS = 10000;
const BASE_RETRY_INTERVAL_MS = 10000; // 10 seconds
const MAX_BACKOFF_MS = 300000; // Cap exponential backoff at 5 minutes
// Function to establish connection to the server
function connectToServer() {
  let isRejected = false;
  client = new net.Socket({ highWaterMark: 1024 * 1024 * 16 });

  client.connect(serverPort, serverIP, clientIP, clientPort, () => {
    hasCleanedUpAfterDisconnect = false;
    reconnectRetryCount = 0;
    rejectionRetryCount = 0;
    console.log('Connected to the TCP server');
    client.write(JSON.stringify({ clientId: clientId }) + '\n');
  });
  client.setKeepAlive(true, 10000);
  let buffer = "";
  client.on('data', (message) => {
    try {
      buffer += message.toString();
      let boundary;

      // Split the data using the newline delimiter
      while ((boundary = buffer.indexOf('\n')) !== -1) {
        const message = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 1);

        if (message.trim()) {

          try {
            if (message.trim() === 'ping') {
              client.write('pong\n');
              continue;
            }
            if (message.includes("Connection rejected")) {
              console.error(message);
              isRejected = true;
              client.end();
              return;
            }
            rejectionRetryCount = 0;
            reconnectRetryCount = 0;
            isRejected = false;
            const receivedCDRArray = JSON.parse(message);
            if (receivedCDRArray.type === 'ack' && receivedCDRArray.context === 'cdr_recv') {
            }
            else if (receivedCDRArray.type === 'cdrs') {
              client.write(JSON.stringify({ type: 'ack', context: 'cdr_send' }) + '\n');
              // const tasks = JSON.parse(receivedCDRArray.toString());
              const tasks = JSON.parse(receivedCDRArray.data);

              // Add received tasks to the local memory queue
              tasks.forEach(task => {
                task = JSON.parse(task);
                incomingTasksQueue.push(task);
                // console.debug('New tasks added to local memory queue:', task);
              });
              console.log('New tasks added to local memory queue');
              // Assign tasks to idle workers
              workers.forEach((_, workerId) => {
                assignTaskToWorker(workerId);
              });
            }
          } catch (error) {
            console.error('Failed to parse JSON:', error);
          }
        }
      }

    } catch (err) {
      console.error('Failed to parse data from server:', err);
    }
  });

  client.on('error', (err) => {
    console.error('TCP client error:', err);
    // setTimeout(connectToServer, 10000);
  });



  client.on('close', async () => {
    if (!hasCleanedUpAfterDisconnect && !appStartState) {

      await handleServerDisconnection();
      hasCleanedUpAfterDisconnect = true;

    } else {
      appStartState = false;
    }

    if (isRejected) {
      rejectionRetryCount++;
      if (rejectionRetryCount <= MAX_REJECTION_RETRIES) {
        console.warn(`Connection rejected. Retrying in ${RETRY_INTERVAL_MS / 1000}s (${rejectionRetryCount}/${MAX_REJECTION_RETRIES})...`);
        setTimeout(connectToServer, RETRY_INTERVAL_MS);
      } else {
        console.error('Max connection rejection retries reached. Exiting.');
        stopWorkers().then(() => {
          console.log('All workers stopped. Exiting.');
          process.exit(1);
        }).catch((err) => {
          console.error('Error stopping workers:', err);
          process.exit(1); // Still exit to avoid hanging
        });
      }
    } else {
      reconnectRetryCount++;
      // Implement exponential backoff with full jitter
      const exponentialDelay = Math.min(BASE_RETRY_INTERVAL_MS * (2 ** (reconnectRetryCount - 1)), MAX_BACKOFF_MS);
      const jitterDelay = Math.floor(Math.random() * exponentialDelay); // Full jitter
      console.warn(`Connection closed. Retrying in ${Math.floor(jitterDelay / 1000)}s (attempt ${reconnectRetryCount})...`);
      setTimeout(connectToServer, jitterDelay);
    }
  });
}


// Track of Active processes globally
// const activeProcesses = new Map();

async function handleServerDisconnection() {
  console.log("==== Server Disconnected. Cleanup Started ... ====");

  // Set shutdown flag immediately
  isShuttingDown = true;

  try {
    // 1. Abort any ongoing preload operations FIRST
    if (preloadController) {
      preloadController.abort();
      console.log('Aborted ongoing preload operations');
    }

    // 2. Kill all spawned processes IMMEDIATELY (before clearing queues)
    await killAllSpawnedProcesses();

    // 3. Clear all task queues
    await queueMutex.runExclusive(async () => {
      const incomingCount = incomingTasksQueue.length;
      const preloadedCount = preloadedTasksQueue.length;

      incomingTasksQueue.length = 0;
      preloadedTasksQueue.length = 0;

      console.log(`Cleared ${incomingCount} incoming and ${preloadedCount} preloaded tasks`);
    });

    await completedTasksMutex.runExclusive(async () => {
      const completedCount = completedTasksQueue.length;
      completedTasksQueue.length = 0;
      console.log(`Discarded ${completedCount} unsent completed tasks`);
    });

    // 4. Terminate all workers
    await terminateAllWorkersImmediate();

    // 5. Clear XFVM queue
    try {
      console.log('Sending CLEAR_QUEUE command to XFVM...');
      await xfvm.connectToXFVM();
      await xfvm.clearQueue();
    } catch (error) {
      console.error('Failed to clear XFVM queue:', error);
    }

    // 6. Cleanup temp files
    await forceCleanupAllTempFiles();

    await new Promise(resolve => setTimeout(resolve, 2000));

    // 7. Reset worker states and recreate workers
    workerStates.fill(false);
    workers.length = 0;

    for (let i = 0; i < numWorkers; i++) {
      createWorker(i);
    }

    await xfvm.closeConnection();

    // Reset shutdown flag
    isShuttingDown = false;

    // Restart preload task loop
    preloadFilesForTasks();

    console.log('=== CLEANUP COMPLETE - READY FOR RECONNECTION ===');

  } catch (error) {
    console.error("Error during cleanup after server disconnection:", error);
  }
}


async function terminateAllWorkersImmediate() {
  console.log('Terminating all workers immediately...');

  // Send shutdown signal to workers first
  workers.forEach((worker, workerId) => {
    try {
      worker.postMessage({ type: 'IMMEDIATE_SHUTDOWN' });
    } catch (error) {
      console.warn(`Failed to send shutdown signal to worker ${workerId}:`, error);
    }
  });

  // Wait briefly then force terminate
  await new Promise(resolve => setTimeout(resolve, 2000));

  workers.forEach((worker, workerId) => {
    try {
      worker.terminate();
      console.log(`Terminated worker ${workerId}`);
    } catch (error) {
      console.warn(`Error terminating worker ${workerId}:`, error);
    }
  });

  workers.length = 0;
  console.log('All workers terminated');
}


function isProcessRunning(pid) {
  try {
    // Sending signal 0 checks if process exists without actually sending a signal
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === 'ESRCH') {
      return false; // Process doesn't exist
    } else if (error.code === 'EPERM') {
      return true; // Process exists but we don't have permission
    } else {
      console.warn(`Unexpected error checking process ${pid}:`, error);
      return false; // Assume it doesn't exist
    }
  }
}

async function killAllSpawnedProcesses() {
  console.log(`Killing ${activeProcesses.size} active spawned processes IMMEDIATELY...`);

  const killPromises = [];

  for (const [pid, info] of activeProcesses.entries()) {
    killPromises.push((async () => {
      try {
        if (info.childProcess && info.childProcess.kill) {
          console.debug(`Killing process tree via child process reference: ${pid}`);

          // Kill the entire process tree
          helper.killProcessTree(pid, 'SIGTERM');

          // Force kill after 500ms
          setTimeout(() => {
            helper.killProcessTree(pid, 'SIGKILL');
          }, 500);
        } else if (isProcessRunning(pid)) {
          console.debug(`Killing process tree via PID: ${pid}`);

          // Kill the entire process tree
          helper.killProcessTree(pid, 'SIGTERM');

          // Force kill after 500ms
          setTimeout(() => {
            helper.killProcessTree(pid, 'SIGKILL');
          }, 500);
        } else {
          console.debug(`Process ${pid} is not running, skipping kill.`);
        }
      } catch (error) {
        console.warn(`Failed to kill process ${pid}:`, error.message);
      }
    })());
  }

  // Wait for all kill operations to complete
  await Promise.allSettled(killPromises);

  // Wait a bit for processes to actually die
  await new Promise(resolve => setTimeout(resolve, 1000));

  activeProcesses.clear();
  console.log('Process cleanup completed');
}



async function forceCleanupAllTempFiles() {
  const client_id = process.env.CLIENT_ID;
  const tempBasePath = process.env.TEMP_STORAGE_PATH;

  const cleanupPaths = [
    `${tempBasePath}/SIP/client_node_${client_id}`,
    `${tempBasePath}/RTP/client_node_${client_id}`,
    `${tempBasePath}/client_node_${client_id}`
  ];

  console.log('Starting aggressive temp file cleanup...');

  const cleanupPromises = cleanupPaths.map(async (cleanupPath) => {
    try {
      const command = `rm -rf ${cleanupPath}/*`;
      await helper.sequentialExecution(command);
      console.log(`Force cleaned: ${cleanupPath}`);
    } catch (error) {
      console.error(`Failed to clean ${cleanupPath}:`, error && (error.message || error.stderr || error));
    }
  });

  await Promise.allSettled(cleanupPromises);
  console.log('Temp file cleanup completed');
}



// Main thread logic
if (isMainThread) {
  // setInterval(() => deleteEmptyFolders(`${process.env.TEMP_STORAGE_PATH || "/media"}/SIP/client_node_${process.env.CLIENT_ID}`), 60 * 1 * 1000);
  // setInterval(() => deleteEmptyFolders(`${process.env.TEMP_STORAGE_PATH || "/media"}/RTP/client_node_${process.env.CLIENT_ID}`), 60 * 1 * 1000);
  // Create worker threads
  for (let i = 0; i < numWorkers; i++) {
    createWorker(i); // Pass unique ID for each worker
  }

  // Establish initial connection to the server
  connectToServer();

} else {
  let totalCount = 0;
  let processedCount = 0; // Store pending tasks
  let completedTasks = {};
  xfvm.connectToXFVM();

  xfvm.onResponse(async (response) => {
    const cdr = await pcapProcessor.handleXFVMResponse(response);
    if (cdr) {
      if (!completedTasks.records) {
        completedTasks.records = [];
      }
      completedTasks.records.push(cdr);
    }
    checkIfTaskComplete(1);
  });
  // Worker thread logic
  const { id: workerId } = workerData; // Extract worker ID from workerData


  async function processDataChunk(chunk) {
    // Start processing
    chunk.client_id = process.env.CLIENT_ID;
    const result = await pcapProcessor.getPcaps(chunk, storagePath);
    return result;
  }
  function checkIfTaskComplete(number) {
    processedCount += number;
    console.debug(`Task ${completedTasks.minute}: Processed ${processedCount} of ${totalCount} tasks`);
    if (processedCount === totalCount) {
      parentPort.postMessage(completedTasks);
      console.log(`TASK ${completedTasks.minute} COMPLETED TIME: `, moment().format("YYYY-MM-DD HH:mm:ss"));
      totalCount = 0;
      processedCount = 0;
      completedTasks = {};
    }
  }
  parentPort.on('message', async (task) => {

    if (task.type === 'IMMEDIATE_SHUTDOWN') {
      console.log(`Worker ${workerId} received immediate shutdown signal`);
      process.exit(0);
    }

    if (task.type == "xfvmResponse") {
      const cdr = await pcapProcessor.handleXFVMResponse(task.data);
      completedTasks.records.push(cdr);
      checkIfTaskComplete(1);
    }
    else {
      const compressionType = process.env.COMPRESSION_TYPE || "zstd";
      let fileExtension = "tar.zst";
      if (compressionType === "lz4") {
        fileExtension = "tar.lz4";
      }
      const [cdrDate, cdrTime] = task.minute.split(' ');  // "2024-10-01" and "09:40"
      const [cdrHour, cdrMinute] = cdrTime.split(':');
      const storage_path = `${storagePath}/client_node_${process.env.CLIENT_ID}/${cdrDate}/${cdrHour}/${cdrMinute}/${cdrDate}-${cdrHour}-${cdrMinute}-${task.id_sensor}.${fileExtension}`;
      completedTasks = {
        records: []
      };
      const currentDate = helper.getCurrentDateTime(); // Timestamp
      task.currentDate = currentDate;
      completedTasks.currentDate = currentDate;
      completedTasks.storage_path = storage_path;
      totalCount = task.records.length;
      completedTasks.localFilePath = task.localFilePath;
      completedTasks.id_sensor = task.id_sensor;
      completedTasks.minute = task.minute;
      completedTasks.worker_id = task.worker_id;
      completedTasks.sensor_path = task.sensor_path;
      completedTasks.client_id = process.env.CLIENT_ID;
      completedTasks.records = [];
      processedCount = 0;
      const result = await processDataChunk(task);
      completedTasks.records.push(...result);
      if (result.length >= 0) {
        checkIfTaskComplete(result.length);
      }
    }
  });

  console.log(`Worker ${workerId} started.`);
}


module.exports = { activeProcesses };
