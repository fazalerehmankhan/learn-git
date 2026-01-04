const { Mutex } = require('async-mutex');
const helper = require('./helper');
const xfvm = require('./xfvm');

// Getter and setter functions
let getters = {};
let setters = {};

// Mutex instances
const queueMutex = new Mutex();
const completedTasksMutex = new Mutex();

/**
 * Initialize the server disconnect handler with getter/setter functions
 */
function initializeHandler(callbacks) {
    getters = {
        getIncomingTasksQueue: callbacks.getIncomingTasksQueue,
        getPreloadedTasksQueue: callbacks.getPreloadedTasksQueue,
        getCompletedTasksQueue: callbacks.getCompletedTasksQueue,
        getWorkerStates: callbacks.getWorkerStates,
        getWorkers: callbacks.getWorkers,
        getPreloadController: callbacks.getPreloadController,
        getIsShuttingDown: callbacks.getIsShuttingDown,
        getHasCleanedUpAfterDisconnect: callbacks.getHasCleanedUpAfterDisconnect,
        getAppStartState: callbacks.getAppStartState
    };

    setters = {
        setIsShuttingDown: callbacks.setIsShuttingDown,
        setHasCleanedUpAfterDisconnect: callbacks.setHasCleanedUpAfterDisconnect,
        setAppStartState: callbacks.setAppStartState
    };
}

/**
 * Handle server disconnection with comprehensive cleanup
 */
async function handleServerDisconnection() {
    console.log("==== Server Disconnected. Cleanup Started ... ====");

    // Set shutdown flag immediately
    setters.setIsShuttingDown(true);

    try {
        // 1. Abort any ongoing preload operations FIRST
        const preloadController = getters.getPreloadController();
        if (preloadController) {
            preloadController.abort();
            console.log('Aborted ongoing preload operations');
        }

        // 2. Kill all spawned processes IMMEDIATELY (before clearing queues)
        await killAllSpawnedProcesses();

        // 3. Clear all task queues
        await queueMutex.runExclusive(async () => {
            const incomingTasksQueue = getters.getIncomingTasksQueue();
            const preloadedTasksQueue = getters.getPreloadedTasksQueue();

            const incomingCount = incomingTasksQueue.length;
            const preloadedCount = preloadedTasksQueue.length;

            incomingTasksQueue.length = 0;
            preloadedTasksQueue.length = 0;

            console.log(`Cleared ${incomingCount} incoming and ${preloadedCount} preloaded tasks`);
        });

        await completedTasksMutex.runExclusive(async () => {
            const completedTasksQueue = getters.getCompletedTasksQueue();
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

        // 7. Reset worker states (worker recreation handled by main app)
        const workerStates = getters.getWorkerStates();
        workerStates.fill(false);

        const workers = getters.getWorkers();
        workers.length = 0;

        await xfvm.closeConnection();

        // Reset shutdown flag and cleanup status
        setters.setIsShuttingDown(false);
        setters.setHasCleanedUpAfterDisconnect(true);

        console.log('=== CLEANUP COMPLETE - READY FOR RECONNECTION ===');

    } catch (error) {
        console.error("Error during cleanup after server disconnection:", error);
    }
}

/**
 * Terminate all workers immediately
 */
async function terminateAllWorkersImmediate() {
    console.log('Terminating all workers immediately...');

    const workers = getters.getWorkers();

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

/**
 * Check if a process is running
 */
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

/**
 * Kill all spawned processes
 */
async function killAllSpawnedProcesses() {
    console.log(`Killing ${helper.activeProcesses.size} active spawned processes IMMEDIATELY...`);

    const killPromises = [];

    for (const [pid, info] of helper.activeProcesses.entries()) {
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

    helper.activeProcesses.clear();
    console.log('Process cleanup completed');
}

/**
 * Force cleanup all temporary files
 */
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

/**
 * Get the current shutdown state
 */
function getShutdownState() {
    return getters.getIsShuttingDown();
}

module.exports = {
    initializeHandler,
    handleServerDisconnection,
    getShutdownState
};