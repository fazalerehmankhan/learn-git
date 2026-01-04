#!/usr/bin/env node
const cluster = require('cluster');
const { manageCdrQueue, clearClientsFromRedis } = require('./fetchData.js')
require('dotenv').config();
const { initializeDatabase, deletePartitions, addPartitions } = require('./utils/mp_db.initializer.js');


const numProcs = process.env.NUMBER_OF_PROCESSES;
const { startServer, clients } = require('./socket_prog/server.js');
require('./utils/logging.js');

async function init() {
	try {
		if (cluster.isMaster) {
			//creates a new MT DB if it doesn't exist already
			await initializeDatabase();
			//populates CDR Queue
			manageCdrQueue();
			await clearClientsFromRedis();
			console.log(`Master ${process.pid} is running`);

			// Fork worker processes
			for (let i = 0; i < numProcs; i++) {
				cluster.fork();
			}

			// Listen for worker exit events
			cluster.on('exit', (worker, code, signal) => {
				console.log(`Worker ${worker.process.pid} died`);
			});
			// deleting the old partitions from database after every 24 hours
			const interval = 24 * 60 * 60 * 1000;// 24 hours in milliseconds: 24 * 60 * 60 * 1000
			setInterval(deletePartitions, interval);
			setInterval(addPartitions, interval);

			// setInterval(manageCdrQueue, 5000);
		} else {
			console.log(`Worker ${process.pid} started`);

			// Start the server for socket connection
			startServer();

			// Assign CDRs to connected clients at regular intervals

		}
	} catch (error) {
		console.error('Error during initialization:', error);
		process.exit(1); // Exit the process if initialization fails
	}
}

init();
