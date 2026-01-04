const net = require('net');
const { getTimestamp } = require('./utils');
require('dotenv').config({ path: __dirname + '/../.env' });
const redisClient = require('../redisClient'); // Ensure redisClient is properly set up
const { writeToMPDB } = require('../utils/mp_db_insertData');
const { sendDataToClient, removeCDRsFromRedis } = require('../utils/assign_client_workload');
const { acquireLockWithRetry } = require('../utils/helper');
const clients = [];

/**
 * Updates the client's connection status.
 * @param {string} clientId - The ID of the client.
 * @param {string} status - The new status of the client.
 */
function updateClientStatus(clientId, status) {
  const client = clients.find(client => client.id === clientId);
  if (client) {
    client.status = status;
  }
}
// Function to remove a client from the clients array
function removeClient(clientId) {
  const index = clients.findIndex(client => client.id === clientId);
  if (index !== -1) {
    clients.splice(index, 1);
  }
}
// Function to handle cleanup of a client connection
async function cleanupClient(socket, clientId, clientAddress, clientInfo, status = 'disconnected') {
  console.log(`${getTimestamp()} - Cleaning up client ${clientId || "unknown"} with status '${status}'`);

  if (clientInfo.pingInterval) {
    clearInterval(clientInfo.pingInterval);
  }

  if (clientId) {
    await redisClient.lRem('clients', 0, clientId);

    const lockKey = `restore_lock:${clientId}`;
    const gotLock = await acquireLockWithRetry(lockKey);

    if (gotLock) {
      try {
        await removeCDRsFromRedis(clientId);
      } finally {
        await redisClient.del(lockKey);
      }
    } else {
      console.warn(`Lock not acquired for ${clientId}, skipping removeCDRsFromRedis`);
    }
    // Remove client from the clients array
    removeClient(clientId);
    if (status === 'disconnected') {
      console.warn(`Connection of client with id ${clientId} and address ${clientAddress} ended`);
      console.debug(`${getTimestamp()} - CONNECTION CLOSED FROM: ${clientAddress}`);
    }
  }

  const index = clients.findIndex(client => client.socket === socket);
  if (index !== -1) {
    clients.splice(index, 1);
  }
}
/**
 * Starts the TCP server to handle incoming client connections.
 */

function startServer() {
  const server = net.createServer((socket) => {
    const clientAddress = `${socket.remoteAddress}:${socket.remotePort}`;
    const timestamp = getTimestamp();

    // Create client info object
    const clientInfo = {
      id: null,  // Initially unknown
      ip: socket.remoteAddress,
      port: socket.remotePort,
      status: 'connected',
      socket: socket,
      lastSeen: Date.now(),
      pingInterval: null
    };
    socket.setKeepAlive(true, 5000);
    console.log(`${timestamp} - NEW CONNECTION FROM: ${clientAddress}`);

    // Variable to store the client ID
    let clientId = null;
    let buffer = "";
    // Start ping mechanism
    clientInfo.pingInterval = setInterval(() => {
      const now = Date.now();

      if (now - clientInfo.lastSeen > 30000) { // 30s timeout
        console.warn(`${getTimestamp()} - Client ${clientId || "unknown"} timed out due to no response`);
        socket.destroy(); // Triggers error or end
        // socket.end();
      } else {
        try {
          socket.write('ping\n');
        } catch (err) {
          console.error(`${getTimestamp()} - Failed to send ping to client ${clientId}:`, err);
        }
      }
    }, 10000); // Ping every 10 seconds

    // Handle data received from the client
    socket.on('data', async (message) => {
      clientInfo.lastSeen = Date.now();
      buffer += message.toString();
      let boundary;

      // Split the data using the newline delimiter
      while ((boundary = buffer.indexOf('\n')) !== -1) {
        const raw = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 1);
        const message = raw.trim();
        if (!message) continue;

        if (message === 'pong') {
          continue; // Client responded to ping
        }
        try {
          const receivedCDRArray = JSON.parse(message);

          if (receivedCDRArray.clientId) {
            clientId = receivedCDRArray.clientId;
            clientInfo.id = clientId;

            // Check if a client with the same clientId already exists
            const rClients = await redisClient.lRange('clients', 0, -1);
            console.log('clients in redis: ', rClients);
            const existingClient = rClients.find(client => client === clientId);

            if (existingClient) {
              // Reject the new connection or handle as per your requirement
              console.error(`${getTimestamp()} - CLIENT ${clientId} ALREADY CONNECTED, REJECTING NEW CONNECTION`);
              socket.write('Connection rejected: client with the same ID is already connected.\n');
              clientId = null;
              socket.end();
              return; // Exit the function to prevent further execution
            }
            await redisClient.rPush('clients', clientId)
            clients.push(clientInfo);
            console.log(`${getTimestamp()} - NEW CLIENT ${clientId} CONNECTED`);
            await sendDataToClient(clientInfo, parseInt(process.env.Client_CDR_QUEUE_THRESHOLD, 10));
          } else if (receivedCDRArray.type === 'ack' && receivedCDRArray.context === 'cdr_send') {

          }
          else {
            // logic for handling CDRs
            socket.write(JSON.stringify({ type: 'ack', context: 'cdr_recv' }) + '\n');
            console.debug(`Processing CDR with id `, receivedCDRArray);
            outQueuePromises = receivedCDRArray.map(async (cdr) => {
              console.log(`Recieved data from Client id: ${cdr.client_id} with calldate: ${cdr.minute}`);
              const exists = await redisClient.hExists(`client:${cdr.client_id}`, `${cdr.minute}:${cdr.id_sensor}`);
              if (exists) {
                await redisClient.hDel(`client:${cdr.client_id}`, `${cdr.minute}:${cdr.id_sensor}`);
                const serializedCdr = JSON.stringify(cdr);
                await redisClient.rPush('outQueue', serializedCdr);
                sendDataToClient(clientInfo, 1);
              }
            });
            await Promise.all(outQueuePromises);
            console.log(`Deleted the assigned CDRs to client ${clientId}`);
            const outLen = await redisClient.lLen('outQueue');
            if (outLen >= process.env.OUT_QUEUE_THRESHOLD) {
              await writeToMPDB(outLen);
            }
          }
        } catch (error) {
          console.log(message.toString());
          console.error(error);
          console.error('Failed to parse JSON:', error);
        }
      }
    });

    // Handle the end of the connection from the client
    socket.on('end', async () => {
      console.log(`${getTimestamp()} - Client ${clientId || "unknown"} disconnected`);
      await cleanupClient(socket, clientId, clientAddress, clientInfo, 'disconnected');
    });
    socket.on('close', async (hadError) => {
      console.warn(`${getTimestamp()} - Socket closed for client ${clientId || "unknown"}, hadError: ${hadError}`);
      await cleanupClient(socket, clientId, clientAddress, clientInfo, 'error');
      // delete clients[clientId];
    });
    // Handle any errors that occur with the client connection
    socket.on('error', async (err) => {
      console.error(`${getTimestamp()} - Error with client ${clientId || "unknown"}:`, err.message);
      await cleanupClient(socket, clientId, clientAddress, clientInfo, 'error');
    });
  });

  // Start the server and listen for incoming connections
  const port = process.env.PORT || 8000;
  const ip = process.env.IP || "localhost";
  server.listen(port, ip, () => {
    const timestamp = getTimestamp();
    console.log(`${timestamp} - Server listening on ${ip}:${port}`);
  });
}

module.exports = { startServer, clients };
