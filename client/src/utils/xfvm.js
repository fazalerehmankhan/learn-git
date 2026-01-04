const net = require('net');
const fs = require('fs').promises;
require('dotenv').config();
const xfvmIP = process.env.XFVM_IP || 'localhost';
const xfvmPort = process.env.XFVM_PORT || 8080;
const EventEmitter = require("events");
const { isMainThread, workerData } = require('worker_threads');
const eventEmitter = new EventEmitter();
let connectionState = 'disconnected'; // track connection state

/**
 * Starts the TCP server to handle incoming client connections.
*/
let client;
const process_type = isMainThread ? "Main Process" : `Worker-${workerData.id}`;
function connectToXFVM() {
  client = new net.Socket({ highWaterMark: 1024 * 1024 * 16 });

  client.connect(xfvmPort, xfvmIP, () => {
    console.log(`${process_type} Connected to the xfvm TCP server`);
    connectionState = 'connected';
  });
  let buffer = "";
  client.on('data', async (data) => {
    buffer += data.toString();
    let boundary;

    // Split the data using the newline delimiter
    while ((boundary = buffer.indexOf('\n')) !== -1) {
      const message = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 1);
      if (message.trim()) {
        try {
          // Emit the response event
          eventEmitter.emit("response", message);
        } catch (err) {
          console.error("FAILED TO EMIT MESSAGE: ", err);
        }

      }
    }
  });
  client.on('error', (err) => {
    console.error('xfvm TCP client error:', err);
    // setTimeout(connectToServer, 10000);
    connectionState = 'disconnected';

  });

  client.on('close', () => {
    console.warn(`xfvm Connection closed from ${process_type}`);
    // Attempt to reconnect after 10 seconds
    connectionState = 'disconnected';
    setTimeout(connectToXFVM, 10000);
  });
}

function clearQueue() {

  if (client && client.writable) {
    try {
      client.write('CLEAR_QUEUE\n');
      console.log('Sent CLEAR_QUEUE command to XFVM');
      return true;
    } catch (error) {
      console.error('Failed to send CLEAR_QUEUE:', error);
      return false;
    }
  } else {
    console.warn('Cannot send CLEAR_QUEUE - client not connected');
    return false;
  }
}

function closeConnection() {
  if (client) {
    try {
      client.destroy();
      console.log(`Forcibly closed XFVM connection by ${process_type}`);
    } catch (error) {
      console.error(`Error closing XFVM connection by ${process_type}:`, error);
    }
    client = null;
    connectionState = 'disconnected';
  }
}

function reconnectToXFVM() {
  console.log(`${process_type} Reconnecting to XFVM...`);
  closeConnection();

  // Wait a moment before reconnecting
  setTimeout(() => {
    connectToXFVM();
  }, 2000);
}



function onResponse(callback) {
  eventEmitter.on("response", callback);
}
async function assignTaskToXFVM(data) {
  // Track when the worker goes idle
  try {
    // console.log('Assigning task to xfvm:', data);
    client.write(data + '\n', (err) => {
      if (err) {
        // If sending fails, log an error and do not clear the queue
        console.error('Failed to send task to xfvm');
      } else {
        // If sending is successful, log success and remove the sent tasks
        // console.log('Task assigned to xfvm');
      }
    });
  } catch (error) {
    console.error('Error assigning task to xfvm:', error);
  }
}

module.exports = { connectToXFVM, assignTaskToXFVM, onResponse, reconnectToXFVM, clearQueue, closeConnection };
