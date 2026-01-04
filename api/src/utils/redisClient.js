// redisClient.js
const redis = require('redis');
require('dotenv').config();

const redisClient = redis.createClient({
  socket: {
    host: process.env.REDIS_IP || 'localhost', // Update this to match your Redis server configuration
    port: process.env.REDIS_PORT || 6379, // Update this to match your Redis server configuration
    reconnectStrategy: false
  }
});
const closeConnection = async () => {
  redisClient.quit()
    .then(() => console.log('Redis client connection closed'))
    .catch(err => console.error('Error closing Redis client connection:', err));
};
const connect = async () => {
  try {
    await redisClient.connect();
    console.log(`Redis client connected to ${process.env.REDIS_IP}:${process.env.REDIS_PORT}`);
    return redisClient;
  } catch (err) {
    console.error('Error connecting to Redis:', err);
    throw new Error('Failed to connect to Redis');
  }
};
redisClient.on('error', (err) => {
  console.error('Redis Client Error', err);
});


module.exports = { redisClient, closeConnection, connect };