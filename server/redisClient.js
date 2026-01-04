// redisClient.js
const redis = require('redis');
require('dotenv').config();

const redisClient = redis.createClient({
    socket: {
        host: process.env.REDIS_IP || 'localhost', // Update this to match your Redis server configuration
        port: process.env.REDIS_PORT || 6379 // Update this to match your Redis server configuration
    }
});

redisClient.on('error', (err) => {
    console.log('Redis Client Error', err);
});

redisClient.connect()
    .then(() => {
        console.log(`Redis client ${process.env.REDIS_IP}:${process.env.REDIS_PORT}  connected`);
    })
    .catch((err) => {
        console.error('Redis client connection error:', err);
    });
module.exports = redisClient;

