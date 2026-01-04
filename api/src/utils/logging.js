const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const level = process.env.LOG_LEVEL || 'info';

// Define your log levels
const logLevels = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3
};

// Define the colors for the log levels
const logColors = {
  error: 'red',
  warn: 'yellow',
  info: 'green',
  debug: 'blue'
};

// Add colors to the console
winston.addColors(logColors);

// Create logger with file and console transports
const logger = winston.createLogger({
  levels: logLevels,
  transports: [
    new DailyRotateFile({
      filename: '/var/log/MP-API-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      maxSize: '20m',
      maxFiles: '7d',
      utc: false,
      format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ level, message, timestamp }) => {
          return `${timestamp} [${level}]: ${message}`;
        })
      ),
      level: level  // Change to 'info' to disable debug logs
    }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      ),
      level: level  // Change to 'info' to disable debug logs
    })
  ]
});

// Helper function to format log arguments
const formatLogArguments = (args) => {
  return args.map(arg => {
    if (Array.isArray(arg)) {
      // If argument is an array, stringify each element
      return JSON.stringify(arg, null, 2);  // Pretty print arrays
    } else if (typeof arg === 'object') {
      // If argument is an object, stringify it
      return JSON.stringify(arg, null, 2);  // Pretty print objects
    }
    return arg;
  }).join(' ');
};

// Override console methods to use Winston
console.error = (...args) => {
  logger.error(formatLogArguments(args));
};

console.warn = (...args) => {
  logger.warn(formatLogArguments(args));
};

console.log = (...args) => {
  logger.info(formatLogArguments(args));
};

console.debug = (...args) => {
  logger.debug(formatLogArguments(args));
};
