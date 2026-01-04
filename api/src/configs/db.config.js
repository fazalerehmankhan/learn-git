// MP DB Details, these are read from .env file. Fallback values are also provided incase no values are provided in .env file
const db = {
  host: process.env.DBHost || "localhost",
  user: process.env.DBUser || "monitor",
  password: process.env.DBPassword || "Dell@123",
  database: process.env.DB || "data_analytics",
  waitForConnections: true,
  connectionLimit: process.env.DBConnectionLimit || 500,
  queueLimit: 0,
};
module.exports = { db };
