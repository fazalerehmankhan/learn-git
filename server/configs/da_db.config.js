require('dotenv').config();

const da_db = {
  host: process.env.DA_DB_Host || "localhost",
  user: process.env.DA_DB_User || "monitor",
  password: process.env.DA_DB_Password || "Dell@123",
  database: process.env.DA_DB || "data_analytics",
  waitForConnections: true,
  connectionLimit: process.env.DA_DB_ConnectionLimit || 500,
  queueLimit: 0,
};
module.exports = { da_db };

