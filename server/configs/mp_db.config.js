require('dotenv').config();

const mp_db = {
  host: process.env.MP_DB_Host || "localhost",
  user: process.env.MP_DB_User || "monitor",
  password: process.env.MP_DB_Password || "Dell@123",
  database: process.env.MP_DB || "data_analytics",
  waitForConnections: true,
  connectionLimit: process.env.MP_DB_ConnectionLimit || 500,
  queueLimit: 0,
};
module.exports = { mp_db };

