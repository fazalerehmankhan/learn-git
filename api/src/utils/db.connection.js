//  Connect to DB and run queries on it.
const mysql = require("mysql2/promise");
const path = require("path");
const { db } = require(path.join(__dirname, "../configs/db.config"));
const conn = mysql.createPool(db);

async function query(sql, params) {
  const [results] = await conn.execute(sql, params);
  // conn.close();
  return results;
}

module.exports = {
  query,
};
