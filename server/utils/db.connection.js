const mysql = require('mysql2/promise');
const path = require('path');
const { da_db } = require(path.join(__dirname, "../configs/da_db.config"));
const { mp_db } = require(path.join(__dirname, "../configs/mp_db.config"));

// Create a MySQL connection pool
const da_pool = mysql.createPool(da_db);
const mp_pool = mysql.createPool(mp_db);

/**
 * Executes a query on the DA DB.
 * @param {string} sql - The SQL query to execute.
 * @param {Array} [params] - Optional parameters for the query.
 * @returns {Promise<Object[]>} - The results of the query.
 */
async function da_query(sql, params = []) {
  const conn = await da_pool.getConnection();
  try {
    const [results] = await conn.query(sql, params);
    return results;
  } catch (error) {
    console.error('DA Database query error:', error);
    throw error;
  }
  finally {
    conn.release(); // ✅ Always release the connection back to the pool
  }
}

/**
 * Executes a query on the Mass PCAP DB.
 * @param {string} sql - The SQL query to execute.
 * @param {Array} [params] - Optional parameters for the query.
 * @returns {Promise<Object[]>} - The results of the query.
 */
async function mp_query(sql, params = []) {
  const conn = await mp_pool.getConnection();
  try {
    const [results] = await conn.query(sql, params); // ✅ Simpler, safer
    return results;
  } catch (error) {
    console.error('MP Database query error:', error);
    throw error;
  } finally {
    conn.release(); // Always release the connection
  }
}
/**
 * Closes the database connection pool for DA DB.
 * @returns {Promise<void>}
 */
async function da_closePool() {
  try {
    await da_pool.end();
  } catch (error) {
    console.error('Error closing DA database connection pool:', error);
    throw error;
  }
}

/**
 * Closes the database connection pool for Mass PCAP DB.
 * @returns {Promise<void>}
 */
async function mp_closePool() {
  try {
    await mp_pool.end();
  } catch (error) {
    console.error('Error closing MT database connection pool:', error);
    throw error;
  }
}
module.exports = {
  da_query,
  da_closePool,
  mp_query,
  mp_closePool
};