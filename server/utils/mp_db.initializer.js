const mysql = require('mysql2/promise');
const { getFormattedDate } = require('./helper');
const retentionDays = process.env.RETENTION_DAYS || 30;
const { mp_db: dbConfig } = require("../configs/mp_db.config");
const adminConfig = { ...dbConfig, database: undefined }; // Remove database name for initial connection

const deletePartitionsProcedure = `
CREATE PROCEDURE drop_partitions_before_date(IN cutoff_date DATE) 
BEGIN 
    DECLARE done INT DEFAULT FALSE; 
    DECLARE part_name VARCHAR(64); 
    DECLARE cur CURSOR FOR  
        SELECT PARTITION_NAME  
        FROM INFORMATION_SCHEMA.PARTITIONS  
        WHERE TABLE_SCHEMA = '${dbConfig.database}' 
          AND TABLE_NAME = 'conversion_log' 
          AND PARTITION_NAME < CONCAT('p', DATE_FORMAT(cutoff_date, '%Y%m%d')); 
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE; 

    -- Open the cursor 
    OPEN cur; 
    read_loop: LOOP 
        FETCH cur INTO part_name; 
        IF done THEN 
            LEAVE read_loop; 
        END IF; 

        -- Prepare and execute the statement to drop the partition 
        SET @query = CONCAT('ALTER TABLE conversion_log DROP PARTITION ', part_name); 
        PREPARE stmt FROM @query; 
        EXECUTE stmt; 
        DEALLOCATE PREPARE stmt; 
    END LOOP; 
    CLOSE cur; 
END
`;

const addPartititionsProcedure = `
CREATE PROCEDURE manage_partitions()     
BEGIN    
    DECLARE today_date DATE;         
    DECLARE tomorrow_date DATE;     
    DECLARE day_after_tomorrow DATE;     
    DECLARE partition_today_name VARCHAR(20);         
    DECLARE partition_tomorrow_name VARCHAR(20);         
    DECLARE partition_today_exists INT DEFAULT 0; 
    DECLARE partition_tomorrow_exists INT DEFAULT 0;        
    SET  today_date = CURDATE();         
    SET tomorrow_date = DATE_ADD(today_date, INTERVAL 1 DAY);  
    SET day_after_tomorrow = DATE_ADD(today_date,INTERVAL 2 DAY);        
    SET partition_today_name = DATE_FORMAT(today_date, 'p%Y%m%d');         
    SET partition_tomorrow_name = DATE_FORMAT(tomorrow_date, 'p%Y%m%d');    
    SELECT COUNT(*)     INTO partition_today_exists     FROM INFORMATION_SCHEMA.PARTITIONS     WHERE TABLE_SCHEMA = '${dbConfig.database}'       AND TABLE_NAME = 'conversion_log'       AND PARTITION_NAME = partition_today_name;     
    Select concat("the value of partition_today_exist :", partition_today_exists) as partition_exist_today; 
    SELECT COUNT(*)     INTO partition_tomorrow_exists      FROM INFORMATION_SCHEMA.PARTITIONS     WHERE TABLE_SCHEMA = '${dbConfig.database}'       AND TABLE_NAME = 'conversion_log'   AND PARTITION_NAME   = partition_tomorrow_name;                  
    Select concat("the value of partition_tomorrow_exist :", partition_tomorrow_exists) as partition_exist_tomorrow; 
    IF partition_today_exists = 0 THEN            
        SET @create_partition_sql = CONCAT('ALTER TABLE conversion_log ADD PARTITION (PARTITION ', partition_today_name, ' VALUES LESS THAN (', '\"', tomorrow_date, '\"));');         
        PREPARE stmt FROM @create_partition_sql;            
        select @create_partition_sql;   
        EXECUTE stmt;             
        DEALLOCATE PREPARE stmt;         
    END IF;               
    IF partition_tomorrow_exists = 0 THEN    
        SET @create_partition_sql = CONCAT('ALTER TABLE conversion_log ADD PARTITION (PARTITION ', partition_tomorrow_name, ' VALUES LESS THAN (', '\"',day_after_tomorrow, '\"));'      );             
        PREPARE stmt FROM @create_partition_sql;    
        select @create_partition_sql;          
        EXECUTE stmt;             
        DEALLOCATE PREPARE stmt;         
    END IF;
END;
`;

const createSchemaProcedure = `
CREATE PROCEDURE create_mp_schema() 
BEGIN     
    DECLARE today DATE;     
    DECLARE tomorrow DATE;     
    DECLARE day_after_tomorrow DATE;     
    DECLARE partition_name_today VARCHAR(20);     
    DECLARE partition_name_tomorrow VARCHAR(20);     
    DECLARE partition_name_day_after_tomorrow VARCHAR(20);     
    DECLARE create_table_sql TEXT;     
    DECLARE stmt VARCHAR(255);     
    DECLARE alter_sql TEXT;
    DECLARE column_exists INT DEFAULT 0;      

    SET today = CURDATE();     
    SET tomorrow = DATE_ADD(today, INTERVAL 1 DAY);     
    SET day_after_tomorrow = DATE_ADD(today, INTERVAL 2 DAY);         
    SET partition_name_today = DATE_FORMAT(today, 'p%Y%m%d');     
    SET partition_name_tomorrow = DATE_FORMAT(tomorrow, 'p%Y%m%d');     
    SET partition_name_day_after_tomorrow = DATE_FORMAT(day_after_tomorrow, 'p%Y%m%d');   

    -- Create conversion_log if not exists
    SET create_table_sql = CONCAT(
        'CREATE TABLE IF NOT EXISTS conversion_log (',
        'id BIGINT AUTO_INCREMENT, ',
        'cdr_id BIGINT NOT NULL, ',
        'call_date DATETIME NOT NULL, ',
        'call_end DATETIME NOT NULL, ',
        'caller VARCHAR(255) NOT NULL, ',
        'called VARCHAR(255) NOT NULL, ',
        'call_id VARCHAR(255) NOT NULL, ',
        'conversion_status TINYINT NOT NULL, ',
        'conversion_date DATETIME NOT NULL, ',
        'storage_path VARCHAR(255) DEFAULT NULL, ',
        'error_msg TEXT NOT NULL, ',
        'client_id VARCHAR(255) DEFAULT NULL, ',
        'PRIMARY KEY (id, cdr_id, conversion_date)) ',
        'PARTITION BY RANGE COLUMNS(conversion_date) (',
        'PARTITION ', partition_name_today, ' VALUES LESS THAN (\"', tomorrow, '\"), ',
        'PARTITION ', partition_name_tomorrow, ' VALUES LESS THAN (\"', day_after_tomorrow, '\"));'
    );        

    SET @stmt = create_table_sql;     
    PREPARE stmt FROM @stmt;     
    EXECUTE stmt;

    -- Check if batch_id column exists
    SELECT COUNT(*) INTO column_exists
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'conversion_log' AND COLUMN_NAME = 'batch_id' AND TABLE_SCHEMA = DATABASE();

    -- If not, add it
    IF column_exists = 0 THEN
        SET alter_sql = 'ALTER TABLE conversion_log ADD COLUMN batch_id BIGINT';
        SET @alter_stmt = alter_sql;
        PREPARE alter_stmt FROM @alter_stmt;
        EXECUTE alter_stmt;
        DEALLOCATE PREPARE alter_stmt;
    END IF;

    -- Create batch_tracker table if not exists
    SET @stmt = 'CREATE TABLE IF NOT EXISTS batch_tracker (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      batch_id BIGINT,
      call_date DATETIME,
      sensor_id VARCHAR(255),
      file_path VARCHAR(1000),
      mass_start_id BIGINT,
      mass_end_id BIGINT,
      conversion_date DATETIME
    )';
    PREPARE stmt FROM @stmt;
    EXECUTE stmt;
END;
`;

async function initializeDatabase() {
  try {

    const connection = await mysql.createConnection(adminConfig);
    const dbName = dbConfig.database;

    try {
      await connection.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
      console.log(`Database '${dbName}' is ready.`);
      const dbConnection = await mysql.createConnection(dbConfig);

      const dropSchemaQuery = `DROP PROCEDURE IF EXISTS create_mp_schema;`;
      await dbConnection.query(dropSchemaQuery);
      const dropAddPartitionQuery = `DROP PROCEDURE IF EXISTS manage_partitions;`;
      await dbConnection.query(dropAddPartitionQuery);
      const dropDeletePartitionQuery = `DROP PROCEDURE IF EXISTS drop_partitions_before_date;`;
      await dbConnection.query(dropDeletePartitionQuery);

      await dbConnection.query(createSchemaProcedure);
      console.log('Stored create_mp_schema procedure created or already exists.');
      await dbConnection.query(addPartititionsProcedure);
      console.log('Stored manage_partitions procedure created or already exists.');
      await dbConnection.query(deletePartitionsProcedure);
      console.log('Stored drop_partitions_before_date procedure created or already exists.');

      await dbConnection.query(`CALL create_mp_schema()`);
      console.log('Stored procedure create_mp_schema called successfully.');

      await dbConnection.query(`CALL manage_partitions()`);
      console.log('Stored procedure manage_partitions called successfully.');

      await dbConnection.query(`CALL drop_partitions_before_date('${getFormattedDate(retentionDays)}')`);
      console.log(`Stored procedure drop_partitions_before_date called with date ${getFormattedDate(retentionDays)} successfully.`);

      await dbConnection.end();
    } catch (error) {
      console.error(`Error creating database '${dbName}':`, error);
      throw error;
    } finally {
      await connection.end();
    }
  } catch (error) {
    console.error('Error initializing databases:', error);
    throw error;
  }
}
const deletePartitions = async () => {
  const dbConnection = await mysql.createConnection(dbConfig);
  try {
    await dbConnection.query(`CALL drop_partitions_before_date('${getFormattedDate(retentionDays)}')`);
    await dbConnection.query(`delete from batch_tracker where conversion_date < '${getFormattedDate(retentionDays)}'`);
    console.log(`Stored procedure drop_partitions_before_date called with date ${getFormattedDate(retentionDays)} successfully.`);
  } catch (error) {
    console.error('Error calling drop_partitions_before_date:', error);
    throw error;
  }
  finally {
    await dbConnection.end();
  }
}

const addPartitions = async () => {
  const dbConnection = await mysql.createConnection(dbConfig);
  try {
    await dbConnection.query(`CALL manage_partitions()`);
    console.log(`Stored procedure manage_partitions successfully.`);
  }
  catch (error) {
    console.error('Error calling manage_partitions:', error);
    throw error;
  }
  finally {
    await dbConnection.end();
  }
}
module.exports = { initializeDatabase, addPartitions, deletePartitions };
