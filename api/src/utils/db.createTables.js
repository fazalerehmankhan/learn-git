const path = require("path");
require("dotenv").config({ path: __dirname + "/./../../.env" });
const db = require(path.join(__dirname, "./db.connection"));

const createUserTables = async () => {
	// Check if the table exists
	try {
		await db.query(
			`CREATE TABLE IF NOT EXISTS \`api_users\` (\`id\` INT auto_increment NOT NULL, \`name\` VARCHAR(256) NOT NULL, \`username\` VARCHAR(256) NOT NULL, \`password\` VARCHAR(256) NOT NULL, unique(username), KEY \`id\` (\`id\`) USING BTREE, PRIMARY KEY (\`id\`)) ENGINE=InnoDB;`
		)
		console.log("API Users table created successfully");
		await db.query(
			`INSERT IGNORE INTO api_users (name, username, password) VALUES ("admin", "admin", "$2a$08$8pHrpl59tpapII61Tlug4OlU7FOTxwJh1NlyAzFJy0oYzkzTn1BcW");`
		);
		console.log("Admin user created successfully");

	} catch (error) {
		console.error(error);
	}

}
module.exports = { createUserTables }