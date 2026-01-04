// This file contains all the functions with Database Queries Related to Authentication Module.
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const path = require("path");
const db = require(path.join(__dirname, "./../utils/db.connection"));
require("dotenv").config({ path: __dirname + "/./../../.env" });

/**
 * @namespace Auth
 */

/**
 * This function is used to register a new user
 * @memberOf Auth
 * @param {string} name - Name of the new user
 * @param {string} username - Username of the new user. Must be unique.
 * @param {string} password - Password of the new user.
 * @returns {object} - Containing Either the refresh and access token or error message in case of error.
 */
async function registerUser(name, username, password) {
  if (!name) return { stderr: "Please provide a name.", status: 400 };
  if (!username) return { stderr: "Please provide a username.", status: 400 };
  if (!password) return { stderr: "Please provide a password.", status: 400 };

  const hashedPassword = bcrypt.hashSync(password, 8);
  try {
    const insertUser = await db.query(
      `Insert into api_users (name,username,password) values (?,?,?)`,
      [`${name}`, `${username}`, `${hashedPassword}`]
    );
    const { accessToken, refreshToken } = getJwtToken(
      insertUser.insertId,
      username
    );
    return {
      auth: true,
      accessToken: accessToken,
      refreshToken: refreshToken,
    };
  } catch (err) {
    if (err.code === "ER_DUP_ENTRY" && err.errno == 1062) {
      return {
        stderr:
          "Username Already In Use. Please try again with a different username.",
        status: 401,
      };
    }
    console.error(err);
    return { stderr: "There was a problem registering the user.", status: 500 };
  }
}

/**
 * This function is used internally to verify that the token actually belongs to a user registered in the database.
 * @memberOf Auth
 * @param {string} username - Username of the user
 * @returns - Details of the user or error message if there was an error.
 */
async function getUser(username) {
  try {
    const verifyUser = await db.query(
      "SELECT id,username,password FROM api_users WHERE username = ?",
      [username]
    );
    // if (!verifyUser[0]) return { stderr: "No User Found", status: 404 };
    if (!verifyUser[0])
      return {
        auth: false,
        token: null,
        stderr: "Invalid Username or Password",
        status: 401,
      };
    return { ...verifyUser[0] };
  } catch (err) {
    console.error(err);
    return { stderr: "There was a problem finding the user.", status: 500 };
  }
}

/**
 * This function is used to login a new user
 * @memberOf Auth
 * @param {string} token - Name of the new user
 * @param {string} username - Username of the new user. Must be unique.
 * @param {string} password - Password of the new user.
 * @returns {object} - Containing Either the refresh and access token or error message in case of error.
 */
async function loginUser(token, username, password) {
  if (!token) {
    if (!username) return { stderr: "Please provide a username.", status: 400 };
    if (!password) return { stderr: "Please provide a password.", status: 400 };
  }
  if (token) {
    try {
      const decoded = jwt.verify(token, process.env.RefreshSecret);
      username = decoded.username;
    } catch (err) {
      return {
        auth: false,
        stderr: "Failed to authenticate Refresh token.",
        err:
          err.name == "SyntaxError"
            ? { name: "JsonWebTokenError", message: "invalid token" }
            : err,
        status: 401,
      };
    }
  }
  try {
    const validateUser = await getUser(username);
    // console.log(!validateUser);
    if (!validateUser.stderr) {
      if (!token) {
        const passwordIsValid = bcrypt.compareSync(
          password,
          validateUser.password
        );
        if (!passwordIsValid)
          return {
            auth: false,
            token: null,
            stderr: "Invalid Username or Password",
            status: 401,
          };
      }
      const { accessToken, refreshToken } = getJwtToken(
        validateUser.id,
        validateUser.username
      );
      return {
        auth: true,
        accessToken: accessToken,
        refreshToken: refreshToken,
      };
    } else {
      return validateUser;
    }
  } catch (err) {
    console.error(err);
    return { stderr: "Error on the server while logging in User", status: 500 };
  }
}

/**
 * This function is internally used to get JWT Tokens. It is called by register and login functions
 * @memberOf Auth
 * @param {string} id - Id of the User. Extracted from DB or JWT token provided
 * @param {string} username - Username of the User. Extracted from DB or JWT token provided.
 * @returns {object} - JWT Token object
 */
function getJwtToken(id, username) {
  const accessToken = jwt.sign(
    { id: id, username: username },
    process.env.AccessSecret,
    {
      expiresIn: 14 * 24 * 60 * 60, // expires in 14 days
    }
  );
  const refreshToken = jwt.sign(
    { id: id, username: username },
    process.env.RefreshSecret,
    {
      expiresIn: 30 * 24 * 60 * 60, // expires in 30 days
    }
  );
  return { accessToken: accessToken, refreshToken: refreshToken };
}

/**
 * This function is used to changePassword of an exisiting user.
 * @memberOf Auth
 * @param {string} password - Existing password of the user whose token is provided.
 * @param {string} newPassword - New Password of the user.
 * @param {string} username - Username of the user. Extracted from the JWT token, and cross verified from DB by the getUser function.
 * @returns {object} - Object containing a message the password update was successfull or error message.
 */
async function changePassword(password, newPassword, username) {
  if (!password)
    return { stderr: "Please provide your existing password.", status: 400 };
  if (!newPassword)
    return { stderr: "Please provide new password.", status: 400 };
  if (password === newPassword)
    return {
      stderr: "New password cannot be same as old password.",
      status: 400,
    };
  if (!username) return { stderr: "Missing access token", status: 401 };

  try {
    const validateUser = await getUser(username);
    if (!validateUser.stderr) {
      const passwordIsValid = bcrypt.compareSync(
        password,
        validateUser.password
      );
      if (!passwordIsValid)
        return {
          auth: false,
          token: null,
          stderr: "Invalid old password provided",
          status: 401,
        };
      const hashedPassword = bcrypt.hashSync(newPassword, 8);
      try {
        await db.query("Update api_users SET password = ? where username = ?", [
          hashedPassword,
          username,
        ]);
      } catch (err) {
        console.error(err);
        return {
          auth: false,
          stderr: "Password update error from DB",
          err: err,
          status: 401,
        };
      }
      return {
        msg: "User Password updated successfully",
      };
    } else {
      return validateUser;
    }
  } catch (err) {
    console.error(err);
    return {
      stderr: "Error on the server while changing Password.",
      status: 500,
    };
  }
}
module.exports = {
  registerUser,
  getUser,
  loginUser,
  changePassword,
};
