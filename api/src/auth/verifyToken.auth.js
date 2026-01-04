// Middleware Function which contains the code to verify acccessToken and lock endpoints
var jwt = require("jsonwebtoken");
const path = require("path");
require("dotenv").config({ path: __dirname + "/./../../.env" });
const { getUser } = require(path.join(__dirname, "./../services/auth"));

const verifyToken = (req, res, next) => {
  if (process.env.Disable_API_Authentication === "True") {
    next();
    return;
  }
  const token = req.headers["x-access-token"];
  if (!token)
    return res.status(401).send({ auth: false, stderr: "No token provided." });

  jwt.verify(token, process.env.AccessSecret, async (err, decoded) => {
    if (err)
      return res.status(401).send({
        auth: false,
        stderr: "Failed to authenticate token.",
        err:
          err.name == "SyntaxError"
            ? { name: "JsonWebTokenError", message: "invalid token" }
            : err,
      });
    req.username = decoded.username;
    const checkUserFromDB = await getUser(req.username);
    if (checkUserFromDB.status)
      return res
        .status(checkUserFromDB.status)
        .send({ stderr: checkUserFromDB.stderr });
    next();
  });
};
module.exports = verifyToken;
