// Auth Module endpoints for User CRUD operations
const express = require("express");
const router = express.Router();
const bodyParser = require("body-parser");
const path = require("path");
require("dotenv").config({ path: __dirname + "/./../../.env" });
const verifyToken = require(path.join(__dirname, "./verifyToken.auth"));
const { registerUser, loginUser, changePassword } = require(path.join(
  __dirname,
  "./../services/auth"
));

router.use(bodyParser.urlencoded({ extended: false }));
router.use(bodyParser.json());

router.get("/", (req, res) => {
  res.json({ msg: "Welcome to Mass PCAPs API - Authentication Module !!!" });
});

router.post("/register", verifyToken, async function (req, res) {
  const result = await registerUser(
    req.body.name,
    req.body.username,
    req.body.password
  );
  if (result.status) res.status(result.status).json({ stderr: result.stderr });
  else {
    res.json(result);
  }
});

router.post("/login", async (req, res) => {
  const result = await loginUser(
    req.headers["x-access-token"] || undefined,
    req.body.username || undefined,
    req.body.password || undefined
  );
  if (result.status) {
    // Extracting status from Json Object and leaving the remaining fields as is.
    const { status, ...userResponse } = result;
    res.status(status).json(userResponse);
  } else {
    res.json(result);
  }
});

router.post("/changePassword", verifyToken, async (req, res) => {
  const result = await changePassword(
    req.body.password,
    req.body.newpassword,
    req.username
  );
  if (result.status) {
    // Extracting status from Json Object and leaving the remaining fields as is.
    const { status, ...userResponse } = result;
    res.status(status).json(userResponse);
  } else {
    res.json(result);
  }
});
module.exports = router;
