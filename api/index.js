#!/usr/bin/env node
const express = require("express");
const app = express();
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const morgan = require("morgan");
const moment = require("moment");
const cluster = require('cluster');
require('./src/utils/logging');

const port = process.env.Port || 5001;
const ip = process.env.ServerIP || "localhost";
const cpuCores = process.env.ApiWorkers || 15;

const pcapRouter = require(path.join(__dirname, "./src/routes/routes"));
const { createUserTables } = require(path.join(__dirname, "./src/utils/db.createTables"));
const authRouter = require(path.join(__dirname, "./src/auth/controller.auth"));
const logDirectory = path.join(__dirname, "./logs");
if (!fs.existsSync(logDirectory)) {
  fs.mkdirSync(logDirectory);
}
const accessLogStream = fs.createWriteStream(path.join(logDirectory, 'access.log'), { flags: 'a' });
try {
  if (cluster.isPrimary) {
    createUserTables();
    // Fork worker processes
    for (let i = 0; i < cpuCores; i++) {
      cluster.fork();
    }
  } else {
    app.use(express.json());
    app.use(
      express.urlencoded({
        extended: true,
      })
    );

    // Creating a new token to extract username, which is added to req object by VerifyToken auth method
    morgan.token("username", function (req) {
      return req.username;
    });

    // New token for localtime zone
    morgan.token("date", () => {
      return moment().format("DD/MMM/YYYY:HH:mm:ss ZZ");
    });

    // Standard Apache combined log output, with one additional token containing Username
    app.use(
      morgan(
        ':remote-addr - :remote-user [:date] ":username" ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent"',
        { stream: accessLogStream }
      )
    );

    app.get("/", (req, res) => {
      res.json({ message: "Welcome to Mass PCAP API !!!" });
    });

    app.use("/", pcapRouter);
    app.use("/auth", authRouter);
    app.use((req, res, next) => {
      res.status(404).json({ stderr: 'Endpoint not found', success: false });
    });
    /* Error handler middleware */
    app.use((err, req, res, next) => {
      const statusCode = err.statusCode || 500;
      console.error(err.message, err.stack);
      res.status(statusCode).json({ message: err.message });
      return;
    });
    app.listen(port, async () => {
      console.log(`Mass PCAP API - Worker ${cluster.worker.id} listening at ${ip}:${port}`);
    });
  }
} catch (err) {
  console.log(err);
}
module.exports = { app }
