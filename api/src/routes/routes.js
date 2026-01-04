const express = require("express");
const router = express.Router();
const path = require("path");
const pdrController = require(path.join(__dirname, "../controllers/pdr.controller"));
const verifyToken = require(path.join(__dirname, "../auth/verifyToken.auth"));

/* GET PCAP Status by Mass-ID. */
router.get("/getPcapStatus", verifyToken, pdrController.getPcapStatus);

/* GET PCAP Status by CDR-ID. */
router.get("/getPcapStatusByCDRId", verifyToken, pdrController.getPcapStatusByCDRId);

/* GET PCAPs Status by Offset and Size. */
router.get("/getBatchPCAPsStatus", verifyToken, pdrController.getBatchPCAPsStatus);

/* GET PCAP by Mass-ID. */
router.get("/getPcap", verifyToken, pdrController.getPcap);

/* GET PCAP by Mass-ID. */
router.get("/getPcapBatchInfo", verifyToken, pdrController.getPcapBatchInfo);

/* GET PCAP by Mass-ID. */
router.post("/cdrsReprocess", verifyToken, pdrController.cdrsReprocess);



module.exports = router;
