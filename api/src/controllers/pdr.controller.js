const path = require("path");
const cdr = require(path.join(__dirname, "../services/pcapDetailRecord"));

/**
 * Handles the request to get the PCAP detail record status for a given massId.
 * 
 * This function performs the following steps:
 * 1. Validates the massId query parameter to ensure it is provided and is a valid integer.
 * 2. Fetches the PCAP detail record status using the provided massId.
 * 3. Returns the result as a JSON response with the appropriate status code.
 *
 * @param {Object} req
 * @param {Object} res 
 * @param {Function} next 
 */

async function getPcapStatus(req, res, next) {
  try {
    // Validate the massId query parameter
    const massId = req.query.massId;
    if (!massId) {
      return res.status(400).json({
        stderr: "Please provide massId.",
        success: "false"
      });
    }

    // Check if massId is NaN or if massId contains non-numeric characters
    if (isNaN(massId) || !/^\d+$/.test(massId) || massId <= 0) {
      return res.status(400).json({
        stderr: "Invalid input: 'massId' must be a positive integer.",
        success: false
      });
    }

    // Check for unsupported query parameters
    const invalidParams = Object.keys(req.query).filter(param => param !== 'massId');
    if (invalidParams.length) {
      return res.status(400).json({
        stderr: `Unsupported query parameter(s): '${invalidParams.join(', ')}'.`,
        success: false
      });
    }

    // Fetch the PCAP detail record status using the provided massId
    let result = await cdr.getPcapStatus(
      req.query.massId
    );
    if (result == null) {
      res.status(404).json({
        stderr: "No record found for the provided Mass ID.",
        success: "false"
      });
    }
    else {
      res.status(200).json({
        ...result,
        success: true
      });
    }
  } catch (err) {
    console.error(`Error while getting PCAP Detail Record `, err.message);
    next(err);
  }
}


/**
 * Handles the request to get the PCAP detail record status for a given massId.
 * 
 * This function performs the following steps:
 * 1. Validates the massId query parameter to ensure it is provided and is a valid integer.
 * 2. Fetches the PCAP detail record status using the provided massId.
 * 3. Returns the result as a JSON response with the appropriate status code.
 *
 * @param {Object} req
 * @param {Object} res 
 * @param {Function} next 
 */

async function getPcapStatusByCDRId(req, res, next) {
  try {
    // Validate the massId query parameter
    const cdrId = req.query.cdrId;
    if (!cdrId) {
      return res.status(400).json({
        stderr: "Please provide cdrId.",
        success: "false"
      });
    }

    // Check if massId is NaN or if massId contains non-numeric characters
    if (isNaN(cdrId) || !/^\d+$/.test(cdrId) || cdrId <= 0) {
      return res.status(400).json({
        stderr: "Invalid input: 'cdrId' must be a positive integer.",
        success: false
      });
    }

    // Check for unsupported query parameters
    const invalidParams = Object.keys(req.query).filter(param => param !== 'cdrId');
    if (invalidParams.length) {
      return res.status(400).json({
        stderr: `Unsupported query parameter(s): '${invalidParams.join(', ')}'.`,
        success: false
      });
    }

    // Fetch the PCAP detail record status using the provided massId
    let result = await cdr.getPcapStatusByCDRId(
      req.query.cdrId
    );
    if (result == null) {
      res.status(404).json({
        stderr: "No record found for the provided CDR ID.",
        success: "false"
      });
    }
    else {
      res.status(200).json({
        ...result,
        success: true
      });
    }
  } catch (err) {
    console.error(`Error while getting PCAP Detail Record `, err.message);
    next(err);
  }
}

async function getPcapBatchInfo(req, res, next) {
  try {
    // Validate the massId query parameter
    const batchId = req.query.batchId;
    if (!batchId) {
      return res.status(400).json({
        stderr: "Please provide batchId.",
        success: "false"
      });
    }

    // Check if batchId is NaN or if massId contains non-numeric characters
    if (isNaN(batchId) || !/^\d+$/.test(batchId) || batchId <= 0) {
      return res.status(400).json({
        stderr: "Invalid input: 'batchId' must be a positive integer.",
        success: false
      });
    }

    // Check for unsupported query parameters
    const invalidParams = Object.keys(req.query).filter(param => param !== 'batchId');
    if (invalidParams.length) {
      return res.status(400).json({
        stderr: `Unsupported query parameter(s): '${invalidParams.join(', ')}'.`,
        success: false
      });
    }

    // Fetch the PCAP detail record status using the provided massId
    let result = await cdr.getPcapBatchInfo(
      req.query.batchId
    );
    if (result == null) {
      res.status(404).json({
        stderr: "No record found for the provided Batch ID.",
        success: "false"
      });
    }
    else {
      res.status(200).json({
        ...result,
        success: true
      });
    }
  } catch (err) {
    console.error(`Error while getting Batch Record `, err.message);
    next(err);
  }
}

/**
 * Fetches the batch PCAP status based on the given offset and size.
 * 
 * - Validates the `offset` and `size` query parameters to ensure they are correctly provided.
 * - Fetches PCAP details using the offset and size parameters.
 * - Handles cases where:
 *   - No records are found (404 Not Found).
 *   - Fewer records are found than requested (206 Partial Content).
 *   - Records are successfully fetched (200 OK).
 * 
 * @param {Object} req 
 * @param {Object} res 
 * @param {Function} next 
 */

async function getBatchPCAPsStatus(req, res, next) {
  try {

    // Validate the offset and size query parameters
    const { offset, size } = req.query;

    // Check if offset and size are missing
    if (!offset && !size) {
      return res.status(400).json({
        stderr: "Missing required query parameters: 'offset and size'.",
        success: "false"
      });
    } else if (!offset) {
      return res.status(400).json({
        stderr: "Missing required query parameter: 'offset'.",
        success: "false"
      });
    } else if (!size) {
      return res.status(400).json({
        stderr: "Missing required query parameter: 'size'.",
        success: "false"
      });
    }

    // Check if offset and size are valid integers
    if (!/^\d+$/.test(offset) || !/^\d+$/.test(size)) {
      return res.status(400).json({
        stderr: "Invalid input: offset/size must be non-negative integers.",
        success: false
      });
    }

    // Check if parsed offset and size are valid
    if (isNaN(parseInt(offset, 10)) || parseInt(offset, 10) < 0 || isNaN(parseInt(size, 10)) || parseInt(size, 10) <= 0) {
      return res.status(400).json({
        stderr: "Invalid input: offset/size must be non-negative integers.",
        success: false
      });
    }

    // Check for unsupported query parameters
    const invalidParams = Object.keys(req.query).filter(param => param !== 'offset' && param !== 'size');
    if (invalidParams.length) {
      return res.status(400).json({
        stderr: `Unsupported query parameter(s): '${invalidParams.join(', ')}'.`,
        success: false
      });
    }

    // Fetch the PCAP detail record status using the provided offset and size
    let results = await cdr.getBatchPCAPsStatus(req.query.offset, req.query.size);
    console.debug(results)

    if (results.pcapInfo[0]) {
      if (results.status == 206) {
        res.status(206).json({
          pcapInfo: results.pcapInfo,
          success: true
        })
      }
      else {
        res.status(200).json({
          ...results,
          success: true
        });
      }
    }
    else {
      res.status(404).json({
        stderr: "No records found for the provided offset and size.",
        success: "false"
      });
    }
  }
  catch (err) {
    console.error(`Error while getting PCAP Detail Record `, err.message);
    next(err);

  }
}

/**
 * Handles the request to get the PCAP file for a given massId.
 * 
 * This function performs the following steps:
 * 1. Validates the massId query parameter to ensure it is provided and is a valid integer.
 * 2. Fetches the PCAP file using the provided massId.
 * 3. Returns the PCAP file for download or an error response if the file is not found.
 *
 * @param {Object} req
 * @param {Object} res 
 * @param {Function} next 
 */

async function getPcap(req, res, next) {
  try {

    // Validate input
    const massId = req.query.massId;
    if (!massId) {
      return res.status(400).json({
        stderr: "Please provide massId.",
        success: "false"
      });
    }

    // Check if massId is not a strictly positive integer
    if (!/^\d+$/.test(massId) || parseInt(massId, 10) <= 0) {
      return res.status(400).json({
        stderr: "Invalid input: 'massId' must be a positive integer.",
        success: false
      });
    }

    // Check for unsupported query parameters
    const invalidParams = Object.keys(req.query).filter(param => param !== 'massId');
    if (invalidParams.length) {
      return res.status(400).json({
        stderr: `Unsupported query parameter(s): '${invalidParams.join(', ')}'.`,
        success: false
      });
    }

    // Fetch the PCAP 
    const result = await cdr.getPCAP(massId);

    // Handle errors from getPCAP
    if (!result.success) {
      return res.status(result.statusCode).json({
        stderr: result.stderr,
        success: false
      });
    }

    // File found, send it for download
    const { pcapPath, pcapName } = result;

    res.download(pcapPath, pcapName, (err) => {
      if (err) {
        console.error("Error during file download: ", err.message);
        return res.status(500).json({
          stderr: "An error occurred while downloading the PCAP file.",
          success: false
        });
      }
    });

  } catch (err) {
    console.error(`Error while getting PCAP Detail Record `, err.message);
    next(err);
  }
}

/**
 * Handles the request to push timestamp entries to redis queue.
 * 
 * @param {Object} req
 * @param {Object} res 
 * @param {Function} next 
 */

async function cdrsReprocess(req, res, next) {
  try {
    // Validate the massId query parameter
    const startTime = req.query.startTime;
    const endTime = req.query.endTime;
    if (!startTime || !endTime) {
      return res.status(400).json({
        stderr: "Please provide start and end time.",
        success: "false"
      });
    }

    // Check for unsupported query parameters
    const allowedParams = ['startTime', 'endTime'];
    const invalidParams = Object.keys(req.query).filter(
      param => !allowedParams.includes(param)
    );
    if (invalidParams.length) {
      return res.status(400).json({
        stderr: `Unsupported query parameter(s): '${invalidParams.join(', ')}'.`,
        success: false
      });
    }

    // Fetch the PCAP detail record status using the provided massId
    let result = await cdr.cdrsReprocess(
      req.query.startTime,
      req.query.endTime
    );
    if (result.statusCode) {
      res.status(result.statusCode).json({
        stderr: result.stderr || "An error occurred while reprocessing CDRs.",
        success: "false"
      });
    }
    else {
      res.status(200).json({
        ...result,
        success: true
      });
    }
  } catch (err) {
    console.error(`Error while adding reprocessing entries to redis `, err.message);
    next(err);
  }
}


module.exports = {
  getPcapStatus,
  getPcapStatusByCDRId,
  getBatchPCAPsStatus,
  getPcap,
  getPcapBatchInfo,
  cdrsReprocess
}