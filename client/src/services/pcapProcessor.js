const helper = require("../utils/helper");
const { rename, mkdir, access, readdir, unlink } = require("fs").promises;
require("dotenv").config();
const xfvm = require('../utils/xfvm');
const moment = require("moment");
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

const re = /^(.*)\.pcap#(\d+)$/;
async function mergeSplitPcaps(destDir) {
  let entries;
  try {
    entries = await readdir(destDir);
  } catch (err) {
    throw new Error(`Failed to read directory ${destDir}: ${err.message}`);
  }
  const splitFilesMap = {};

  for (const name of entries) {
    const match = re.exec(name);
    if (match) {
      const baseName = match[1];
      const number = parseInt(match[2], 10);
      const fullPath = path.join(destDir, name);
      if (!splitFilesMap[baseName]) splitFilesMap[baseName] = [];
      splitFilesMap[baseName].push({ path: fullPath, number });
    }
  }
  for (const [baseName, splitFiles] of Object.entries(splitFilesMap)) {
    // Sort ascending
    splitFiles.sort((a, b) => a.number - b.number);

    const originalPath = path.join(destDir, `${baseName}.pcap`);
    const tempOutputPath = path.join(destDir, `${baseName}.pcap.temp`);

    const args = splitFiles.map(f => f.path);

    // Append original if it exists
    try {
      await access(originalPath);
      args.push(originalPath);
    } catch (_) {
      // file doesn't exist, skip
    }

    // Construct the shell command
    const cmd = `cat ${args.map(p => `"${p}"`).join(' ')} > "${tempOutputPath}"`;

    try {
      await execAsync(cmd);
    } catch (err) {
      console.error(`cat command failed for ${baseName}: ${err.message}\nOutput: ${err.stdout || ''}`);
    }

    // Replace original
    try {
      await rename(tempOutputPath, originalPath);
    } catch (err) {
      console.error(`Failed to rename ${tempOutputPath} to ${originalPath}: ${err.message}`);
    }

    // Delete split files
    for (const file of splitFiles) {
      try {
        await unlink(file.path);
      } catch (err) {
        console.warn('Failed to remove split file', { path: file.path, error: err.message });
      }
    }
  }
}

async function waitForFileAndRename(src, dest, retries = 3, delay = 100) {

  for (let i = 0; i < retries; i++) {
    try {
      await access(src); // File is now available
      await mkdir(path.dirname(dest), { recursive: true }); // Ensure dest dir exists
      await rename(src, dest);
      return true;
    } catch (err) {
      if (err.code === 'ENOENT' && i < retries - 1) {
        console.log(`File not found, retrying in ${delay}ms... (${i + 1}/${retries})`);
        await new Promise((res) => setTimeout(res, delay));
      } else {
        console.error(`❌ Rename failed after ${i + 1} attempts:`, err);
        return false;
      }
    }
  }
}
// Build a map of base filenames to their duplicates
async function buildDuplicateMap(tempDirPathSIP) {
  // Read all files in the directory
  const files = await readdir(tempDirPathSIP);
  const map = new Map();
  // Group files by base name
  for (const file of files) {
    // detect base name (strip trailing .~N~ if present)
    const match = file.match(/^(.*?)(\.\~\d+\~)?$/);
    const base = match ? match[1] : file;

    if (!map.has(base)) {
      map.set(base, []);
    }
    map.get(base).push(file);
  }

  // Sort each list so base comes first, then .~1~, .~2~, etc.
  for (const [base, list] of map.entries()) {
    list.sort((a, b) => {
      if (a === base) return -1;
      if (b === base) return 1;
      return a.localeCompare(b);
    });
  }
  return map;
}

/**
 * This function returns a Pcap file for a given CDR ID.
 *
 * @memberOf CDR
 * @param {integer} cdrID - The starting CDR ID for which record fetching should start
 * @returns {object} - Prepares the pcap file and returns an object containg path to that pcap file in Tempdir. The calling function then serves the pcap file back to user.
 */
async function getPcaps(cdrs, pcapPath) {
  const client_id = process.env.CLIENT_ID;
  const sip_cdrs = [];
  const [cdrDate, cdrTime] = cdrs.minute.split(' ');  // "2024-10-01" and "09:40"
  const [cdrHour, cdrMinute] = cdrTime.split(':');
  const sipPath = `${cdrs.sensor_path}/${cdrDate}/${cdrHour}/${cdrMinute}/SIP/sip_${cdrDate}-${cdrHour}-${cdrMinute}.tar.gz`;
  let rtpPath = `${cdrs.sensor_path}/${cdrDate}/${cdrHour}/${cdrMinute}/RTP/rtp_${cdrDate}-${cdrHour}-${cdrMinute}.tar`;
  const currentDate = cdrs.currentDate // Timestamp
  const [datePart, timePart] = currentDate.split(' ');
  const [hour, minute] = timePart.split(':');
  if (cdrs.records.length === 0) {
    console.warn("No records found for the given CDRs.");
    return sip_cdrs;

  }

  const compressionType = process.env.COMPRESSION_TYPE || "zstd";
  let fileExtension = "tar.zst";
  if (compressionType === "lz4") {
    fileExtension = "tar.lz4";
  }
  const mergedDir = `${pcapPath}/client_node_${client_id}/${cdrDate}/${cdrHour}/${cdrMinute}`;
  const temp_mergedDir = `${process.env.TEMP_STORAGE_PATH}/client_node_${client_id}/${cdrDate}/${cdrHour}/${cdrMinute}/${cdrs.records[0].id_sensor}`;
  const storagePath = `${pcapPath}/client_node_${client_id}/${cdrDate}/${cdrHour}/${cdrMinute}/${cdrDate}-${cdrHour}-${cdrMinute}-${cdrs.records[0].id_sensor}.${fileExtension}`;

  console.debug("SIP PATH:", sipPath);
  const SIP_Exists = await helper.fileChecker(sipPath);
  if (SIP_Exists) {
    const tempDirPathSIP = `${process.env.TEMP_STORAGE_PATH}/SIP/client_node_${client_id}/worker_${cdrs.worker_id}/${datePart}/${hour}/${minute}`;
    const tempDirPathRTP = `${process.env.TEMP_STORAGE_PATH}/RTP/client_node_${client_id}/worker_${cdrs.worker_id}/${datePart}/${hour}/${minute}`;
    console.debug(`DIRECTORY ${cdrs.minute} CREATION START TIME: `, moment().format("YYYY-MM-DD HH:mm:ss"));
    try {
      await mkdir(tempDirPathSIP, { recursive: true });
      await mkdir(tempDirPathRTP, { recursive: true });
      await mkdir(mergedDir, { recursive: true });
      await mkdir(temp_mergedDir, { recursive: true });
    }
    catch (err) {
      console.error("Error creating temp directory:", err);
    }
    const sipCommand = `tar --use-compress-program='pigz -p ${process.env.CompressionCores || 4}' --backup=numbered -xf ${sipPath} -C ${tempDirPathSIP}`;
    try {
      console.log(`STARTING UNTAR SIP ${moment().format("YYYY-MM-DD HH:mm:ss")} ${sipPath}`);
      await helper.sequentialExecution(sipCommand);
      await mergeSplitPcaps(tempDirPathSIP);
      rtpPath = cdrs.localFilePath;
    }
    catch (err) {
      console.error("Error extracting SIP tar:", err);
    }
    const duplicateMap = await buildDuplicateMap(tempDirPathSIP);
    console.log(`TIME when task ${cdrs.minute} started: `, moment().format("YYYY-MM-DD HH:mm:ss"));
    const records = cdrs.records;

    // const batchSize = 100;
    for (let i = 0; i < records.length; i++) {
      // const batch = records.slice(i, i + batchSize);
      // await Promise.all(batch.map(async (record) => {
      let record = records[i];
      record.client_id = client_id;
      record.call_id = record.fbasename;
      record.SIPPath = tempDirPathSIP;
      record.RTPPath = tempDirPathRTP;
      record.conversion_date = currentDate;
      let filename = `${record.fbasename.replace(/[^A-Za-z0-9:\-\.@]/g, '_')}`;
      if (filename.length > parseInt(process.env.MAX_CALL_ID_LEN || 87, 10)) {
        filename = `${filename.slice(0, 54)}_hash_${helper.getStringMD5(filename.slice(54) + ".pcap")}`;
      }
      else {
        filename = `${filename}.pcap`;
      }
      let candidate = null;
      if (duplicateMap.has(filename)) {
        candidate = duplicateMap.get(filename).pop(); // take the next available file
      }
      if (!candidate) {
        candidate = filename; // fallback to original if none left
      }
      if (!rtpPath && record.tarPositions) {
        record.conversion_status = 0;
        record.storage_path = null;
        record.error_msg = "RTP tar not found";
        record.client_id = client_id;
        sip_cdrs.push(record);
      }
      else if (record.tarPositions && rtpPath) {
        record.worker_id = cdrs.worker_id;
        const tarPos = record.tarPositions;
        record.tarPositions = null;
        record.sipPcap = candidate;
        const message = `${rtpPath} ${filename} ${tarPos} ${tempDirPathRTP}/${record.cdr_id}.pcap ${JSON.stringify(record)}`;
        xfvm.assignTaskToXFVM(message);
      } else {
        try {
          const src = `${tempDirPathSIP}/${candidate}`;
          const dest = `${temp_mergedDir}/${record.cdr_id}.pcap`;
          const success = await waitForFileAndRename(src, dest);

          if (success) {
            record.conversion_status = 1;
            record.storage_path = storagePath;
            record.error_msg = null;
            record.client_id = client_id;
          } else {
            record.conversion_status = 0;
            record.storage_path = null;
            record.error_msg = "SIP PCAP Not Found.";
            record.client_id = client_id;
          }
        } catch (error) {
          console.error(`Error processing record ${record.cdr_id}:`, error);
          record.conversion_status = 0;
          record.storage_path = null;
          record.error_msg = "SIP PCAP Not Found.";
          record.client_id = client_id;
        }
        sip_cdrs.push(record);
      }
      // }));
    }

    console.log(`ONLY XFVM TASKS REMAINING ${cdrs.minute} :`, moment().format("YYYY-MM-DD HH:mm:ss"));
    return sip_cdrs;
  }
  else {
    for (let i = 0; i < cdrs.records.length; i++) {
      let record = cdrs.records[i];
      record.call_id = record.fbasename;
      record.conversion_status = 0;
      record.conversion_date = currentDate;
      record.storage_path = null;
      record.error_msg = "SIP tar not found";
      record.client_id = client_id;
      sip_cdrs.push(record);
    }
    return sip_cdrs;
  }
}
async function handleXFVMResponse(data) {
  try {
    // Extract file path before JSON
    const jsonMatch = data.trim().match(/{[^{}]*}$/s); // Match JSON from start `{` to end `}`
    const cdr = jsonMatch ? JSON.parse(jsonMatch[0]) : null;

    const currentDate = cdr.conversion_date // Timestamp
    if (data.startsWith("ERROR")) {
      console.error(data);
      cdr.conversion_status = 0;
      cdr.conversion_date = currentDate;
      cdr.storage_path = null;
      cdr.error_msg = "RTP extraction failed";
      return cdr;
    }
    const [cdrDate, cdrTime] = cdr.call_date.split(' ');  // "2024-10-01" and "09:40"
    const [cdrHour, cdrMinute] = cdrTime.split(':');
    const compressionType = process.env.COMPRESSION_TYPE || "zstd";
    let fileExtension = "tar.zst";
    if (compressionType === "lz4") {
      fileExtension = "tar.lz4";
    }
    const storagePath = `${process.env.PCAP_STORAGE_PATH || "/isilon"}/client_node_${process.env.CLIENT_ID}/${cdrDate}/${cdrHour}/${cdrMinute}/${cdrDate}-${cdrHour}-${cdrMinute}-${cdr.id_sensor}.${fileExtension}`;

    const temp_mergedDir = `${process.env.TEMP_STORAGE_PATH || "/media"}/client_node_${process.env.CLIENT_ID}/${cdrDate}/${cdrHour}/${cdrMinute}/${cdr.id_sensor}`;
    const tempDirPathSIP = cdr.SIPPath;
    const tempDirPathRTP = cdr.RTPPath;
    let filename = `${cdr.fbasename.replace(/[^A-Za-z0-9:\-\.@]/g, '_')}`;
    if (filename.length > parseInt(process.env.MAX_CALL_ID_LEN || 87, 10)) {
      filename = `${filename.slice(0, 54)}_hash_${helper.getStringMD5(filename.slice(54) + ".pcap")}`;
    }
    else {
      filename = `${filename}.pcap`;
    }

    try {
      try {
        await access(`${tempDirPathSIP}/${cdr.sipPcap}`);
      } catch (error) {
        console.error(`SIP PCAP not found: ${tempDirPathSIP}/${cdr.sipPcap}`);
        cdr.conversion_status = 0;
        cdr.conversion_date = currentDate;
        cdr.storage_path = null;
        cdr.error_msg = "SIP PCAP Not Found.";
        return cdr;
      }
      const result = await helper.sequentialExecution(`mergecap -w ${temp_mergedDir}/${cdr.cdr_id}.pcap ${tempDirPathSIP}/${cdr.sipPcap} ${tempDirPathRTP}/${cdr.cdr_id}.pcap`);
      if (result == -1) {
        throw new Error("mergecap failed with result -1");
      }
      cdr.conversion_status = 1;
      cdr.conversion_date = currentDate;
      cdr.storage_path = storagePath;
      cdr.error_msg = null;
      return cdr;

    } catch (error) {

      const finalMergedFile = `${temp_mergedDir}/${cdr.cdr_id}.pcap`;

      try {
        await access(finalMergedFile); // Checks if file exists
        cdr.conversion_status = 1;
        cdr.conversion_date = currentDate;
        cdr.storage_path = storagePath;
        cdr.error_msg = null;
        return cdr;
      } catch {
        cdr.conversion_status = 0;
        cdr.conversion_date = currentDate;
        cdr.storage_path = null;
        cdr.error_msg = "SIP RTP Merging Failed";
        console.log("SIP RTP merging failed for CDR ID: ", cdr.cdr_id);
        return cdr;
      }
    }
  }
  catch (err) {
    return null;
  }
}
module.exports = {
  getPcaps, handleXFVMResponse
};
