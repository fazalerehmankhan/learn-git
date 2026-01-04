const fs = require('fs');
const path = require('path');

// Function to recursively delete empty folders if they are at least 5 minutes old
function deleteEmptyFolders(directory) {
  try {
    if (!fs.existsSync(directory)) {
      return false;
    }

    const items = fs.readdirSync(directory);
    let isEmpty = true;

    for (const item of items) {
      const fullPath = path.join(directory, item);
      const stats = fs.statSync(fullPath);
      const ageInMinutes = (Date.now() - stats.ctimeMs) / 60000;

      if (stats.isDirectory()) {
        const isSubFolderEmpty = deleteEmptyFolders(fullPath);
        if (isSubFolderEmpty && ageInMinutes >= 5) {
          fs.rmdirSync(fullPath);
          console.log(`Deleted empty folder (5+ min old): ${fullPath}`);
        } else {
          isEmpty = false;
        }
      } else {
        isEmpty = false;
      }
    }

    return isEmpty;
  } catch (err) {
    // console.error(`Error processing directory: ${directory}`, err);
    return false;
  }
}

module.exports = { deleteEmptyFolders };