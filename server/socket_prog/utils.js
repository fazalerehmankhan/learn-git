
function getTimestamp() {
  const now = new Date();
  return `${now.toISOString().slice(0, 19).replace('T', ' ')}`;
}

module.exports = { getTimestamp };
