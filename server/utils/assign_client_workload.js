const redisClient = require('../redisClient');
const { acquireLockWithRetry } = require('../utils/helper')

const storeCDRsInRedis = async (client_id, CDRs) => {
	try {
		for (const i in CDRs) {
			const cdr = JSON.parse(CDRs[i]);
			console.debug("STORING IN Client HASH", cdr);
			await redisClient.hSet(`client:${client_id}`, `${cdr.minute}:${cdr.id_sensor}`, JSON.stringify(cdr));
		}
	} catch (error) {
		console.log("Error while writing CDRs to hash", error)
	}
}

const sendDataToClient = async (client, numberOfEntries) => {
	const script = `
	local list = KEYS[1]
	local count = tonumber(ARGV[1])
	local length = redis.call('LLEN', list)
	if length == 0 then
		return {}
	end
	local realCount = math.min(length, count)
	local entries = redis.call('LRANGE', list, 0, realCount - 1)
	redis.call('LTRIM', list, realCount, -1)
	return entries
	`;
	try {
		if (!client.socket || !client.socket.writable || client.socket.destroyed) {
			console.error(`Socket for client ${client.id} is not writable or has been destroyed. Cannot send data.`);
			return;
		}
		console.log(`Try sending CDRs to Client with id ${client.id}`);
		const len = await redisClient.lLen('cdrQueue');
		console.log("CDR QUEUE LENGTH: ", len);
		const entries = await redisClient.eval(script, {
			keys: ['cdrQueue'],
			arguments: [String(numberOfEntries)],
		});
		console.log("Retrieved entries length from cdrQueue:", entries.length);
		// Check if there are any entries to send
		if (entries.length > 0) {
			console.log(`Sending the data to client with id ${client.id}`);
			console.debug('Sending the following data to client:', entries);

			// Convert the entries to a JSON string before sending
			const dataToSend = JSON.stringify(entries);
			await storeCDRsInRedis(client.id, entries);
			// Log the data being sent to the client
			console.debug(`Sending data to client ${client.id}:`, dataToSend);
			const message = JSON.stringify({ type: 'cdrs', data: dataToSend });
			try {
				await new Promise((resolve, reject) => {
					client.socket.write(message + '\n', (err) => {
						if (err) return reject(err);

						let buffer = '';
						const timeout = setTimeout(() => {
							client.socket.removeListener('data', onData);
							reject(new Error("No ACK from client"));
						}, 20000); // 20 second ACK timeout
						const onData = (chunk) => {
							buffer += chunk.toString();
							let newlineIndex;
							while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
								const line = buffer.slice(0, newlineIndex).trim();
								buffer = buffer.slice(newlineIndex + 1);
								if (!line) continue;

								try {
									const parsed = JSON.parse(line);
									if (parsed.type === 'ack' && parsed.context === 'cdr_send') {
										console.log(`Received ACK from client ${client.id}`);

										clearTimeout(timeout);
										client.socket.removeListener('data', onData);
										resolve();
										return;
									}
								} catch {
									// Ignore JSON parse errors and continue buffering
								}
							}
						};

						client.socket.on('data', onData);
					});
				});

				console.log(`Data sent to client ${client.id} successfully.`);

			} catch (error) {
				console.error('Error sending data to client:', error.stack || error.message || error);
				const lockKey = `restore_lock:${client.id}`;
				const gotLock = await acquireLockWithRetry(lockKey);
				console.log(`Attempting to restore CDRs to cdrQueue for client ${client.id} after failed send.`);
				if (gotLock) {
					console.log(`Acquired lock for client ${client.id} to restore CDRs to cdrQueue.`);

					try {
						// Push the entries back to the front of the queue in reverse order
						for (let i = entries.length - 1; i >= 0; i--) {
							cdr = JSON.parse(entries[i]);
							const exists = await redisClient.hExists(`client:${client.id}`, `${cdr.minute}:${cdr.id_sensor}`);
							if (exists) {
								await redisClient.hDel(`client:${client.id}`, `${cdr.minute}:${cdr.id_sensor}`);
								await redisClient.rPush('cdrQueue', entries[i]);
							}
						}
					}
					catch (restoreError) {
						console.error('Error restoring CDRs to cdrQueue:', restoreError);
					}
					finally {
						await redisClient.del(lockKey);
					}
				}
				console.log('Pushed entries back to cdrQueue after failed send');
				setTimeout(async function () {
					await sendDataToClient(client, numberOfEntries);
				}, 30000);
			}
		}
		else {
			console.warn('Insufficient entries in cdrQueue to send, setting timeout...');
			setTimeout(async function () {
				await sendDataToClient(client, numberOfEntries);
			}, 30000);
		}

	} catch (error) {
		console.error('Error processing Redis queue entries:', error);
		setTimeout(async function () {
			await sendDataToClient(client, numberOfEntries);
		}, 30000);
	}
};


const removeCDRsFromRedis = async (client_id) => {
	console.debug("CLIENT ID IN REMOVE FROM REDIS:  ", client_id);
	const results = await redisClient.multi().hGetAll(`client:${client_id}`).del(`client:${client_id}`).exec();
	const cdrs = Object.values(results[0]);

	console.log('assigning cdrs in inqueue from hash');
	console.debug('assigning these cdrs in inqueue from hash:', cdrs);
	for (const cdr of cdrs) {
		await redisClient.rPush('cdrQueue', cdr);
	}
}


module.exports = { sendDataToClient, removeCDRsFromRedis };