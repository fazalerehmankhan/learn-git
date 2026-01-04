# Release

## v2.8.0
 - [feature] Added feature of pendingList entries timeout from redis.

## v2.7.0
 - [feature] Added Cleanup Mechanism on the client application. when server client connection breaks.
 - the client clears up all in memory queues. kill all active processes like tar, scp mergecap etc initiated by the client
 - sends CLEAR_QUEUE command to XFVM to stop it from processing further.
 - cleans up temp directories and files of that client.

## v2.6.0

- [feature] Add support of handling duplicate Call IDs in a single minute on the same sensor.

## v2.5.0

- [feature] Add support of handling CALL IDs with length greater than 87 characters.

## v2.4.0

- [feature] Add new API Endpoint for pushing timestamp entries to Redis Queue. It help reprocessing CDR entries on the base of time range
- [feature] Add jitter base exponential backup logic in client reconnecting and 5 retries in case of connection rejected.
- Code clean up of server and client socket connection

## v2.3.5

- Update the Offset Size API enpoint, remove the series detection logic and return only available records with 206 status code in case of incomplete data.

## v2.3.4

- [bugfix] Handle ping messages if the ping string found in call id, also add ACKs on client and server on recieving messages.

## v2.3.3

- [hotfix] Add ping pong mechanism to detect if the client disconnect without sending signal due to network issue

## v2.3.2

- [bugfix] Update regex for reading pcaps from isilon storage

## v2.3.1

- [bugfix] Resolve bugfix of missing headers of SIP PCAPs.
- [bugfix] Resolve bugfix of duplicate entries of CDRs in MP DB.
- [bugfix] Update MySQL insert query for handling unepected characters in error msg.

## v2.3.0

- [feature] Add files compression feature in solution, support lz4 and zstd.
- [feature] Add new API endpoint for fetching pcap batch info.

## v2.2.1

- [bugfix] Optimize the DA Database query for fetching CDRs data per minute.

## v2.2.0

- [bugfix] Update the response message from "SIP Rename failed" to "SIP PCAP not found".
- [bugfix] Resolve the Isilon and MP Database inconsistency Bug.
- [bugfix] Resolve the DA DB and MP DB records inconsistency Bug.
- [feature] Solution will be able to process pcaps as long as files moved to isilon storage.

## v2.1.0

- [bugfix] Resolve the RTP Extraction failed bug.
- [feature] Add new API endpoint for getting pcap status by cdr id and update API responses.

## v2.0.1

[hotfix] Add logic to fetch only those CDRs from the database where the calldate is at least 4 hours older than the current time.

## v2.0.0

[feature] Change the solution design to processing cdrs in Per Minute Batches.

## v1.0.1

[bugfix] Delete directories from /tmp in case of failure conditions

## v1.0.0

Stable release for Mass PCAP to be pushed to/used in production.



