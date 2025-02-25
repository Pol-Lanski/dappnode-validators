require('dotenv').config();

const ENDPOINT = process.env.ENDPOINT
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE, 10) || 500;
const CONCURRENCY_LIMIT = parseInt(process.env.CONCURRENCY_LIMIT, 10) || 250;
const RETRY_LIMIT = parseInt(process.env.RETRY_LIMIT, 10) || 3;
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const GRAFITI_SEARCH = process.env.GRAFITI_SEARCH || 'dappnode'
const KEY = process.env.KEY
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const winston = require('winston');
const { MongoClient } = require('mongodb');

const logger = winston.createLogger({
    level: LOG_LEVEL,
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
        })
    ),
    transports: [new winston.transports.Console()]
});

function formatDurationMs(ms) {
    if (ms <= 0) return '00:00:00';
    let totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    totalSeconds %= 3600;
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return [
        hours.toString().padStart(2, '0'),
        minutes.toString().padStart(2, '0'),
        seconds.toString().padStart(2, '0'),
    ].join(':');
}

let shutdownRequested = false;
function setupGracefulShutdown() {
    const handleSignal = (signal) => {
        logger.warn(`Received ${signal}. Graceful shutdown requested...`);
        shutdownRequested = true;
    };
    process.on('SIGINT', handleSignal);
    process.on('SIGTERM', handleSignal);
}

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/';
const MONGO_DBNAME = process.env.MONGO_DBNAME || 'dappnode';

let mongoClient;
let db;

async function main() {
    setupGracefulShutdown();

    // 1) Connect to Mongo
    mongoClient = new MongoClient(MONGO_URI, {});
    await mongoClient.connect();
    db = mongoClient.db(MONGO_DBNAME);
    logger.info(`Connected to MongoDB at ${MONGO_URI}, DB="${MONGO_DBNAME}"`);

    // 2) Determine last processed slot, plus head slot
    const lastProcessedSlot = await getMeta('last_processed_slot');
    let startSlot = lastProcessedSlot !== null ? parseInt(lastProcessedSlot, 10) + 1 : 0;

    const headSlot = await getHeadSlot();
    if (startSlot > headSlot) {
        logger.info(
            `last_processed_slot=${startSlot - 1} >= headSlot=${headSlot}. No new slots to process.`
        );
    } else {
        logger.info(`Will ingest blocks from slot ${startSlot} to slot ${headSlot}...`);
        await ingestBlocks(startSlot, headSlot);
    }

    // 3) Check if we already computed the final step for this head slot
    const lastStatsSlot = await getMeta('last_stats_for_slot');
    if (lastStatsSlot && parseInt(lastStatsSlot, 10) === headSlot) {
        logger.info(
            `We already computed final stats for headSlot=${headSlot}. Skipping final step.`
        );
    } else {
        logger.info(
            `No final stats found for headSlot=${headSlot}, or headSlot advanced. Running final step...`
        );
        await runFinalStep(headSlot);
    }

    logger.info('Script #2 completed.');
    await mongoClient.close();
    process.exit(0);
}
async function runFinalStep(currentHeadSlot) {
    logger.info(`Querying DB for graffiti containing the string ${GRAFITI_SEARCH}...`);

    // 1) Find all blocks whose graffiti has GRAFITI_SEARCH variable (case-insensitive)
    const blocksColl = db.collection('blocks');
    const query = { graffiti: { $regex: new RegExp(GRAFITI_SEARCH, 'i') } };
    const blocksCursor = blocksColl.find(query, { projection: { proposer_index: 1 } });
    const uniqueProposersSet = new Set();
    await blocksCursor.forEach(doc => {
        if (doc.proposer_index != null) {
            uniqueProposersSet.add(doc.proposer_index);
        }
    });
    const uniqueProposers = [...uniqueProposersSet];
    logger.info(`Found ${uniqueProposers.length} unique proposers using ${GRAFITI_SEARCH} graffiti.`);

    if (uniqueProposers.length === 0) {
        // No need to do anything else
        await setMeta('last_stats_for_slot', currentHeadSlot.toString());
        await db.collection('stats_history').insertOne({
            slot: currentHeadSlot,
            unique_proposers: 0,
            active_ongoing: 0,
            unique_operators: 0,
            run_ts: new Date()
        });
        return;
    }

    // 2) For concurrency-limited checking, figure out which ones have no validator doc
    const validatorsColl = db.collection('validators');
    // Find which _ids we already have
    const existingDocs = await validatorsColl
        .find({ _id: { $in: uniqueProposers } }, { projection: { _id: 1 } })
        .toArray();
    const alreadyCheckedSet = new Set(existingDocs.map(d => d._id));
    const neverCheckedValidators = uniqueProposers.filter(valIndex => !alreadyCheckedSet.has(valIndex));

    logger.info(
        `We have ${uniqueProposers.length} total ${GRAFITI_SEARCH} proposers, ` +
        `${alreadyCheckedSet.size} already checked, ` +
        `${neverCheckedValidators.length} never checked.`
    );

    let activeCount = 0;

    if (neverCheckedValidators.length > 0) {
        logger.info('Concurrency-limited validator checks for new, never-checked validators...');
        const results = await checkValidatorsInParallel(neverCheckedValidators, currentHeadSlot);

        // Count how many came back as "active_ongoing"
        for (const r of results) {
            if (r && r.last_known_status === 'active_ongoing') {
                activeCount++;
            }
        }
    }

    logger.info(`Newly-checked validators that are active_ongoing: ${activeCount}`);

    // 3) Now see how many unique withdrawal addresses among ALL these proposers
    // (some might have been checked previously)
    const addresses = await validatorsColl.distinct('withdrawal_address', {
        validator_index: { $in: uniqueProposers },
        withdrawal_address: { $ne: '' }
    });

    const allActiveValidators = await validatorsColl.countDocuments({ last_known_status: 'active_ongoing' });

    const uniqueOperatorsCount = addresses.length;
    logger.info(
        `Unique operators (parsed ETH1 addresses) among ${GRAFITI_SEARCH} proposers: ${uniqueOperatorsCount}`
    );

    // 4) Store these stats so we know next time we run, we have them
    await setMeta('last_stats_for_slot', currentHeadSlot.toString());

    // Insert a doc in stats_history
    const statsColl = db.collection('stats_history');
    await statsColl.insertOne({
        slot: currentHeadSlot,
        unique_proposers: uniqueProposers.length,
        newly_active_ongoing: activeCount,
        unique_operators: uniqueOperatorsCount,
        graffiti_search: GRAFITI_SEARCH,
        run_ts: new Date()
    });

    logger.info(
        `Final step done. Stats at slot=${currentHeadSlot}: ${GRAFITI_SEARCH}_validator_proposers=${uniqueProposers.length}, newly_active_ongoing=${activeCount}, unique_operators=${uniqueOperatorsCount}, all active validators count=${allActiveValidators}`
    );
}

async function checkValidatorsInParallel(validatorIndices, currentHeadSlot) {
    const VALIDATOR_CONCURRENCY = 350;

    const results = [];
    let index = 0;
    let inFlight = 0;

    const startTime = Date.now();
    let checkedCount = 0;
    const total = validatorIndices.length;

    return new Promise(resolve => {
        const next = async () => {
            if (index >= total) {

                if (inFlight === 0) resolve(results);
                return;
            }
            if (shutdownRequested) {
                resolve(results);
                return;
            }

            const valIndex = validatorIndices[index];
            index++;
            inFlight++;

            try {
                const doc = await fetchValidatorInfo(valIndex); // fetch from Beacon node
                if (doc) {
                    // Insert into DB
                    await insertValidatorRow(valIndex, {
                        validator_index: valIndex,
                        withdrawal_credentials: doc.withdrawal_credentials,
                        withdrawal_address: doc.withdrawal_address,
                        last_known_status: doc.status
                    });
                    results.push({ validator_index: valIndex, last_known_status: doc.status });
                }
            } catch (err) {
                logger.error(`Validator ${valIndex} check failed: ${err.message}`);
            } finally {
                inFlight--;
                checkedCount++;

                // Show progress every 25 or at the end, with ETA
                if (checkedCount % 25 === 0 || checkedCount === total) {
                    const elapsedMs = Date.now() - startTime;
                    const elapsed = formatDurationMs(elapsedMs);
                    const avgMs = elapsedMs / checkedCount;
                    const remain = total - checkedCount;
                    const etaMs = avgMs * remain;
                    const eta = formatDurationMs(etaMs);
                    logger.info(
                        `Validator check progress: ${checkedCount}/${total}, Elapsed: ${elapsed}, ETA: ${eta}`
                    );
                }

                next();
            }
        };

        const workers = Math.min(VALIDATOR_CONCURRENCY, validatorIndices.length);
        for (let i = 0; i < workers; i++) {
            next();
        }
    });
}

async function fetchValidatorInfo(validatorIndex) {
    const url = `${ENDPOINT}/eth/v1/beacon/states/head/validators/${validatorIndex}?dkey=${KEY}`;
    const resp = await fetch(url);
    if (!resp.ok) {
        logger.debug(`fetchValidatorInfo(${validatorIndex}): HTTP ${resp.status}`);
        return null;
    }
    const json = await resp.json();
    if (!json.data || !json.data.validator) return null;

    const { status, validator } = json.data;
    const wc = validator.withdrawal_credentials;
    const parsedAddress = parseWithdrawalAddress(wc);

    return {
        status,
        withdrawal_credentials: wc,
        withdrawal_address: parsedAddress
    };
}


async function getMeta(key) {
    const metaDoc = await db.collection('meta').findOne({ _id: key });
    return metaDoc ? metaDoc.value : null;
}

async function setMeta(key, value) {
    await db.collection('meta').updateOne(
        { _id: key },
        { $set: { value } },
        { upsert: true }
    );
}

async function insertValidatorRow(validatorIndex, data) {
    const validatorsColl = db.collection('validators');
    await validatorsColl.updateOne(
        { _id: validatorIndex },
        { $set: data },
        { upsert: true }
    );
}

function parseWithdrawalAddress(withdrawalCredentials) {
    if (
        withdrawalCredentials &&
        withdrawalCredentials.startsWith('0x0100') &&
        withdrawalCredentials.length === 66 // "0x" + 64 hex chars
    ) {
        // Last 40 hex chars represent a 20-byte ETH1 address
        const eth1Hex = '0x' + withdrawalCredentials.slice(-40);
        return eth1Hex.toLowerCase();
    }
    return '';
}

async function insertBlock(slot, proposerIndex, graffiti) {
    const blocksColl = db.collection('blocks');
    await blocksColl.updateOne(
        { _id: slot },
        {
            $set: {
                proposer_index: proposerIndex,
                graffiti: graffiti
            }
        },
        { upsert: true }
    );
}
async function ingestBlocks(startSlot, endSlot) {
    const totalSlots = endSlot - startSlot + 1;
    const totalBatches = Math.ceil(totalSlots / BATCH_SIZE);
    logger.info(`Starting ingestion of ${totalSlots} slots in ${totalBatches} batch(es).`);

    const scriptStart = Date.now();
    let batchCount = 0;
    let totalBatchTime = 0;

    for (let batchStart = startSlot; batchStart <= endSlot; batchStart += BATCH_SIZE) {
        if (shutdownRequested) {
            logger.warn('Shutdown requested during ingestion. Stopping early...');
            break;
        }

        const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, endSlot);
        const slotsThisBatch = [];
        for (let s = batchStart; s <= batchEnd; s++) {
            slotsThisBatch.push(s);
        }

        const batchStartTime = Date.now();
        logger.info(
            `Ingesting slots [${batchStart}..${batchEnd}] (${slotsThisBatch.length} slots)...`
        );

        // concurrency-limited fetch
        await processBatchSlots(slotsThisBatch);

        // after this batch
        batchCount++;
        const batchElapsed = Date.now() - batchStartTime;
        totalBatchTime += batchElapsed;
        const avgBatchTime = totalBatchTime / batchCount;
        const batchesLeft = totalBatches - batchCount;
        const etaMs = avgBatchTime * batchesLeft;

        logger.info(
            `Batch done in ${formatDurationMs(batchElapsed)}. 
      Batches done=${batchCount}/${totalBatches}. ETA: ${formatDurationMs(etaMs)}`
        );

        // The last slot we fully processed is batchEnd
        await setMeta('last_processed_slot', batchEnd.toString());
    }

    const totalElapsed = Date.now() - scriptStart;
    logger.info(
        `Finished ingestion up to slot=${endSlot}. Total time: ${formatDurationMs(totalElapsed)}`
    );
}

async function processBatchSlots(slotArray) {
    return new Promise((resolve) => {
        let index = 0;
        let inFlight = 0;
        const next = async () => {
            if (index >= slotArray.length) {
                if (inFlight === 0) {
                    resolve();
                }
                return;
            }
            if (shutdownRequested) {
                resolve();
                return;
            }

            const slot = slotArray[index];
            index++;
            inFlight++;

            try {
                const blockData = await fetchBlockWithRetry(slot);
                if (blockData) {
                    const { graffiti, proposerIndex } = extractGraffitiAndProposer(blockData);
                    // since we are ingesting all blocks, we can query them later for any specific graffiti
                    await insertBlock(slot, proposerIndex || null, graffiti || '');
                }
            } catch (err) {
                logger.error(
                    `processBatchSlots: Slot ${slot} failed after retries. Err=${err.message}`
                );
            } finally {
                inFlight--;
                next();
            }
        };

        // Start up to CONCURRENCY_LIMIT workers
        const workers = Math.min(CONCURRENCY_LIMIT, slotArray.length);
        for (let i = 0; i < workers; i++) {
            next();
        }
    });
}

async function fetchBlockWithRetry(slot) {
    let attempts = 0;
    while (attempts < RETRY_LIMIT) {
        try {
            const data = await getBeaconBlock(slot);
            return data;
        } catch (err) {
            attempts++;
            logger.warn(`Slot ${slot} fetch fail (#${attempts}): ${err.message}`);
            if (attempts >= RETRY_LIMIT) {
                logger.error(`Slot ${slot} - out of retries`);
                throw err;
            }
            // wait a bit
            await new Promise(r => setTimeout(r, 500 * attempts));
        }
    }
    return null;
}

async function getHeadSlot() {
    const url = "https://docs-demo.quiknode.pro/eth/v1/beacon/headers" //public endpoint
    const resp = await fetch(url);
    if (!resp.ok) {
        throw new Error(`getHeadSlot: HTTP error ${resp.status}`);
    }
    const json = await resp.json();
    if (!json.data || !json.data.length) {
        throw new Error('No data from /eth/v1/beacon/headers');
    }
    return parseInt(json.data[0].header.message.slot, 10);
}

async function getBeaconBlock(slot) {
    const url = `${ENDPOINT}/eth/v2/beacon/blocks/${slot}?dkey=${KEY}`;
    const resp = await fetch(url);
    if (resp.status === 404) {
        // no block at this slot
        return null;
    }
    if (!resp.ok) {
        throw new Error(`HTTP status ${resp.status} at slot=${slot}`);
    }
    const json = await resp.json();
    return json.data;
}

function extractGraffitiAndProposer(blockData) {
    if (!blockData) return { graffiti: null, proposerIndex: null };
    try {
        const proposerIndex = parseInt(blockData.message.proposer_index, 10);
        const rawGraffiti = blockData.message.body.graffiti;
        const graffiti = decodeGraffiti(rawGraffiti);
        return { graffiti, proposerIndex };
    } catch (err) {
        return { graffiti: null, proposerIndex: null };
    }
}

function decodeGraffiti(hexStr) {
    if (!hexStr || !hexStr.startsWith('0x')) return '';
    const hex = hexStr.slice(2);
    const buffer = Buffer.from(hex, 'hex');
    // remove trailing null chars
    return buffer.toString('ascii').replace(/\x00+$/, '');
}

if (require.main === module) {
    main().catch((err) => {
        logger.error(`Fatal error in main(): ${err.message}`);
        process.exit(1);
    });
}
