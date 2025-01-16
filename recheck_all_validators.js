require('dotenv').config();
const { MongoClient } = require('mongodb');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const winston = require('winston');

// Helper for HH:MM:SS
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
        seconds.toString().padStart(2, '0')
    ].join(':');
}

const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
        })
    ),
    transports: [new winston.transports.Console()]
});

const ENDPOINT = process.env.ENDPOINT 
const MONGO_URI = process.env.MONGO_URI
const MONGO_DBNAME = process.env.MONGO_DBNAME 

async function parseWithdrawalAddress(withdrawalCredentials) {
    if (
        withdrawalCredentials &&
        withdrawalCredentials.startsWith('0x0100') &&
        withdrawalCredentials.length === 66
    ) {
        const eth1Hex = '0x' + withdrawalCredentials.slice(-40);
        return eth1Hex.toLowerCase();
    }
    return '';
}

async function recheckValidatorsConcurrently(allValidatorIds) {
    const CONCURRENCY = 100; // tweak as desired
    const results = [];
    let index = 0;
    let inFlight = 0;

    const startTime = Date.now();
    let checkedCount = 0;
    const total = allValidatorIds.length;

    return new Promise(resolve => {
        const next = async () => {
            if (index >= total) {
                if (inFlight === 0) resolve(results);
                return;
            }

            const valIndex = allValidatorIds[index];
            index++;
            inFlight++;

            try {
                const status = await fetchValidatorStatus(valIndex);
                results.push({ valIndex, status });
            } catch (err) {
                logger.error(`Validator ${valIndex} recheck failed: ${err.message}`);
            } finally {
                inFlight--;
                checkedCount++;

                if (checkedCount % 25 === 0 || checkedCount === total) {
                    const elapsedMs = Date.now() - startTime;
                    const elapsed = formatDurationMs(elapsedMs);
                    const avgMs = elapsedMs / checkedCount;
                    const remain = total - checkedCount;
                    const etaMs = avgMs * remain;
                    const eta = formatDurationMs(etaMs);
                    logger.info(
                        `Recheck progress: ${checkedCount}/${total}, Elapsed: ${elapsed}, ETA: ${eta}`
                    );
                }

                next();
            }
        };

        const workers = Math.min(CONCURRENCY, allValidatorIds.length);
        for (let i = 0; i < workers; i++) {
            next();
        }
    });
}

async function fetchValidatorStatus(valIndex) {
    const url = `${ENDPOINT}/eth/v1/beacon/states/head/validators/${valIndex}`;
    const resp = await fetch(url);
    if (!resp.ok) {
        logger.debug(`fetchValidatorStatus(${valIndex}): HTTP ${resp.status}`);
        return null;
    }
    const json = await resp.json();
    if (!json.data || !json.data.validator) return null;

    const { status, validator } = json.data;
    const wc = validator.withdrawal_credentials;
    const parsedAddress = await parseWithdrawalAddress(wc);

    // Update in DB
    const validatorsColl = global.db.collection('validators');
    await validatorsColl.updateOne(
        { _id: valIndex },
        {
            $set: {
                validator_index: valIndex,
                withdrawal_credentials: wc,
                withdrawal_address: parsedAddress,
                last_known_status: status
            }
        },
        { upsert: true }
    );

    return status;
}

async function main() {
    let mongoClient;
    try {
        logger.info('Re-checking ALL validators in DB...');
        mongoClient = new MongoClient(MONGO_URI);
        await mongoClient.connect();
        global.db = mongoClient.db(MONGO_DBNAME); // store in global so fetchValidatorStatus can use it
        logger.info(`Connected to MongoDB at ${MONGO_URI}, DB="${MONGO_DBNAME}"`);

        // 1) Grab all validators from DB
        const validatorsColl = global.db.collection('validators');
        const allDocs = await validatorsColl
            .find({}, { projection: { _id: 1 } })
            .toArray();
        if (allDocs.length === 0) {
            logger.info('No validators in DB to re-check. Exiting.');
            return;
        }
        logger.info(`Found ${allDocs.length} validator doc(s). Will re-check all.`);

        // 2) concurrency-limited recheck
        const allValidatorIds = allDocs.map(d => d._id);
        await recheckValidatorsConcurrently(allValidatorIds);

        logger.info('All validators recheck complete!');
    } catch (err) {
        logger.error(`Error in recheck_all_validators: ${err.message}`);
        process.exit(1);
    } finally {
        if (mongoClient) await mongoClient.close();
    }
}

if (require.main === module) {
    main().catch(err => {
        logger.error(`Fatal error: ${err.message}`);
        process.exit(1);
    });
}
