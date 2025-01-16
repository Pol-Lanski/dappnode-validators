# Dappnode graffiti script

A Node.js script designed to ingest blockchain data, process validator information, and store relevant statistics into a MongoDB database. It provides us with the amount of active validators using "dappnode" as graffiti and the number of operators in total. It's designed to run on a daily basis to refresh information.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)

## Features

- **Batch Processing:** Efficiently ingests large numbers of blockchain slots in configurable batch sizes. (crucial during initial sync)
- **Concurrency Control:** Limits the number of concurrent operations to prevent overwhelming resources. (crucial during initial sync)
- **Retry Mechanism:** Implements retry logic for failed network requests to enhance reliability.
- **Graceful Shutdown:** Handles termination signals to ensure safe shutdown without data corruption.
- **Comprehensive Logging:** Uses `winston` for detailed logging with configurable log levels.
- **MongoDB Integration:** Stores ingested data and metadata in MongoDB for persistent storage and easy querying.
- **Validator Management:** Fetches and processes validator information, including parsing withdrawal addresses.

## Prerequisites

- **Node.js:** v16.x or higher
- **npm:** v6.x or higher
- **MongoDB:** Accessible instance (local or remote)
- **Beacon node:** Access to a beacon node (historical for first sync)

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/flisko/dappnode-validators.git
   cd flisko/dappnode-validators

   ```

   I recommend you run this script locally first, to fill the db. Then you can run it via github actions daily to get updates stats.

## Contributing

The script can easily be modified to look for any other graffitis and or improve the codebase since we are storing all blocks in the db.
