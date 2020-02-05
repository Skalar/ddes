# DDES

> JavaScript/TypeScript framework for Distributed Event Sourcing & CQRS

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![NPM version](https://img.shields.io/npm/v/@ddes/core.svg)](https://www.npmjs.com/org/ddes)
[![Node](https://img.shields.io/npm/v/@ddes/core.svg)](https://www.npmjs.com/org/ddes)
[![Type definitions](https://img.shields.io/npm/types/@ddes/core.svg)](https://s3-eu-west-1.amazonaws.com/ddes-docs/latest/index.html)
[![Build Status](https://travis-ci.org/Skalar/ddes.svg?branch=master)](https://travis-ci.org/Skalar/ddes)
## Usage

See [website](https://ddes.io) for documentation.

## Development

### Local DynamoDB, Postgres and S3 for running tests

```bash
# Start
docker-compose up -d

# Stop and clean
docker-compose down
```

### Running tests

```bash
scripts/test --watch
```
