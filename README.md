# DDES

> JavaScript/TypeScript framework for Distributed Event Sourcing & CQRS

[![Build Status](https://travis-ci.org/skalar/ddes.svg?branch=master)](https://travis-ci.org/skalar/ddes)

## Usage

See the [website](https://ddes.io) and [API Docs](https://github.com/skalar/ddes).

## Development

### Local DynamoDB and S3 for running tests

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
