#!/usr/bin/env node

const {cli} = require('.')

cli().catch(error => {
  console.error(error)
  process.exit(1)
})
