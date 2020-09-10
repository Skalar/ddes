#!/usr/bin/env node
/* eslint-disable */

const {cli} = require('.')

cli().catch(error => {
  console.error(error)
  process.exit(1)
})
