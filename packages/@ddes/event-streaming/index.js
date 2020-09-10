/* eslint-disable */
const semver = require('semver')

if (typeof Symbol.asyncIterator === 'undefined') {
  // @ts-ignore
  Symbol.asyncIterator =
    Symbol.asyncIterator || Symbol.for('Symbol.asyncIterator')
}

if (semver.satisfies(process.versions.node, '>=10')) {
  try {
    module.exports = require('./dist')
  } catch (error) {
    module.exports = require('./lib')
  }
} else {
  throw new Error('NodeJS >=10 or newer is required')
}
