const semver = require('semver')

Symbol.asyncIterator =
  Symbol.asyncIterator || Symbol.for('Symbol.asyncIterator')

if (semver.satisfies(process.versions.node, '<8.10')) {
  throw new Error('NodeJS >=8.10 or newer is required')
} else if (semver.satisfies(process.versions.node, '>=8.10.0 <10')) {
  // tslint:disable-next-line:no-var-requires
  module.exports = require('./dist/node-8.10')
} else if (semver.satisfies(process.versions.node, '>=10')) {
  // tslint:disable-next-line:no-var-requires
  module.exports = require('./dist/node-10')
}
