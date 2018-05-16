const semver = require('semver')

if (semver.satisfies(process.versions.node, '<8.10')) {
  throw new Error('NodeJS >=8.10 or newer is required')
} else if (semver.satisfies(process.versions.node, '>=8.10.0 <10')) {
  module.exports = require('./dist/node-8.10/lib/TransformerGui')
} else if (semver.satisfies(process.versions.node, '>=10')) {
  try {
    module.exports = require('./dist/node-10/lib/TransformerGui')
  } catch (error) {
    module.exports = require('./lib/TransformerGui')
  }
}
