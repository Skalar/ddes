try {
  module.exports = require('./dist')
} catch (error) {
  module.exports = require('./lib')
}
