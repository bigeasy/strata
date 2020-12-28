const crypto = require('crypto')
const fnv = require('hash.fnv')

module.exports = function (buffer) {
    return fnv(0, buffer, 0, buffer.length)
}
