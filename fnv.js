const fnv = require('hash.fnv')

module.exports = function (buffer) {
    return Number(fnv(0, buffer, 0, buffer.length)).toString(16)
}
