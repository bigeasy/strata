var crypto = require('crypto')

module.exports = function (checksum, binary) {
    if (typeof checksum == 'function') return checksum
    var algorithm = checksum || 'sha1'
    if (algorithm == 'none') {
        return null
    }
    if (binary) {
        return function (buffer, start, end) {
            var hash = crypto.createHash(algorithm)
            hash.update(buffer.slice(start, end))
            return new Buffer(hash.digest('hex'), 'hex')
        }
    }
    return function (buffer, start, end) {
        var hash = crypto.createHash(algorithm)
        hash.update(buffer.slice(start, end))
        return hash.digest('hex')
    }
}
