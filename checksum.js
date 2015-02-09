var crypto = require('crypto')

module.exports = function (checksum) {
    if (typeof checksum == 'function') return checksum
    var algorithm
    switch (algorithm = checksum || 'sha1') {
    case 'none':
        // todo: return null
        return function () {
            return {
                update: function () {},
                digest: function () { return '0' }
            }
        }
    default:
        return function (m) { return crypto.createHash(algorithm) }
    }
}
