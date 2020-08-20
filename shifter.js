module.exports = function (checksum) {
    return function (array) {
        if (array.length == 0) {
            return null
        }
        const checksum = array.shift(), header = array.shift(), key = array.shift()
        if (header.lengths.length == 1) {
            return [ header, key, null ]
        }
        return [ header, key, array.shift() ]
    }
}
