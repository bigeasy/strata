module.exports = function (checksum) {
    return function (array) {
        if (array.length == 0) {
            return null
        }
        var checksum = array.shift(), header = array.shift()
        if (header.length == 0) {
            return [ header, null ]
        }
        return [ header, array.shift() ]
    }
}
