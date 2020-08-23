module.exports = function (checksum) {
    return function (array) {
        if (array.length == 0) {
            return null
        }
        const checksum = array.shift(), header = array.shift()
        return [ header ].concat(array.splice(0, header.lengths.length))
    }
}
