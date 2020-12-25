module.exports = function (checksum) {
    return function (array) {
        if (array.length == 0) {
            return null
        }
        const checksum = array.shift()
        const lengths = array.shift()
        const header = array.shift()
        return [ header ].concat(array.splice(0, lengths[0].length - 1))
    }
}
