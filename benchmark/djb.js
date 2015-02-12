function djb (block, start, end) {
    var seed = 0
    for (var i = start; i < end; i++) {
        seed = (seed * 33 + block[i]) >>> 0
    }
    return new Buffer([
        seed >>> 24 & 0xff,
        seed >>> 16 & 0xff,
        seed >>> 8 & 0xff,
        seed & 0xff
    ])
}
module.exports = djb
