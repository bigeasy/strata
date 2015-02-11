function djb (block, start, end) {
    var seed = 0
    for (var i = start; i < end; i++) {
        seed = (seed * 33 + block[i]) >>> 0
    }
    return seed
}
module.exports = djb
