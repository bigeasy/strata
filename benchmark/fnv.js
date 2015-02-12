function fnv (block, start, end) {
    var hash = (0 ^ 2166136261) >>> 0
    for (var i = start; i < end; i++) {
        hash = (hash ^ block[i]) >>> 0
        hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24) >>> 0
//        hash = hash >>> 0
    }
    var buffer = new Buffer(4)
    buffer.writeUInt32LE(hash, 0, true)
    return buffer
}
module.exports = fnv
