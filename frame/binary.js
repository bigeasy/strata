// TODO Use `hash.djb` or other libraries.
var createChecksum = require('../checksum')

function Binary (checksum) {
    checksum = this.checksum = createChecksum(checksum, true)
    this.checksumLength = checksum ? checksum(new Buffer(0), 0, 0).length : 0
}

Binary.prototype.serialize = function (queue, header, body, serializer) {
    var bodyLength = 0
    if (body) {
        var body = serializer.serialize(body)
        var bodyLength = serializer.sizeOf(body)
    }
    var length = 8 + this.checksumLength + ((header.length + 1) * 4) + bodyLength
    var buffer = queue.slice(length)
    var offset = -4
    var payloadStart
    buffer.writeUInt32BE(buffer.length, offset += 4, true)
    buffer.writeUInt32BE(0xaaaaaaaa, offset += 4, true)
    payloadStart = (offset += this.checksumLength) + 4
    buffer.writeUInt32BE(header.length, offset += 4, true)
    for (var i = 0, I = header.length; i < I; i++) {
        buffer.writeInt32BE(header[i], offset += 4, true)
    }
    if (body) {
        serializer.write(body, buffer, offset += 4, buffer.length)
    }
    var checksum = this.checksum
    if (checksum) {
        var digest = checksum(buffer, payloadStart, buffer.length)
        digest.copy(buffer, 8, 0, digest.length)
    }
    return {
        heft: body == null ? 0 : bodyLength,
        length: length
    }
}

Binary.prototype.length = function (buffer, i, I) {
    var remaining = I - i
    if (remaining < 4) {
        return null
    }
    return buffer.readUInt32BE(i, true)
}

Binary.prototype.deserialize = function (deserialize, buffer, offset, length) {
    var start = offset
    var remaining = length - offset
    if (remaining < 4) {
        return null
    }
    var length = buffer.readUInt32BE(offset, true)
    var end = offset + length
    if (remaining < length) {
        return null
    }
    offset += 8
    var checksum = this.checksum
    if (checksum != null) {
        var digest = checksum(buffer, offset + this.checksumLength, end)
        for (var i = 0, I = digest.length; i < I; i++) {
            if (buffer[offset++] !== digest[i]) {
                throw new Error('invalid checksum')
            }
        }
    }
    var headerCount = buffer.readUInt32BE(offset, true)
    var header = []
    for (var i = 0; i < headerCount; i++) {
        header.push(buffer.readInt32BE(offset += 4, true))
    }
    offset += 4
    if (offset < end) {
        var body = deserialize(buffer, offset, end)
    }
    return {
        length: end - start,
        heft: body == null ? null : end - offset,
        header: header,
        body: body || null
    }
}

module.exports = Binary
