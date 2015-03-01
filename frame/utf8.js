var ok = require('assert').ok
var createChecksum = require('../checksum')

function UTF8 (checksum) {
    var checksum = this.checksum = createChecksum(checksum)
    this.dummyDigest = checksum ? checksum(new Buffer(0), 0, 0) : '0'
}

UTF8.prototype.serialize = function (queue, header, body, serializer) {
    var checksum = this.checksum, digest = this.dummyDigest,
        entry, buffer, json, line, length

    ok(header.every(function (n) {
        return typeof n == 'number'
    }), 'header values must be numbers')

    entry = header.slice()
    json = JSON.stringify(entry)

    length = 0

    var separator = ''
    if (body != null) {
        body = serializer.serialize(body)
        separator = ' '
        var bodyLength = serializer.sizeOf(body)
        length += bodyLength
        var temporary = new Buffer(bodyLength)
        serializer.write(body, temporary, 0, temporary.length)
        body = temporary
    }

    line = this.dummyDigest + ' ' + json + separator

    length += Buffer.byteLength(line, 'utf8') + 1

    var entire = length + String(length).length + 1
    if (entire < length + String(entire).length + 1) {
        length = length + String(entire).length + 1
    } else {
        length = entire
    }

    buffer = queue.slice(length)

    buffer.write(String(length) + ' ' + line)
    if (body != null) {
        body.copy(buffer, buffer.length - 1 - body.length)
    }
    buffer[length - 1] = 0x0A

    if (checksum) {
        var digest = checksum(buffer, String(length).length + digest.length + 2, length - 1)
        buffer.write(digest, String(length).length + 1)
    }

    return {
        heft: body == null ? 0 : bodyLength,
        length: length
    }
}

UTF8.prototype.length = function (buffer, i, I) {
    var start = i
    for (; i < I; i++) {
        if (buffer[i] == 0x20) break
    }
    if (buffer[i] != 0x20) {
        return null
    }
    return parseInt(buffer.toString('utf8', start, i))
}

UTF8.prototype.deserialize = function (deserialize, buffer, i, I) {
    var start = i
    for (; i < I; i++) {
        if (buffer[i] == 0x20) break
    }
    if (buffer[i] != 0x20) {
        return null
    }
    var size = parseInt(buffer.toString('utf8', start, i))
    if (I - start < size) {
        return null
    }
    for (var count = 2, i = start; i < I && count; i++) {
        if (buffer[i] == 0x20) count--
    }
    if (count) {
        throw new Error('corrupt line: could not find end of line header')
    }
    var checksumStart = i
    for (count = 1; i < I && count; i++) {
        if (buffer[i] == 0x20 || buffer[i] == 0x0a) count--
    }
    if (count) {
        throw new Error('couldn not find end of line header record')
    }
    var fields = buffer.toString('utf8', start, i - 1).split(' ')
    var checksum = this.checksum
    if (checksum) {
        var digest = checksum(buffer, checksumStart, start + size - 1)
        ok(fields[1] == '-' || digest == fields[1], 'corrupt line: invalid checksum')
    }
    var body, length
    if (buffer[i - 1] == 0x20) {
        body = buffer.slice(i, start + size - 1)
        length = body.length
    }
    if (buffer[i - 1] == 0x20) {
        i += body.length + 1
        var bodyLength = body.length
        body = deserialize(body, 0, body.length)
    }
    var entry = {
        heft: body ? bodyLength : 0,
        length: i - start,
        header: JSON.parse(fields[2]),
        body: body || null
    }
    ok(entry.header.every(function (n) {
        return typeof n == 'number'
    }), 'header values must be numbers')
    return entry
}

module.exports = UTF8
