module.exports = function (checksum) {
    var NULL = Buffer.alloc(0), EOL = Buffer.from('\n')
    return function (header, body) {
        var buffers
        if (body == null) {
            buffers = [ NULL ]
            header.length = 0
        } else {
            if (!Buffer.isBuffer(body)) {
                body = Buffer.from(JSON.stringify(body))
            }
            buffers = [ body, EOL ]
            header.length = body.length + 1
        }
        buffers.unshift(Buffer.from(JSON.stringify(header) + '\n'))
        var record = Buffer.concat(buffers)
        return Buffer.concat([ Buffer.from(JSON.stringify(checksum(record, 0, record.length)) + '\n'), record ])
    }
}
