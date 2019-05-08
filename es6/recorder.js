module.exports = function (checksum) {
    var NULL = Buffer.alloc(0), EOL = Buffer.from('\n')
    return function (header, body) {
        var buffers = [], checksums = []
        if (body == null) {
            header.length = 0
        } else {
            if (!Buffer.isBuffer(body)) {
                header.json = true
                body = Buffer.from(JSON.stringify(body))
            }
            buffers.push(EOL, body)
            header.length = body.length + 1
            checksums.push(checksum(body, 0, body.length))
        }
        header = Buffer.from(JSON.stringify(header))
        buffers.push(EOL, header)
        checksums.push(checksum(header, 0, header.length))
        buffers.push(EOL, Buffer.from(JSON.stringify(checksums.reverse())))
        return Buffer.concat(buffers.reverse())
    }
}
