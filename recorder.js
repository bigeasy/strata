module.exports = function (checksum) {
    return function (header, body) {
        if (!Buffer.isBuffer(body)) {
            body = Buffer.from(JSON.stringify(body) + '\n')
        }
        header.length = body.length
        var record = Buffer.concat([ Buffer.from(JSON.stringify(header) + '\n'), body ])
        return Buffer.concat([ Buffer.from(JSON.stringify(checksum(record, 0, record.length)) + '\n'), record ])
    }
}
