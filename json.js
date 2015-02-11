exports.serializer = {
    serialize: function (body) {
        return JSON.stringify(body)
    },
    sizeOf: function (body) {
        return Buffer.byteLength(body, 'utf8')
    },
    write: function (body, buffer, offset, length) {
        buffer.write(body, offset, length, 'utf8')
    }
}

exports.deserialize = function (buffer, start, end) {
    return JSON.parse(buffer.toString('utf8', start, end, 'utf8'))
}
