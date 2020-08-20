module.exports = function (checksum) {
    const EOL = Buffer.from('\n')
    return function (header, parts) {
        const payload = [], buffers = [], checksums = [], length = [], json = []
        for (const part of parts) {
            json.push(!Buffer.isBuffer(part))
            const buffer = json[json.length - 1]
                ? Buffer.from(JSON.stringify(part))
                : part
            payload.push(buffer, EOL)
            length.push(buffer.length + 1)
        }
        buffers.unshift(Buffer.concat(payload))
        checksums.unshift(checksum(buffers[0], 0, buffers[0].length))
        buffers.unshift(Buffer.concat([ Buffer.from(JSON.stringify({ json, length, header })), EOL ]))
        checksums.unshift(checksum(buffers[0], 0, buffers[0].length))
        buffers.unshift(Buffer.from(JSON.stringify(checksums)), EOL)
        return Buffer.concat(buffers)
    }
}
