module.exports = function (checksum) {
    const EOL = Buffer.from('\n')
    return function (header, parts) {
        const payload = [], buffers = [], checksums = [], lengths = []
        for (const part of parts) {
            payload.push(part, EOL)
            lengths.push(part.length + 1)
        }
        buffers.unshift(Buffer.concat(payload))
        checksums.unshift(checksum(buffers[0], 0, buffers[0].length))
        buffers.unshift(Buffer.concat([ Buffer.from(JSON.stringify({ lengths, header })), EOL ]))
        checksums.unshift(checksum(buffers[0], 0, buffers[0].length))
        buffers.unshift(Buffer.from(JSON.stringify(checksums)), EOL)
        return Buffer.concat(buffers)
    }
}
