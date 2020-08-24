const assert = require('assert')
const EOL = Buffer.from('\n')

class Player {
    constructor (checksum) {
        this._checksum = checksum
        this._remainder = Buffer.alloc(0)
        this._entry = {
            state: 'checksum',
            header: null,
            checksums: null,
            sizes: []
        }
    }

    split (chunk) {
        const entries = []
        let { state, header, checksums, sizes } = this._entry, start = 0
        let offset = this._remainder.length
        chunk = Buffer.concat([ this._remainder, chunk ])
        SPLIT: for (;;) {
            switch (state) {
            case 'checksum': {
                    const index = chunk.indexOf(0xa, offset)
                    if (!~index) {
                        break SPLIT
                    }
                    sizes.push(index - start + 1)
                    checksums = JSON.parse(chunk.slice(start, index + 1))
                    start = offset = index + 1
                    state = 'header'
                }
                break
            case 'header': {
                    const index = chunk.indexOf(0xa, offset)
                    if (!~index) {
                        break SPLIT
                    }
                    sizes.push(index - start + 1)
                    const buffer = chunk.slice(start, index + 1)
                    assert.equal(checksums[0], this._checksum.call(null, buffer, 0, buffer.length))
                    header = JSON.parse(buffer.toString())
                    state = 'payload'
                    start = offset = index + 1
                }
                break
            case 'payload': {
                    const length = header.lengths.reduce((sum, value) => sum + value, 0)
                    if (chunk.length - start < length) {
                        break SPLIT
                    }
                    const checksum = this._checksum.call(null, chunk, start, start + length)
                    assert.equal(checksums[1], checksum)
                    const parts = []
                    for (const length of header.lengths) {
                        sizes.push(length)
                        let part = chunk.slice(start, start + length - 1)
                        offset = start = start + length
                        parts.push(part)
                    }
                    entries.push({
                        header: header.header,
                        parts: parts,
                        sizes: sizes
                    })
                    sizes = []
                    state = 'checksum'
                }
                break
            }
        }
        this._remainder = chunk.slice(start)
        this._entry = { state, start, header, checksums, sizes }
        return entries
    }

    empty () {
        return this._remainder.length == 0
    }
}

module.exports = Player
